(in-package :cl)

(defpackage :lumen.obs.metrics
  (:use :cl :alexandria)
  (:import-from :lumen.core.http
    :request :response :req-headers :req-method :req-path :req-query
    :resp-status :resp-headers :resp-body)
  (:import-from :lumen.core.router :defroute)
  (:import-from :lumen.utils :ensure-header :alist-get :alist-set)
  (:import-from :bordeaux-threads :make-lock :with-lock-held)
  (:import-from :lumen.core.middleware :defmiddleware)
  ;; AJOUT : get-path-stats exporté pour l'inspecteur
  (:export :metrics-middleware :install-metrics-route :get-path-stats))

(in-package :lumen.obs.metrics)

(defparameter *lock* (make-lock "metrics-lock"))

;; --- Stockage des métriques ---
(defparameter *inflight* 0)
(defparameter *req-total* (make-hash-table :test 'equal))   ; (method status-class) -> count
(defparameter *req-dur* (make-hash-table :test 'equal))   ; method -> histogram
(defparameter *resp-size* (make-hash-table :test 'equal))   ; method -> summary
(defparameter *req-size* (make-hash-table :test 'equal))   ; method -> summary
(defparameter *req-path-stats* (make-hash-table :test 'equal)) ; "METHOD /path" -> (:count N :sum-ms M :errors E)
(defparameter *start-time-seconds* (/ (get-universal-time) 1.0))

(defparameter *duration-buckets*
  #(0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10))

;; --- Helpers Internes ---

(defun %status-class (n)
  (let ((d (truncate n 100))) (format nil "~Dxx" d)))

(defun %inc! (ht key &optional (delta 1))
  (with-lock-held (*lock*)
    (incf (gethash key ht 0) delta)))

(defun %sum-count! (ht key value)
  (with-lock-held (*lock*)
    (let* ((cell (or (gethash key ht)
                     (setf (gethash key ht) '((:sum . 0.0) (:count . 0)))))
           (sum  (lumen.utils:alist-get cell :sum))
           (cnt  (lumen.utils:alist-get cell :count)))
      (lumen.utils:alist-set cell :sum (+ sum (coerce value 'double-float)))
      (lumen.utils:alist-set cell :count (1+ cnt))
      cell)))

(defun %hist-observe! (ht key seconds)
  (with-lock-held (*lock*)
    (let* ((h (or (gethash key ht)
                  (setf (gethash key ht)
                        `((:sum . 0.0) (:count . 0)
                          (:buckets . ,(mapcar (lambda (b) (cons b 0))
                                               (coerce *duration-buckets* 'list)))))))
           (sum (lumen.utils:alist-get h :sum))
           (cnt (lumen.utils:alist-get h :count))
           (bks (lumen.utils:alist-get h :buckets)))
      (lumen.utils:alist-set h :sum (+ sum (coerce seconds 'double-float)))
      (lumen.utils:alist-set h :count (1+ cnt))
      (dolist (bk bks)
        (when (<= seconds (car bk)) (incf (cdr bk))))
      (push (cons :inf (1+ (or (cdar (last bks)) 0))) bks)
      h)))

(defun %record-path-stats! (method path ms error-p)
  "Enregistre les stats spécifiques à une route pour l'Inspecteur."
  (let ((key (format nil "~A ~A" method path)))
    (with-lock-held (*lock*)
      (let ((entry (or (gethash key *req-path-stats*)
                       (setf (gethash key *req-path-stats*) 
                             (list :count 0 :sum-ms 0 :errors 0)))))
        (incf (getf entry :count))
        (incf (getf entry :sum-ms) ms)
        (when error-p
          (incf (getf entry :errors)))))))

(defun %utf8-length (s)
  (length (trivial-utf-8:string-to-utf-8-bytes s)))

;; --- API Publique ---

(defun get-path-stats (method path)
  "Retourne la plist (:count N :sum-ms M :errors E) pour l'inspecteur."
  (let ((key (format nil "~A ~A" (string-upcase method) path)))
    (with-lock-held (*lock*) ;; Lecture thread-safe
      ;; On retourne une copie pour éviter les effets de bord pendant le rendu
      (copy-list (gethash key *req-path-stats*)))))

;; Helper pour la taille du body (si pas déjà défini ailleurs)
#|
(defun %body-length (body)
  (cond
    ((stringp body) (trivial-utf-8:utf-8-byte-length body))
    ((typep body '(simple-array (unsigned-byte 8) (*))) (length body))
    (t 0)))
|#
(defun %body-length (body)
  (cond
    ((stringp body) (trivial-utf-8:utf-8-byte-length body))
    ;; CORRECTION : On accepte n'importe quel vecteur d'octets, pas juste les simple-array
    ((typep body '(vector (unsigned-byte 8))) (length body))
    (t 0)))

(lumen.core.middleware:defmiddleware metrics-middleware
    ((ignore-paths :initarg :ignore-paths 
                   :initform '("/health" "/favicon.ico") 
                   :accessor mw-ignore-paths))
    (req next)
  
  (let ((path (lumen.core.http:req-path req)))
    
    ;; 1. SKIP si le path est ignoré
    (if (member path (slot-value mw 'ignore-paths) :test #'string=)
        (funcall next req)
        
        ;; 2. LOGIQUE METRICS
        (let* ((method (string-upcase (or (lumen.core.http:req-method req) "GET")))
               ;; Optimisation : On cherche d'abord le pattern de route (injecté par le routeur)
               ;; Sinon on fallback sur le path brut.
               (route-pattern (or (lumen.core.http:ctx-get req :route-pattern) path))
               (clen (lumen.utils:alist-get (lumen.core.http:req-headers req) "content-length"))
               (req-bytes (when clen (parse-integer clen :junk-allowed t)))
               (t0 (get-internal-real-time)))
          
          ;; Gauge In-Flight (+1)
          (bt:with-lock-held (*lock*) (incf *inflight*))
          
          (unwind-protect
               (let ((resp (funcall next req)))
                 (let* ((status (lumen.core.http:resp-status resp))
                        (body   (lumen.core.http:resp-body resp))
                        (resp-bytes (%body-length body))
                        ;; Calcul temps
                        (ms (/ (- (get-internal-real-time) t0)
                               (/ internal-time-units-per-second 1000.0)))
                        (sec (/ ms 1000.0))
                        (is-error (>= status 500)))

                   ;; A. Mise à jour stats globales (Prometheus)
                   ;; Note: On utilise 'route-pattern' ici pour grouper /users/1 et /users/2
                   (%inc! *req-total* (list method route-pattern (%status-class status)) 1)
                   (%hist-observe! *req-dur* method sec)
                   (when resp-bytes (%sum-count! *resp-size* method resp-bytes))
                   (when req-bytes  (%sum-count! *req-size* method req-bytes))

                   ;; B. Mise à jour stats Inspecteur (Interne Lumen)
                   (%record-path-stats! method route-pattern ms is-error)
                   
                   resp))
            
            ;; Cleanup In-Flight (-1)
            (bt:with-lock-held (*lock*) (decf *inflight*)))))))

;; --- Rendu Prometheus (Inchangé) ---
(defun %render-prometheus ()
  (with-output-to-string (s)
    (labels ((pp (fmt &rest args) (apply #'format s fmt args))
             (emit-hist (name method h)
               (let* ((bks (remove :inf (copy-list (lumen.utils:alist-get h :buckets)) :key #'car)))
                 (dolist (bk bks)
                   (pp "~A_bucket{method=\"~A\",le=\"~A\"} ~D~%" name method (car bk) (cdr bk)))
                 (let ((inf (assoc :inf (lumen.utils:alist-get h :buckets))))
                   (pp "~A_bucket{method=\"~A\",le=\"+Inf\"} ~D~%" name method (cdr inf)))
                 (pp "~A_sum{method=\"~A\"} ~,6F~%" name method (lumen.utils:alist-get h :sum))
                 (pp "~A_count{method=\"~A\"} ~D~%" name method (lumen.utils:alist-get h :count)))))
      
      (pp "# HELP lumen_http_requests_total Total HTTP requests.~%")
      (pp "# TYPE lumen_http_requests_total counter~%")
      (maphash (lambda (k v) (pp "lumen_http_requests_total{method=\"~A\",status_class=\"~A\"} ~D~%" (first k) (second k) v)) *req-total*)

      (pp "# HELP lumen_http_inflight_requests In-flight HTTP requests.~%")
      (pp "# TYPE lumen_http_inflight_requests gauge~%")
      (pp "lumen_http_inflight_requests ~D~%" (with-lock-held (*lock*) *inflight*))

      (pp "# HELP lumen_http_request_duration_seconds Histogram.~%")
      (pp "# TYPE lumen_http_request_duration_seconds histogram~%")
      (maphash (lambda (m h) (emit-hist "lumen_http_request_duration_seconds" m h)) *req-dur*)
      
      ;; (Ajoutez resp-size / req-size si désiré ici, j'ai abrégé pour la clarté)
      (pp "# HELP process_start_time_seconds Start time.~%")
      (pp "# TYPE process_start_time_seconds gauge~%")
      (pp "process_start_time_seconds ~,3F~%" *start-time-seconds*))))

(defun install-metrics-route ()
  (defroute GET "/metrics" (req)
    (declare (ignore req))
    (make-instance 'lumen.core.http:response
                   :status 200
                   :headers '(("Content-Type" . "text/plain; version=0.0.4; charset=utf-8"))
                   :body (%render-prometheus))))
