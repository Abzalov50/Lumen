(in-package :cl)

(defpackage :lumen.obs.metrics
  (:use :cl :alexandria)
  (:import-from :lumen.core.http
    :request :response :req-headers :req-method :req-path :req-query
    :resp-status :resp-headers :resp-body)
  (:import-from :lumen.core.router :defroute)
  (:import-from :lumen.utils :ensure-header :alist-get :alist-set)
  (:import-from :bordeaux-threads :make-lock :with-lock-held)
  (:export :metrics-middleware :install-metrics-route))

(in-package :lumen.obs.metrics)

(defparameter *lock* (make-lock "metrics-lock"))

;; petits compteurs/jauges/histogrammes ad hoc (labels minimales pour éviter la cardinalité)
(defparameter *inflight* 0) ; gauge
(defparameter *req-total* (make-hash-table :test 'equal))   ; key: (method status-class)
(defparameter *req-dur*   (make-hash-table :test 'equal))   ; key: method  -> plist (:sum :count :buckets alist)
(defparameter *resp-size* (make-hash-table :test 'equal))   ; key: method  -> plist (:sum :count)
(defparameter *req-size*  (make-hash-table :test 'equal))   ; key: method  -> plist (:sum :count)
(defparameter *start-time-seconds* (/ (get-universal-time) 1.0)) ; approx. (UTC secs)

(defparameter *duration-buckets*
  ;; seconds (Prometheus histogram)
  #(0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10))

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
			  (:buckets . ,(mapcar (lambda (b)
						 (cons b 0))
					       (coerce *duration-buckets* 'list)))))))
           (sum (lumen.utils:alist-get h :sum))
           (cnt (lumen.utils:alist-get h :count))
           (bks (lumen.utils:alist-get h :buckets)))
      (lumen.utils:alist-set h :sum (+ sum (coerce seconds 'double-float)))
      (lumen.utils:alist-set h :count (1+ cnt))
      ;; bucket <= le
      (dolist (bk bks)
        (when (<= seconds (car bk)) (incf (cdr bk))))
      ;; +Inf
      (push (cons :inf (1+ (or (cdar (last bks)) 0))) bks)
      h)))

(defun %utf8-length (s)
  (length (trivial-utf-8:string-to-utf-8-bytes s)))

(defun %render-prometheus ()
  (with-output-to-string (s)
    (labels ((pp (fmt &rest args) (apply #'format s fmt args))
             (emit-hist (name method h)
               (let* ((bks (remove :inf (copy-list (lumen.utils:alist-get h :buckets))
				   :key #'car)))
                 (dolist (bk bks)
                   (pp "~A_bucket{method=\"~A\",le=\"~A\"} ~D~%"
                       name method (car bk) (cdr bk)))
                 ;; +Inf
                 (let ((inf (assoc :inf (lumen.utils:alist-get h :buckets))))
                   (pp "~A_bucket{method=\"~A\",le=\"+Inf\"} ~D~%" name method (cdr inf)))
                 (pp "~A_sum{method=\"~A\"} ~,6F~%" name method (lumen.utils:alist-get h :sum))
                 (pp "~A_count{method=\"~A\"} ~D~%" name method (lumen.utils:alist-get h :count)))))
      ;; HELP/TYPE
      (pp "# HELP lumen_http_requests_total Total HTTP requests by method/status class.~%")
      (pp "# TYPE lumen_http_requests_total counter~%")
      (maphash (lambda (k v)
                 (destructuring-bind (m sc) k
                   (pp "lumen_http_requests_total{method=\"~A\",status_class=\"~A\"} ~D~%" m sc v)))
               *req-total*)

      (pp "# HELP lumen_http_inflight_requests In-flight HTTP requests.~%")
      (pp "# TYPE lumen_http_inflight_requests gauge~%")
      (pp "lumen_http_inflight_requests ~D~%" (with-lock-held (*lock*) *inflight*))

      (pp "# HELP lumen_http_request_duration_seconds Request duration histogram.~%")
      (pp "# TYPE lumen_http_request_duration_seconds histogram~%")
      (maphash (lambda (m h) (emit-hist "lumen_http_request_duration_seconds" m h)) *req-dur*)

      (pp "# HELP lumen_http_response_size_bytes Response size bytes (sum/count).~%")
      (pp "# TYPE lumen_http_response_size_bytes summary~%")
      (maphash (lambda (m cell)
                 (pp "lumen_http_response_size_bytes_sum{method=\"~A\"} ~,0F~%"
		     m (lumen.utils:alist-get cell :sum))
                 (pp "lumen_http_response_size_bytes_count{method=\"~A\"} ~D~%"
		     m (lumen.utils:alist-get cell :count)))
               *resp-size*)

      (pp "# HELP lumen_http_request_size_bytes Request size bytes (sum/count).~%")
      (pp "# TYPE lumen_http_request_size_bytes summary~%")
      (maphash (lambda (m cell)
                 (pp "lumen_http_request_size_bytes_sum{method=\"~A\"} ~,0F~%"
		     m (lumen.utils:alist-get cell :sum))
                 (pp "lumen_http_request_size_bytes_count{method=\"~A\"} ~D~%"
		     m (lumen.utils:alist-get cell :count)))
               *req-size*)

      (pp "# HELP process_start_time_seconds UNIX time the process started.~%")
      (pp "# TYPE process_start_time_seconds gauge~%")
      (pp "process_start_time_seconds ~,3F~%" *start-time-seconds*))))

(defun metrics-middleware (next)
  "Compteur + histogramme durée + in-flight gauge + tailles."
  (lambda (req)
    (let* ((method (string-upcase (or (lumen.core.http:req-method req) "GET")))
           (clen   (lumen.utils:alist-get (lumen.core.http:req-headers req)
					  "content-length"))
           (req-bytes (when clen (parse-integer clen :junk-allowed t)))
           (t0 (get-internal-real-time)))
      (with-lock-held (*lock*) (incf *inflight*))
      (unwind-protect
           (let ((resp (funcall next req)))
             ;; post
             (let* ((status (lumen.core.http:resp-status resp))
                    (body   (lumen.core.http:resp-body resp))
                    (resp-bytes
                      (cond
                        ((stringp body) (%utf8-length body))
                        ((typep body '(simple-array (unsigned-byte 8) (*))) (length body))
                        (t nil))) ; streaming inconnu
                    (ms (/ (- (get-internal-real-time) t0)
                           (/ internal-time-units-per-second 1000.0)))
                    (sec (/ ms 1000.0)))
               (%inc! *req-total* (list method (%status-class status)) 1)
               (%hist-observe! *req-dur* method sec)
               (when resp-bytes (%sum-count! *resp-size* method resp-bytes))
               (when req-bytes  (%sum-count! *req-size*  method req-bytes))
               resp))
        (with-lock-held (*lock*) (decf *inflight*))))))

(defun install-metrics-route ()
  "Expose /metrics en texte Prometheus."
  (defroute GET "/metrics" (req)
    (declare (ignore req))
    (make-instance 'lumen.core.http:response
                   :status 200
                   :headers '(("Content-Type" . "text/plain; version=0.0.4; charset=utf-8")
                              ("Cache-Control" . "no-store"))
                   :body (%render-prometheus))))
