(in-package :cl)

(defpackage :lumen.core.middleware
  (:use :cl :alexandria :lumen.core.body :lumen.core.http)
  (:import-from :lumen.core.pipeline :middleware :handle :defmiddleware)
  (:import-from :lumen.utils :-> :->> :str-prefix-p :ensure-header :parse-http-date
		:format-http-date)
  (:import-from :lumen.core.mime :guess-content-type)
  (:import-from :lumen.core.session :session-id :session-data :session-get :session-set! :session-del! :verify-signed-sid :sign-sid :store-get :store-put! :store-del! :rand-bytes :*session-ttl* :*session-cookie* :*secure-cookie*)
  (:import-from :lumen.core.jwt :*jwt-secret* :jwt-encode :jwt-decode)
  (:import-from :lumen.core.ratelimit :allow?)
  (:import-from :lumen.core.http-range :respond-file)
  (:import-from :lumen.core.router :dispatch)
  (:import-from :lumen.core.trace :with-tracing :*trace-root* :print-trace-waterfall
		:current-context :with-propagated-context)
  (:export :defmiddleware :logger-middleware :json-only-middleware
   :cors-middleware :cors-auto :static-middleware :cookie-middleware
   :form-parser-middleware :body-parser-middleware :query-parser-middleware
	   :request-id-middleware :error-middleware :form-parser-middleware
   :multipart-parser-middleware :router-middleware
   :etag-middleware :compression-middleware
	   :last-modified-middleware :session-middleware :csrf-middleware
   :auth-middleware :auth-required :roles-allowed
	   :https-redirect-middleware :rate-limit-middleware :timeout-middleware
	   :max-body-size-middleware :context-middleware :access-log-middleware
   :parse-query-string-to-alist :trust-proxy-middleware :inspector-middleware
   :trace-middleware))

(in-package :lumen.core.middleware)

;;; ---------------------------------------------------------------------------
;;; 2. HELPERS (Logger Utils)
;;; ---------------------------------------------------------------------------

(defun %status-color (status)
  (cond
    ((<= 200 status 299) "32")  ; vert
    ((<= 300 status 399) "36")  ; cyan
    ((<= 400 status 499) "33")  ; jaune
    (t                   "31"))) ; rouge

(defun %emacs-repl-p ()
  (or (member :swank *features*)
      (member :slynk *features*)
      (uiop:getenv "INSIDE_EMACS")))

(defun %supports-ansi-p (stream)
  (declare (ignore stream))
  (and (not (%emacs-repl-p))
       (not (member :win32 *features*))
       (let ((term (uiop:getenv "TERM")))
         (and term (not (string= term "")) (not (string-equal term "dumb"))))))

;;; ---------------------------------------------------------------------------
;;; 3. LOGGER MIDDLEWARE (Refactorisé en CLOS)
;;; ---------------------------------------------------------------------------

(defmiddleware logger-middleware
    ((stream :initarg :stream 
             :initform *terminal-io* :accessor logger-stream)
     (color  :initarg :color  
             :initform :auto)
     (color-enabled-p :initform nil :accessor logger-color-p)
     ;; --- AJOUT DU LOCK ---
     ;; Chaque instance de logger a son propre verrou pour protéger son stream
     (lock :initform (bt:make-lock "logger-lock") :accessor logger-lock))
    (req next)
  
  (labels ((h (name)
             (cdr (assoc name (lumen.core.http:req-headers req) :test #'string-equal))))
    (let* ((t0   (get-internal-real-time))
           (ip   (or (let* ((xff (h "x-forwarded-for")))
                       (and xff (string-trim " " (car (uiop:split-string xff :separator ",")))))
                     (h "x-real-ip")
                     (lumen.core.http:ctx-get req :remote-addr)
                     "-"))
           (host (or (h "host") ""))
           (meth (or (lumen.core.http:req-method req) "GET"))
           (path (or (lumen.core.http:req-path req) "/"))
           
           ;; Exécution
           (resp (funcall next req))
           
           ;; Mesures
           (ms    (/ (- (get-internal-real-time) t0)
                     (/ internal-time-units-per-second 1000.0)))
           (msi   (round ms))
           (st    (lumen.core.http:resp-status resp))
           (ts    (local-time:format-timestring
                   nil (local-time:now)
                   :format '(:year "-" (:month 2) "-" (:day 2) " "
                             (:hour 2) ":" (:min 2) ":" (:sec 2))))
           
           ;; Récupération des slots via 'mw' (l'instance)
           (out   (slot-value mw 'stream))
           (lock  (slot-value mw 'lock))
           (use-color (slot-value mw 'color-enabled-p)))

      ;; Écriture Thread-Safe avec le lock de l'instance
      (bt:with-lock-held (lock) 
        (if use-color
            (format out "~&[~A] ~A ~A ~A ~A -> ~C[0;~am~D~C[0m (~D ms)~%"
                    ts ip meth host path
                    #\Esc (%status-color st) st #\Esc msi)
            (format out "~&[~A] ~A ~A ~A ~A -> ~D (~D ms)~%"
                    ts ip meth host path st msi))
        (finish-output out))
      
      resp)))

;;; Méthode d'initialisation pour optimiser le flag couleur
(defmethod initialize-instance :after ((mw logger-middleware) &key)
  (with-slots (stream color color-enabled-p) mw
    (setf color-enabled-p 
          (if (eq color :auto) 
              (%supports-ansi-p stream) 
              (not (null color))))))

;;; ---------------------------------------------------------------------------
;;; 3. QUERY PARSER MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %url-decode (s)
  (when s (with-output-to-string (out)
            (loop for i from 0 below (length s) do
              (let ((c (char s i)))
                (cond ((char= c #\+) (write-char #\Space out))
                      ((and (char= c #\%) (<= (+ i 2) (1- (length s))))
                       (let ((h1 (digit-char-p (char s (1+ i)) 16)) (h2 (digit-char-p (char s (+ i 2)) 16)))
                         (if (and h1 h2) (progn (write-char (code-char (+ (* h1 16) h2)) out) (incf i 2)) (write-char c out))))
                      (t (write-char c out))))))))

(defun parse-query-string-to-alist (qs)
  (if (or (null qs) (zerop (length qs))) nil
      (loop for pair in (uiop:split-string qs :separator "&")
            for p = (let ((len (length pair)))
                      (loop for i from 0 below len when (char= (char pair i) #\=) when (or (zerop i) (not (member (char pair (1- i)) '(#\! #\> #\<)))) return i))
            for k = (%url-decode (subseq pair 0 (or p (length pair))))
            for v = (%url-decode (if p (subseq pair (1+ p)) ""))
            collect (cons k v))))

(defmiddleware query-parser-middleware () (req next)
  (let ((q (lumen.core.http:req-query req)))
    (when (stringp q)
      (setf (lumen.core.http:req-query req) (parse-query-string-to-alist q)))
    (funcall next req)))

;;; ---------------------------------------------------------------------------
;;; 4. JSON ONLY MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun path-matches? (path paths prefixes)
  (or (and paths (member path paths :test #'string=))
      (and prefixes (some (lambda (pfx) (and (<= (length pfx) (length path)) (string= pfx path :end2 (length pfx)))) prefixes))))

(defmiddleware json-only-middleware
    ((paths :initarg :paths :initform nil)
     (prefixes :initarg :prefixes :initform '("/api"))
     (exclude-paths :initarg :exclude-paths :initform nil)
     (exclude-prefixes :initarg :exclude-prefixes :initform nil)
     (respect-existing :initarg :respect-existing :initform t))
    (req next)
  
  (let* ((resp (funcall next req))
         (path (lumen.core.http:req-path req))
         (guess (lumen.core.mime:content-type-for path))
         (ct (cdr (assoc "content-type" (lumen.core.http:resp-headers resp) :test #'string-equal))))
    
    (with-slots (paths prefixes exclude-paths exclude-prefixes respect-existing) mw
      (cond
        ;; Asset (connu via extension) -> Skip
        ((and guess (not (string= (string-downcase guess) "application/octet-stream"))) resp)
        ;; Exclu explicitement -> Skip
        ((path-matches? path exclude-paths exclude-prefixes) resp)
        ;; Respect existant -> Skip
        ((and respect-existing ct (not (cl-ppcre:scan "^application/json" (string-downcase ct)))) resp)
        ;; Match API -> Force JSON
        ((path-matches? path paths prefixes)
         (setf (lumen.core.http:resp-headers resp)
               (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "content-type" "application/json; charset=utf-8"))
         resp)
        ;; Default -> Skip
        (t resp)))))

;;; ---------------------------------------------------------------------------
;;; 5. CORS MIDDLEWARE (Stateful)
;;; ---------------------------------------------------------------------------

(defmiddleware cors-middleware
    ((mode :initarg :mode :initform :dev) ;; :dev | :prod
     (origins :initarg :origins :initform '("*"))
     (methods :initarg :methods :initform '("GET" "POST" "PUT" "PATCH" "DELETE" "OPTIONS"))
     (headers :initarg :headers :initform '("Content-Type" "Authorization"))
     (expose :initarg :expose :initform nil)
     (credentials :initarg :credentials :initform nil)
     (max-age :initarg :max-age :initform 600))
    (req next)
  
  (with-slots (mode origins methods headers expose credentials max-age) mw
    (let* ((req-h (lumen.core.http:req-headers req))
           (origin (cdr (assoc "origin" req-h :test #'string-equal)))
           (acr-method (cdr (assoc "access-control-request-method" req-h :test #'string-equal)))
           (acr-headers (cdr (assoc "access-control-request-headers" req-h :test #'string-equal)))
           (is-options (string= (lumen.core.http:req-method req) "OPTIONS"))
           (preflight (and is-options acr-method))
           
           ;; Calcul Allow-Origin
           (allow-origin 
             (cond 
               ((eq mode :dev) (or (and credentials origin) "*"))
               ((eq mode :prod)
                (cond ((and origin (member origin origins :test #'string=)) origin)
                      ((member "*" origins :test #'string=) "*")
                      (t nil)))
               (t nil))))

      (labels ((ensure (r n v) 
                 (setf (lumen.core.http:resp-headers r)
                       (lumen.utils:ensure-header (lumen.core.http:resp-headers r) n v)))
               (decorate (r)
                 (ensure r "vary" "Origin")
                 (when allow-origin
                   (let ((eff (if (and credentials (string= allow-origin "*") origin) origin allow-origin)))
                     (ensure r "access-control-allow-origin" eff)
                     (when (and credentials (not (string= eff "*")))
                       (ensure r "access-control-allow-credentials" "true"))))
                 (when expose
                   (ensure r "access-control-expose-headers" (format nil "~{~A~^, ~}" expose)))
                 r))

        (if preflight
            ;; -- PREFLIGHT RESPONSE --
            (let ((resp (make-instance 'lumen.core.http:response :status 204 :body "")))
              (ensure resp "vary" "Origin, Access-Control-Request-Method, Access-Control-Request-Headers")
              (when allow-origin
                (let* ((eff (if (and credentials (string= allow-origin "*") origin) origin allow-origin))
                       (all-hdrs (let ((base (format nil "~{~A~^, ~}" headers)))
                                   (if (and acr-headers (plusp (length acr-headers)))
                                       (format nil "~A, ~A" base acr-headers) base))))
                  (ensure resp "access-control-allow-origin" eff)
                  (ensure resp "access-control-allow-methods" (format nil "~{~A~^, ~}" methods))
                  (ensure resp "access-control-allow-headers" all-hdrs)
                  (ensure resp "access-control-max-age" (princ-to-string max-age))
                  (when (and credentials (not (string= eff "*")))
                    (ensure resp "access-control-allow-credentials" "true"))))
              resp)
            
            ;; -- NORMAL RESPONSE --
            (decorate (funcall next req)))))))

;;; ---------------------------------------------------------------------------
;;; 6. FACTORY AUTO-CONFIGURATION
;;; ---------------------------------------------------------------------------

(defun cors-auto ()
  "Créer une instance de CORS middleware basée sur la config Lumen."
  (let* ((profile (lumen.core.config:cfg-profile))
         (prod-p  (member profile '(:prod :production :staging) :test #'eq))
         (mode    (if prod-p :prod :dev))
         ;; Lecture config
         (origins (or (lumen.core.config:cfg-get-list :cors/origins) (if prod-p nil '("*"))))
         (creds   (lumen.core.config:cfg-get-bool :cors/credentials :default nil))
         (max-age (lumen.core.config:cfg-get-duration :cors/max-age :default "600s")))
    
    (make-instance 'cors-middleware
                   :mode mode
                   :origins origins
                   :credentials creds
                   :max-age max-age)))


;;; ---------------------------------------------------------------------------
;;; 6. STATIC MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %file-rfc1123 (pn) (let ((ut (ignore-errors (file-write-date pn)))) (and ut (lumen.utils:format-http-date ut))))

(defun %weak-etag-from-file (pn)
  (handler-case
      (let* ((size (with-open-file (in pn :direction :input :element-type '(unsigned-byte 8)) (file-length in)))
             (mt (file-write-date pn)))
        (format nil "W/\"~X-~X\"" size mt))
    (error () nil)))

(defun safe-join (root rel)
  "Concatène ROOT et REL en empêchant tout échappement."
  (labels ((forbidden () (merge-pathnames #P"__forbidden__" (uiop:ensure-directory-pathname (truename root)))))
    (handler-case
        (let* ((rel* (or rel ""))
               (rel1 (substitute #\/ #\\ rel*))
               (drive-pos (position #\: rel1))
               (starts-with-drive (and drive-pos (= drive-pos 1)))
               (rel2 (with-output-to-string (s)
                       (dolist (part (remove-if (lambda (p) (or (string= p "") (string= p ".") (string= p ".."))) (uiop:split-string rel1 :separator "/")))
                         (when (> (length part) 0) (format s "~A/" part)))))
               (rel3 (if (and (> (length rel2) 0) (char= (char rel2 (1- (length rel2))) #\/) (not (and (> (length rel1) 0) (char= (char rel1 (1- (length rel1))) #\/)))) (subseq rel2 0 (1- (length rel2))) rel2))
               (root-dir (uiop:ensure-directory-pathname (truename root)))
               (abs (merge-pathnames rel3 root-dir)))
          (if starts-with-drive (forbidden)
              (let ((root-str (namestring root-dir)) (abs-str (namestring (uiop:ensure-pathname abs))))
                (if (and (>= (length abs-str) (length root-str)) (string= root-str abs-str :end2 (length root-str))) abs (forbidden)))))
      (error () (forbidden)))))

(defun directory-index-html (dir rel-url &key (hide-dotfiles t))
  (let* ((rel (or rel-url "/"))
         (rel* (if (char= (char rel (1- (length rel))) #\/) rel (concatenate 'string rel "/")))
         (files (uiop:directory-files dir))
         (subs (uiop:subdirectories dir)))
    (labels ((visible-p (pn)
               (let ((name (car (last (uiop:split-string (namestring pn) :separator "/")))))
                 (or (not hide-dotfiles) (and name (not (and (> (length name) 0) (char= (char name 0) #\.))))))))
      (with-output-to-string (s)
        (format s "<!doctype html><meta charset='utf-8'><title>Index of ~A</title>" rel*)
        (format s "<style>body{font:14px system-ui,sans-serif;margin:24px}ul{list-style:none;padding:0}li{margin:4px 0}a{text-decoration:none;color:#0366d6}a:hover{text-decoration:underline}</style>")
        (format s "<h1>Index of ~A</h1><ul>" rel*)
        (when (> (length rel*) 1)
          (let* ((trim (if (char= (char rel* (1- (length rel*))) #\/) (subseq rel* 0 (1- (length rel*))) rel*))
                 (pidx (or (position #\/ trim :from-end t) 0)) (up (if (= pidx 0) "/" (subseq trim 0 pidx))))
            (format s "<li>↩︎ <a href='~A'>..</a></li>" up)))
        (dolist (sd (sort (remove-if-not #'visible-p subs) #'string< :key #'namestring))
          (let ((nm (car (last (uiop:split-string (namestring sd) :separator "/"))))) (format s "<li>📁 <a href='~A/'>~A/</a></li>" nm nm)))
        (dolist (f (sort (remove-if-not #'visible-p files) #'string< :key #'namestring))
          (let ((nm (car (last (uiop:split-string (namestring f) :separator "/"))))) (format s "<li>📄 <a href='~A'>~A</a></li>" nm nm)))
        (format s "</ul>")))))

(defmiddleware static-middleware
    ((prefix :initarg :prefix :initform "/" :accessor static-prefix)
     (dir    :initarg :dir :initform #P"./public/" :accessor static-dir)
     (auto-index :initarg :auto-index :initform t)
     (try-index :initarg :try-index :initform "index.html")
     (redirect-dir :initarg :redirect-dir :initform t)
     (hide-dotfiles :initarg :hide-dotfiles :initform t)
     (file-cache-secs :initarg :file-cache-secs :initform 3600)
     (dir-cache-secs :initarg :dir-cache-secs :initform 300)
     (spa-fallback :initarg :spa-fallback :initform nil))
    (req next)
  
  (let* ((path (lumen.core.http:req-path req))
         (prefix* (slot-value mw 'prefix))
         (root* (truename (slot-value mw 'dir))) ;; Peut errorer si le dossier n'existe pas à l'init
         (plen (length prefix*)))
    
    (if (and (>= (length path) plen)
             (string= prefix* path :end2 plen))
        ;; Match Prefix
        (let* ((rel (subseq path plen))
               (target (safe-join root* rel))
               (dir-truename (ignore-errors (lumen.utils:probe-directory target)))
               )
          
          (handler-case
              (cond
                ;; -------- DOSSIER --------
                (dir-truename
                 (with-slots (redirect-dir try-index auto-index hide-dotfiles dir-cache-secs) mw
                   (let ((has-slash (and (> (length path) 0) (char= (char path (1- (length path))) #\/))))
                     (cond
                       ;; Redirect /foo -> /foo/
                       ((and redirect-dir (not has-slash))
                        (make-instance 'lumen.core.http:response :status 301 
                                       :headers `(("location" . ,(concatenate 'string path "/")) 
                                                  ("cache-control" . ,(format nil "public, max-age=~D" dir-cache-secs)))
                                       :body ""))
                       ;; Try Index
                       ((and try-index (probe-file (merge-pathnames try-index (uiop:ensure-directory-pathname dir-truename))))
                        ;; Re-dispatch vers fichier index
                        (let ((idx-path (merge-pathnames try-index (uiop:ensure-directory-pathname dir-truename))))
                          (%serve-file req idx-path mw)))
                       ;; Auto Index
                       (auto-index
                        (let ((html (directory-index-html (uiop:ensure-directory-pathname dir-truename) 
                                                          (if (plusp (length path)) path "/")
                                                          :hide-dotfiles hide-dotfiles))
                              (lm (%file-rfc1123 dir-truename)))
                          (make-instance 'lumen.core.http:response :status 200 
                                         :headers `(("content-type" . "text/html; charset=utf-8")
                                                    ("cache-control" . ,(format nil "public, max-age=~D" dir-cache-secs))
                                                    ,@(when lm `(("last-modified" . ,lm))))
                                         :body html)))
                       ;; Pas d'index -> SPA Fallback ?
                       (t (maybe-spa-fallback req mw next))))))

                ;; -------- FICHIER --------
                ((probe-file target)
                 (%serve-file req target mw))

                ;; -------- NOT FOUND --------
                (t (maybe-spa-fallback req mw next)))
            
            (error (c) 
              (format t "~&[Static Error] ~A~%" c)
              (lumen.core.http:respond-404 "Static File Error"))))
        
        ;; Pas de Match Prefix -> Next
        (funcall next req))))

;;; Helpers internes au MW (méthodes auxiliaires ou flet)
(defmethod %serve-file (req pn (mw static-middleware))
  (let* ((file-cache-secs (slot-value mw 'file-cache-secs))
         (ct (lumen.core.mime:guess-content-type pn))
         (lm (lumen.core.middleware::%file-rfc1123 pn))
         (etag (lumen.core.middleware::%weak-etag-from-file pn))
         (size (with-open-file (in pn :element-type '(unsigned-byte 8)) (file-length in))))
    
    ;; OPTIMISATION : Si le fichier est < 5 MB, on le charge en RAM pour permettre la compression
    (let ((body (if (< size (* 5 1024 1024))
                    (alexandria:read-file-into-byte-vector pn)
                    ;; Sinon Stream classique (gros fichiers, vidéos...)
                    (lambda (out) 
                      (with-open-file (in pn :element-type '(unsigned-byte 8))
                        (let ((buf (make-array 4096 :element-type '(unsigned-byte 8))))
                          (loop for n = (read-sequence buf in)
                                while (plusp n)
                                do (funcall out (subseq buf 0 n)))))))))

      (make-instance 'lumen.core.http:response 
                     :status 200 
                     :headers `(("content-type" . ,ct)
                                ("accept-ranges" . "bytes")
                                ("content-length" . ,(write-to-string size))
                                ("cache-control" . ,(format nil "public, max-age=~D" file-cache-secs))
                                ,@(when lm `(("last-modified" . ,lm)))
                                ,@(when etag `(("etag" . ,etag))))
                     :body body))))

(defmethod maybe-spa-fallback (req (mw static-middleware) next)
  (let ((spa (slot-value mw 'spa-fallback))
        (root (truename (slot-value mw 'dir))))
    (if (and spa 
             (or (string= (lumen.core.http:req-method req) "GET") 
                 (string= (lumen.core.http:req-method req) "HEAD"))
             (probe-file (merge-pathnames spa root)))
        ;; Serve SPA index
        (let ((resp (%serve-file req (merge-pathnames spa root) mw)))
          (setf (lumen.core.http:resp-headers resp)
                (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "cache-control" "no-store"))
          resp)
        ;; Sinon pass
        (funcall next req))))

;;; ---------------------------------------------------------------------------
;;; 7. COOKIE MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defmiddleware cookie-middleware () (req next)
  (let* ((h (lumen.core.http:req-headers req))
         (raw (cdr (assoc "cookie" h :test #'string-equal))))
    (when raw
      (setf (lumen.core.http:req-cookies req)
            (lumen.core.http:parse-cookie-header raw)))
    (funcall next req)))

;; Helpers fonctionnels pour le user-land (restent utilisables)
(defun cookie (req name)
  (cdr (assoc name (lumen.core.http:req-cookies req) :test #'string=)))

(defun set-cookie! (resp name value &rest opts)
  (lumen.core.http:add-set-cookie
   resp
   (apply #'lumen.core.http:format-set-cookie name value opts)))

;;; ---------------------------------------------------------------------------
;;; 8. REQUEST ID MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %rand-hex (nbytes)
  (let ((hex "0123456789abcdef"))
    (with-output-to-string (s)
      (dotimes (i (* 2 nbytes)) (write-char (char hex (random 16)) s)))))

(defun make-request-id ()
  (let ((h (%rand-hex 16)))
    (format nil "~A-~A-4~A-~A~A-~A" (subseq h 0 8) (subseq h 8 12) (subseq h 13 16) (subseq h 16 17) (subseq h 17 20) (subseq h 20 32))))

(defmiddleware request-id-middleware
    ((header-name :initarg :header-name :initform "X-Request-ID"))
    (req next)
  
  (let* ((rid (or (lumen.core.http:ctx-get req :request-id) (make-request-id)))
         (resp (progn 
                 (lumen.core.http:ctx-set! req :request-id rid)
                 (funcall next req))))
    
    (setf (lumen.core.http:resp-headers resp)
          (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                     (slot-value mw 'header-name) 
                                     rid))
    resp))

;;; ---------------------------------------------------------------------------
;;; 9. ERROR WRAPPER MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defparameter *debug-p* nil) ;; Flag global de dev

(defun %default-error-renderer (e req)
  (declare (ignore req))
  (let* ((msg (princ-to-string e))
         (bt  (when *debug-p* (with-output-to-string (s) (ignore-errors (uiop:print-backtrace :stream s)))))
         (payload `((:error . ((:type . "internal") (:message . ,msg) ,@(when bt (list (cons :backtrace bt))))))))
    (lumen.core.http:respond-json payload :status 500)))

(defmiddleware error-middleware
    ((handler :initarg :handler :initform #'%default-error-renderer))
    (req next)
  
  (handler-case
      (funcall next req)
    (error (e)
      (funcall (slot-value mw 'handler) e req))))

;;; ---------------------------------------------------------------------------
;;; 10. FORM & MULTIPART MIDDLEWARES
;;; ---------------------------------------------------------------------------

(defmiddleware form-parser-middleware () (req next)
  (unless (lumen.core.http:ctx-get req :body-consumed)
    (let* ((headers (lumen.core.http:req-headers req))
           (ct  (cdr (assoc "content-type" headers :test #'string-equal)))
           (len (cdr (assoc "content-length" headers :test #'string-equal)))
           (len* (when len (parse-integer len :junk-allowed t))))
      (when (and ct len* (> len* 0) (search "application/x-www-form-urlencoded" (string-downcase ct)))
        (lumen.core.http:ctx-set! req :form (lumen.core.body:parse-urlencoded (lumen.core.http:req-body-stream req) len*))
        (lumen.core.http:ctx-set! req :body-consumed t))))
  (funcall next req))

(defmiddleware multipart-parser-middleware 
    ((max-size :initarg :max-size 
               :initform (* 10 1024 1024) ;; 10MB par défaut
               :accessor mw-max-size
               :documentation "Taille maximale en octets (Body complet)")) 
    (req next)
  
  (unless (lumen.core.http:ctx-get req :body-consumed)
    (let* ((h (lumen.core.http:req-headers req))
           (ct (cdr (assoc "content-type" h :test #'string-equal)))
           (len-str (cdr (assoc "content-length" h :test #'string-equal)))
           (len (and len-str (parse-integer len-str :junk-allowed t)))
           (limit (slot-value mw 'max-size)))

      ;; On ne s'active que pour du multipart
      (when (and ct (search "multipart/form-data" (string-downcase ct)))
        
        ;; 1. SECURITY CHECK : Content-Length vs Max-Size
        ;; Si le client annonce une taille trop grande, on rejette AVANT de lire le stream.
        (when (and len (> len limit))
          (return-from handle
            (lumen.core.http:respond-json 
             `((:error . "Payload Too Large")
               (:message . ,(format nil "The request body exceeds the limit of ~D bytes." limit)))
             :status 413)))

        ;; 2. SECURITY CHECK : Content-Length manquant ?
        ;; Pour du multipart, c'est louche. On peut décider de rejeter ou de laisser parser.
        ;; Ici, par prudence, on pourrait rejeter si on veut être strict.
        ;; (unless len (return-from handle (lumen.core.http:respond-json ... :status 411)))

        ;; 3. PARSING
        ;; On passe 'limit' au parseur pour qu'il s'arrête si le flux réel dépasse la limite 
        ;; (cas où le header mentirait).
        ;; Note: Supposons que parse-multipart accepte un argument :limit
        (let ((res (lumen.core.body:parse-multipart 
                    (lumen.core.http:req-body-stream req) 
                    (or len limit) ;; On donne une estimation ou la limite max
                    ct
		    :limit (slot-value mw 'max-size))))
          
          (when res
            (lumen.core.http:ctx-set! req :fields (cdr (assoc :fields res)))
            (lumen.core.http:ctx-set! req :files  (cdr (assoc :files  res)))
            (lumen.core.http:ctx-set! req :body-consumed t))))))
  
  ;; Passage au suivant
  (funcall next req))

;;; ---------------------------------------------------------------------------
;;; 11. BODY PARSER MIDDLEWARE (JSON)
;;; ---------------------------------------------------------------------------

(defmiddleware body-parser-middleware
    ((max-size :initarg :max-size 
               :initform (* 1 1024 1024) ;; 1 MB par défaut pour du JSON
               :accessor mw-max-size))
    (req next)
  
  (unless (lumen.core.http:ctx-get req :body-consumed)
    (let* ((h (lumen.core.http:req-headers req))
           (ct (cdr (assoc "content-type" h :test #'string-equal)))
           (len-str (cdr (assoc "content-length" h :test #'string-equal)))
           (len (and len-str (parse-integer len-str :junk-allowed t)))
           (limit (slot-value mw 'max-size)))

      (when (and ct (search "application/json" (string-downcase ct)))
        
        ;; 1. Security Check (Header)
        (when (and len (> len limit))
          (return-from handle
            (lumen.core.http:respond-json 
             `((:error . "Payload Too Large")
               (:message . ,(format nil "JSON body exceeds limit of ~D bytes" limit)))
             :status 413)))

        ;; 2. Parsing
        (when (and len (> len 0))
          (let ((val (lumen.core.body:parse-json 
                      (lumen.core.http:req-body-stream req) 
                      len 
                      :limit limit)))
            (when val
              (lumen.core.http:ctx-set! req :json val)
              (lumen.core.http:ctx-set! req :body-consumed t)))))))
  
  (funcall next req))

;;; ---------------------------------------------------------------------------
;;; 12. ETAG MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %fnv1a-64 (bytes)
  (let ((hash #xCBF29CE484222325) (prime #x100000001B3))
    (dotimes (i (length bytes))
      (setf hash (logxor hash (aref bytes i)))
      (setf hash (mod (* hash prime) (expt 2 64))))
    hash))

(defun compute-etag (body &key (weak t))
  (let* ((bytes (etypecase body
                  (string (trivial-utf-8:string-to-utf-8-bytes body))
                  ((vector (unsigned-byte 8)) body)
                  (null (trivial-utf-8:string-to-utf-8-bytes ""))))
         (h (format nil "~16,'0X" (lumen.core.middleware::%fnv1a-64 bytes)))
         (sig (format nil "~A-~D" h (length bytes))))
    
    ;; CORRECTION ICI : ~A au lieu de ~S
    (if weak
        (format nil "W/\"~A\"" sig) 
        (format nil "\"~A\"" sig))))

(defmiddleware etag-middleware
    ((weak :initarg :weak :initform t)
     (only-status :initarg :only-status :initform '(200))
     (add-cache-control :initarg :add-cache-control :initform nil))
    (req next)
  
  (let* ((resp (funcall next req))
         (status (lumen.core.http:resp-status resp)))
    
    (with-slots (weak only-status add-cache-control) mw
      (labels ((ensure-etag! (resp)
                 (let* ((hdrs (lumen.core.http:resp-headers resp))
                        (existing (cdr (assoc "etag" hdrs :test #'string=))))
                   (cond
                     (existing 
                      (when add-cache-control 
                        (setf (lumen.core.http:resp-headers resp) 
                              (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "cache-control" add-cache-control)))
                      existing)
                     ((functionp (lumen.core.http:resp-body resp)) nil) ;; Cannot compute for stream
                     (t (let ((val (compute-etag (lumen.core.http:resp-body resp) :weak weak)))
                          (setf (lumen.core.http:resp-headers resp) 
                                (lumen.utils:ensure-header hdrs "etag" val))
                          (when add-cache-control
                            (setf (lumen.core.http:resp-headers resp)
                                  (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "cache-control" add-cache-control)))
                          val))))))
        
        (if (and (member status only-status) (or (string= (lumen.core.http:req-method req) "GET") (string= (lumen.core.http:req-method req) "HEAD")))
            (let ((etag-val (ensure-etag! resp))
                  (inm (cdr (assoc "if-none-match" (lumen.core.http:req-headers req) :test #'string-equal))))
              (if (and etag-val inm (string= (string-trim " W/" inm) (string-trim " W/" etag-val))) ;; Simplified weak match
                  (make-instance 'lumen.core.http:response :status 304 :headers (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "etag" etag-val) :body "")
                  resp))
            resp)))))

;;; ---------------------------------------------------------------------------
;;; 13. COMPRESSION MIDDLEWARE
;;; ---------------------------------------------------------------------------
(defun %gzip-bytes (u8vec)
  (let ((out (flexi-streams:make-in-memory-output-stream :element-type '(unsigned-byte 8))))
    (salza2:with-compressor (c 'salza2:gzip-compressor 
                               :callback (lambda (chunk end)
                                           (write-sequence chunk out :end end)))
      (salza2:compress-octet-vector u8vec c))
    (flexi-streams:get-output-stream-sequence out)))

(defun %deflate-bytes (u8vec)
  (let ((out (flexi-streams:make-in-memory-output-stream :element-type '(unsigned-byte 8))))
    (salza2:with-compressor (c 'salza2:zlib-compressor 
                               :callback (lambda (chunk end)
                                           (write-sequence chunk out :end end)))
      (salza2:compress-octet-vector u8vec c))
    (flexi-streams:get-output-stream-sequence out)))

#|
(defmiddleware compression-middleware
    ((threshold :initarg :threshold :initform 1024)
     (min-ratio :initarg :min-ratio :initform 0.95)
     (skip-types :initarg :skip-types 
                 :initform '("image/" "video/" "audio/" "application/zip" "application/pdf"))
     (only-types :initarg :only-types :initform nil))
    (req next)
  
  (let* ((resp (funcall next req))
         (status (lumen.core.http:resp-status resp))
         (resp-h (lumen.core.http:resp-headers resp))
         (ct (cdr (assoc "content-type" resp-h :test #'string=))))
    
    (with-slots (threshold min-ratio skip-types only-types) mw
      (cond
        ((or (member status '(204 304)) (assoc "content-encoding" resp-h :test #'string=)) resp)
        ((and skip-types (some (lambda (p) (lumen.utils:str-prefix-p p (string-downcase (or ct "")))) skip-types)) resp)
        
        ;; Négociation
        (t (let* ((ae (cdr (assoc "accept-encoding" (lumen.core.http:req-headers req) :test #'string-equal)))
                  (wants-gzip (and ae (search "gzip" ae))))
             (if (not wants-gzip) 
                 resp
                 (let* ((orig (lumen.core.http:resp-body resp))
                        (bytes (etypecase orig
                                 (string (trivial-utf-8:string-to-utf-8-bytes orig))
                                 ((vector (unsigned-byte 8)) orig)
                                 (null #())))
                        (len (length bytes)))
                   
                   (if (< len threshold)
                       resp
                       (let* ((encoded (%gzip-bytes bytes))
                              (ratio (/ (length encoded) (float len))))
                         (if (> ratio min-ratio)
                             resp
                             (progn
                               (setf (lumen.core.http:resp-headers resp)
                                     (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "content-encoding" "gzip"))
                               
                               ;; 2. CORRECTION CRITIQUE : MISE A JOUR DU CONTENT-LENGTH
                               (setf (lumen.core.http:resp-headers resp)
                                     (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "content-length" (write-to-string (length encoded))))

                               ;; 3. REMPLACEMENT DU CORPS
                               (setf (lumen.core.http:resp-body resp) encoded)
                               resp))))))))))))
|#
(lumen.core.middleware:defmiddleware compression-middleware
    ((threshold :initarg :threshold :initform 1024)
     (min-ratio :initarg :min-ratio :initform 0.95)
     (skip-types :initarg :skip-types 
                 :initform '("image/" "video/" "audio/" "application/zip" "application/pdf"))
     (only-types :initarg :only-types :initform nil))
    (req next)
  
  (let* ((resp (funcall next req))
         (status (lumen.core.http:resp-status resp))
         (resp-h (lumen.core.http:resp-headers resp))
         (ct (cdr (assoc "content-type" resp-h :test #'string=))))
    
    (with-slots (threshold min-ratio skip-types only-types) mw
      (cond
        ((or (member status '(204 304)) (assoc "content-encoding" resp-h :test #'string=)) resp)
        ((and skip-types (some (lambda (p) (lumen.utils:str-prefix-p p (string-downcase (or ct "")))) skip-types)) resp)
        
        (t (let* ((ae (cdr (assoc "accept-encoding" (lumen.core.http:req-headers req) :test #'string-equal)))
                  (wants-gzip (and ae (search "gzip" ae))))
             
             (if (not wants-gzip) 
                 resp
                 (let* ((orig (lumen.core.http:resp-body resp))
                        (bytes (etypecase orig
                                 (string (trivial-utf-8:string-to-utf-8-bytes orig))
                                 ((vector (unsigned-byte 8)) orig)
				 (function #())
                                 (null #())))
                        (len (length bytes)))
                   
                   ;; DEBUG LOG
                   (format t "~&[COMPRESS] Path: ~A | Orig Size: ~D | Type: ~A~%" 
                           (lumen.core.http:req-path req) len (type-of orig))

                   (if (< len threshold)
                       resp
                       (let* ((raw-encoded (lumen.core.middleware::%gzip-bytes bytes))
                              ;; 1. COERCITION : On force un simple-array pour éviter les bugs de flux
                              (encoded (coerce raw-encoded '(simple-array (unsigned-byte 8) (*))))
                              (new-len (length encoded))
                              (ratio (/ new-len (float len))))
                         
                         (format t "~&[COMPRESS] -> Gzip Size: ~D (Ratio: ~F)~%" new-len ratio)

                         (if (> ratio min-ratio)
                             resp
                             (progn
                               (setf (lumen.core.http:resp-headers resp)
                                     (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "content-encoding" "gzip"))
                               
                               ;; 2. NETTOYAGE : On retire l'ancien content-length avant de mettre le nouveau
                               ;; (Gère les cas où il existe déjà en lowercase ou MixedCase)
                               (let ((clean-headers (remove "content-length" (lumen.core.http:resp-headers resp) 
                                                            :test #'string-equal :key #'car)))
                                 (setf (lumen.core.http:resp-headers resp)
                                       (acons "content-length" (write-to-string new-len) clean-headers)))

                               (setf (lumen.core.http:resp-body resp) encoded)
                               resp))))))))))))
;;; ---------------------------------------------------------------------------
;;; 14. LAST MODIFIED CONDITIONAL MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defmiddleware last-modified-middleware
    ((only-status :initarg :only-status :initform '(200)))
    (req next)
  
  (let* ((resp (funcall next req))
         (status (lumen.core.http:resp-status resp)))
    
    (with-slots (only-status) mw
      (if (and (member status only-status) (or (string= (lumen.core.http:req-method req) "GET") (string= (lumen.core.http:req-method req) "HEAD")))
          (let* ((req-h (lumen.core.http:req-headers req))
                 (resp-h (lumen.core.http:resp-headers resp))
                 (ims (cdr (assoc "if-modified-since" req-h :test #'string-equal)))
                 (inm (cdr (assoc "if-none-match" req-h :test #'string-equal)))
                 (lm (cdr (assoc "last-modified" resp-h :test #'string=))))
            
            ;; Priorité à ETag (If-None-Match) : si présent, on laisse ETag gérer
            (if (and lm ims (null inm))
                (let ((ims-ut (lumen.utils:parse-http-date ims))
                      (lm-ut (lumen.utils:parse-http-date lm)))
                  (if (and ims-ut lm-ut (>= ims-ut lm-ut))
                      (make-instance 'lumen.core.http:response :status 304 :headers (lumen.utils:ensure-header (copy-list resp-h) "last-modified" lm) :body "")
                      resp))
                resp))
          resp))))

;;; ---------------------------------------------------------------------------
;;; 15. SESSION MIDDLEWARE (Cookie Signed)
;;; ---------------------------------------------------------------------------

(defmiddleware session-middleware
    ((secret :initarg :secret :initform nil) ;; Requis
     (ttl :initarg :ttl :initform (* 24 3600))
     (cookie-name :initarg :cookie-name :initform "lumen_sid")
     (http-only :initarg :http-only :initform t)
     (secure :initarg :secure :initform nil)
     (path :initarg :path :initform "/"))
    (req next)
  
  (with-slots (secret ttl cookie-name http-only secure path) mw
    (assert (and secret (plusp (length secret))) () "session-middleware: :secret is required.")
    
    ;; 1. Lecture
    (let* ((raw (or (cdr (assoc cookie-name (lumen.core.http:req-cookies req) :test #'string=)) ""))
           (sid (and (> (length raw) 0) (lumen.core.session:verify-signed-sid raw secret)))
           (data (and sid (lumen.core.session:store-get sid))))
      
      (unless sid
        (setf sid (lumen.core.session:make-session-id))
        (setf data '()))
      
      ;; Injection Context
      (lumen.core.http:ctx-set! req :session-id sid)
      (lumen.core.http:ctx-set! req :session data)
      
      ;; 2. Exécution
      (let ((resp (funcall next req)))
        
        ;; 3. Persistance
        (let ((sid* (lumen.core.session:session-id req))
              (dat* (lumen.core.session:session-data req)))
          (lumen.core.session:store-put! sid* dat* ttl)
          
          ;; Cookie Refresh
          (lumen.core.http:add-set-cookie 
           resp 
           (lumen.core.http:format-set-cookie 
            cookie-name 
            (lumen.core.session:sign-sid sid* secret)
            :path path :http-only http-only :secure secure :max-age ttl)))
        resp))))

;;; ---------------------------------------------------------------------------
;;; 16. CSRF MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %random-token ()
  (cl-base64:usb8-array-to-base64-string (lumen.core.session:rand-bytes 32) :uri t))

(defmiddleware csrf-middleware
    ((cookie-name :initarg :cookie-name :initform "csrf_token")
     (header-name :initarg :header-name :initform "x-csrf-token")
     (methods :initarg :methods :initform '("POST" "PUT" "PATCH" "DELETE"))
     (path :initarg :path :initform "/")
     (skip-if :initarg :skip-if :initform nil))
    (req next)
  
  (with-slots (cookie-name header-name methods path skip-if) mw
    (let* ((method (lumen.core.http:req-method req))
           (mut? (member method methods :test #'string=))
           ;; Récupération ou génération Token en Session
           (tok (or (lumen.core.session:session-get req :csrf)
                    (let ((tkn (%random-token)))
                      (lumen.core.session:session-set! req :csrf tkn) 
                      tkn))))
      
      (labels ((emit (r) 
                 (lumen.core.http:add-set-cookie r (lumen.core.http:format-set-cookie cookie-name tok :path path :http-only nil))))
        
        (cond
          ;; Skip Check
          ((or (not mut?) (and skip-if (funcall skip-if req)))
           (let ((resp (funcall next req)))
             (emit resp)
             resp))
          
          ;; Verify
          (t
           (let* ((hdr (lumen.core.http:req-headers req))
                  (hdr-raw (cdr (assoc header-name hdr :test #'string-equal)))
                  (hdr-tok (if (and hdr-raw (string= hdr-raw cookie-name))
                               (cdr (assoc cookie-name (lumen.core.http:req-cookies req) :test #'string=))
                               hdr-raw))
                  (form (lumen.core.http:ctx-get req :form))
                  (field-tok (cdr (assoc "csrf_token" form :test #'string=)))
                  (ok (or (and hdr-tok (string= hdr-tok tok))
                          (and field-tok (string= field-tok tok)))))
             
             (if ok
                 (let ((resp (funcall next req)))
                   (emit resp)
                   resp)
                 (let ((resp (lumen.core.http:respond-json '((:error . "CSRF invalid")) :status 403)))
                   (emit resp)
                   resp)))))))))

;;; ---------------------------------------------------------------------------
;;; 17. AUTH JWT MIDDLEWARE (The Big One)
;;; ---------------------------------------------------------------------------

(defun %bearer-token (hdrs)
  (let ((raw (cdr (assoc "authorization" hdrs :test #'string-equal))))
    (when raw
      (let ((s (string-trim " " raw)))
        (if (and (>= (length s) 7) (string-equal "Bearer " s :end2 7))
            (subseq s 7) nil)))))

(defun %query-token (req keys)
  (let ((qp (lumen.core.http:req-query req)))
    (loop for k in keys thereis (cdr (assoc k qp :test #'string-equal)))))

(defun %normalize-scopes (v)
  (handler-case
      (let ((lst (cond ((null v) nil)
                       ((listp v) v)
                       ((stringp v) (cl-ppcre:split "\\s+" v)) ;; Space separated scopes
                       (t (list (princ-to-string v))))))
        (remove-duplicates (mapcar #'string lst) :test #'string=))
    (error () nil)))

(defmiddleware auth-middleware
    ((secret :initarg :secret :initform nil)
     (required-p :initarg :required-p :initform nil)
     (roles-allow :initarg :roles-allow :initform nil)
     (scopes-allow :initarg :scopes-allow :initform nil)
     (scopes-mode :initarg :scopes-mode :initform :any) ;; :any | :all
     (leeway :initarg :leeway :initform 60)
     (allow-query :initarg :allow-query :initform t)
     (admin-roles :initarg :admin-roles :initform '("admin"))
     (bypass-admin :initarg :bypass-admin :initform t))
    (req next)
  
  (with-slots (secret required-p roles-allow scopes-allow scopes-mode leeway allow-query admin-roles bypass-admin) mw
    (let* ((hdrs (lumen.core.http:req-headers req))
           (tok (or (%bearer-token hdrs)
                    (and allow-query (%query-token req '("access_token" "token")))
                    (cdr (assoc "access_token" (lumen.core.http:req-cookies req) :test #'string=)))))
      
      ;; 1. Decode & Hydrate Context
      (when (and tok secret)
        (multiple-value-bind (payload ok) 
            (ignore-errors (lumen.core.jwt:jwt-decode tok :secret secret :verify t :leeway leeway))
          (when ok
            (lumen.core.http:ctx-set! req :jwt payload)
            (lumen.core.http:ctx-set! req :user-id (cdr (assoc :sub payload))) ;; Standard claim
            ;; Tenant hydration
            (let ((tid (or (cdr (assoc :tenant-id payload)) (cdr (assoc :tenant payload)))))
              (when tid (lumen.core.http:ctx-set! req :tenant-id tid))))))
      
      ;; 2. Authorization Logic
      (let* ((jwt (lumen.core.http:ctx-get req :jwt))
             (role (and jwt (cdr (assoc :role jwt))))
             (scopes (%normalize-scopes (and jwt (cdr (assoc :scopes jwt)))))
             (is-admin (and role (member role admin-roles :test #'string=)))
             
             (roles-ok (or (null roles-allow)
                           (member role roles-allow :test #'string=)
                           (and is-admin bypass-admin)))
             
             (scopes-ok (or (null scopes-allow)
                            (if (eq scopes-mode :all)
                                (every (lambda (s) (member s scopes :test #'string=)) scopes-allow)
                                (some (lambda (s) (member s scopes :test #'string=)) scopes-allow))
                            (and is-admin bypass-admin))))
        
        (cond
          ;; Missing Token but Required
          ((and required-p (null jwt))
           (lumen.core.http:respond-json '((:error . "Unauthorized")) :status 401))
          
          ;; Forbidden (Role)
          ((not roles-ok)
           (lumen.core.http:respond-json '((:error . "Forbidden (Role)")) :status 403))
          
          ;; Forbidden (Scope)
          ((not scopes-ok)
           (lumen.core.http:respond-json '((:error . "Forbidden (Scope)")) :status 403))
          
          ;; Authorized
          (t 
           (when (and is-admin (or roles-allow scopes-allow))
             (lumen.core.http:ctx-set! req :auth-bypass "admin"))
           (funcall next req)))))))

;;; ---------------------------------------------------------------------------
;;; 18. AUTH FACTORIES (Aliases for compatibility)
;;; ---------------------------------------------------------------------------

(defun auth-required (&key (roles nil) (scopes nil))
  "Factory pour créer un middleware Auth strict."
  (make-instance 'auth-middleware 
                 :required-p t 
                 :roles-allow roles
                 :scopes-allow scopes
                 :secret lumen.core.jwt:*jwt-secret*))

(defun roles-allowed (roles)
  "Factory raccourci pour restreindre par rôle."
  (auth-required :roles roles))

;;; ---------------------------------------------------------------------------
;;; 19. HTTPS REDIRECT MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %request-secure-p (req)
  (or (lumen.core.http:ctx-get req :secure)
      (let* ((h (lumen.core.http:req-headers req))
             (xfp (cdr (assoc "x-forwarded-proto" h :test #'string-equal)))
             (xfs (cdr (assoc "x-forwarded-ssl"   h :test #'string-equal))))
        (or (and xfp (string-equal xfp "https"))
            (and xfs (string-equal xfs "on"))))))

(defun %https-location (req ssl-port)
  (let* ((h (lumen.core.http:req-headers req))
         (host (or (cdr (assoc "x-forwarded-host" h :test #'string-equal))
                   (cdr (assoc "host" h :test #'string-equal)) "localhost"))
         (host0 (subseq host 0 (position #\: host)))
         (path (lumen.core.http:req-path req))
         (q (lumen.core.http:req-query req))
         (qs (cond ((stringp q) (if (plusp (length q)) (format nil "?~A" q) ""))
                   ((listp q) (if q (format nil "?~{~A=~A~^&~}" (loop for (k . v) in q collect k collect v)) ""))
                   (t "")))
         (port-suffix (if (and ssl-port (not (= ssl-port 443))) (format nil ":~D" ssl-port) "")))
    (format nil "https://~A~A~A~A" host0 port-suffix path qs)))

(defun %path-has-prefix-p (path prefix)
  (and (<= (length prefix) (length path)) (string= prefix path :end2 (length prefix))))

(defmiddleware https-redirect-middleware
    ((prefixes :initarg :prefixes :initform '("/"))
     (except-prefixes :initarg :except-prefixes :initform nil)
     (ssl-port :initarg :ssl-port :initform 443)
     (hsts :initarg :hsts :initform nil)
     (hsts-max-age :initarg :hsts-max-age :initform 15552000)
     (hsts-subdomains :initarg :hsts-subdomains :initform t)
     (hsts-preload :initarg :hsts-preload :initform nil))
    (req next)
  
  (with-slots (prefixes except-prefixes ssl-port hsts hsts-max-age hsts-subdomains hsts-preload) mw
    (let* ((path (lumen.core.http:req-path req))
           (in-scope (some (lambda (p) (%path-has-prefix-p path p)) prefixes))
           (excluded (some (lambda (p) (%path-has-prefix-p path p)) except-prefixes))
           (secure? (%request-secure-p req)))
      
      (cond
        ;; Déjà HTTPS
        (secure? 
         (let ((resp (funcall next req)))
           (when (and hsts (not excluded))
             (let ((val (with-output-to-string (s)
                          (format s "max-age=~D" hsts-max-age)
                          (when hsts-subdomains (format s "; includeSubDomains"))
                          (when hsts-preload (format s "; preload")))))
               (setf (lumen.core.http:resp-headers resp)
                     (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "strict-transport-security" val))))
           resp))
        
        ;; HTTP -> Redirect
        ((and in-scope (not excluded))
         (make-instance 'lumen.core.http:response 
                        :status 301 
                        :headers `(("location" . ,(%https-location req ssl-port)) ("cache-control" . "no-store"))
                        :body ""))
        
        ;; HTTP Hors scope -> Next
        (t (funcall next req))))))

;;; ---------------------------------------------------------------------------
;;; 20. RATE LIMIT MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defmiddleware rate-limit-middleware
    ((capacity :initarg :capacity :initform 10)
     (refill :initarg :refill :initform 1)
     (route-key :initarg :route-key :initform "default"))
    (req next)
  
  (with-slots (capacity refill route-key) mw
    (if (lumen.core.ratelimit:allow? req :capacity capacity :refill-per-sec refill :route-key route-key)
        (funcall next req)
        (lumen.core.http:respond-json '((:error . ((:type . "rate_limit") (:message . "Too Many Requests")))) :status 429))))

;;; ---------------------------------------------------------------------------
;;; 21. REQUEST TIMEOUT MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defmiddleware timeout-middleware
    ((ms :initarg :ms :initform 5000))
    (req next)
  
  (let* ((lock (bt:make-lock "rt-lock"))
         (cv   (bt:make-condition-variable))
         (resp nil)
         (done nil)
         ;; 1. CAPTURE : On prend le contexte du thread principal (HTTP Request)
         (parent-ctx (lumen.core.trace:current-context)))
    
    (bt:make-thread
     (lambda ()
       ;; 2. PROPAGATION : On l'injecte dans ce nouveau thread anonyme
       (lumen.core.trace:with-propagated-context parent-ctx
         
         ;; Le reste est inchangé, mais maintenant 'next' verra le contexte parent !
         (let ((r (funcall next req)))
           (bt:with-lock-held (lock)
             (setf resp r done t)
             (bt:condition-notify cv))))))
    
    (bt:with-lock-held (lock)
      (unless done
        (unless (bt:condition-wait cv lock :timeout (/ (slot-value mw 'ms) 1000.0))
          (return-from handle 
            (lumen.core.http:respond-json '((:error . ((:type . "timeout") (:message . "gateway timeout")))) :status 504)))))
    resp))

;;; ---------------------------------------------------------------------------
;;; 22. MAX BODY SIZE MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defmiddleware max-body-size-middleware
    ((bytes :initarg :bytes :initform (* 2 1024 1024))) ;; 2MB
    (req next)
  
  (let* ((clen (cdr (assoc "content-length" (lumen.core.http:req-headers req) :test #'string-equal)))
         (n (and clen (parse-integer clen :junk-allowed t)))
         (limit (slot-value mw 'bytes)))
    
    (when (and n (> n limit))
      (return-from handle
        (lumen.core.http:respond-json 
         `((:error . ((:type . "payload_too_large") (:message . ,(format nil "max ~D bytes" limit))))) 
         :status 413)))
    
    ;; Injection de la limite globale pour les parseurs downstream qui n'auraient pas leur propre config
    (lumen.core.http:ctx-set! req :max-body-size limit)
    (funcall next req)))

;;; ---------------------------------------------------------------------------
;;; 23. REQUEST CONTEXT MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %peer-ip (req)
  (or (let* ((xff (cdr (assoc "x-forwarded-for" (lumen.core.http:req-headers req) :test #'string-equal))))
        (when xff (string-trim " " (car (uiop:split-string xff :separator ",")))))
      (cdr (assoc "x-real-ip" (lumen.core.http:req-headers req) :test #'string-equal))
      (lumen.core.http:ctx-get req :remote-addr)
      "-"))

(defun %user-agent (req)
  (or (cdr (assoc "user-agent" (lumen.core.http:req-headers req) :test #'string-equal)) ""))

(defmiddleware context-middleware
    ((audit-enabled-p :initarg :audit-enabled-p :initform t)
     (generate-request-id-p :initarg :generate-request-id-p :initform nil)) ;; Souvent géré par request-id-middleware
    (req next)
  
  (with-slots (audit-enabled-p generate-request-id-p) mw
    ;; 1. Request ID (Fallback)
    (when (and generate-request-id-p (not (lumen.core.http:ctx-get req :request-id)))
      (lumen.core.http:ctx-set! req :request-id (make-request-id)))
    
    ;; 2. IP / UA
    (lumen.core.http:ctx-set! req :ip (%peer-ip req))
    (lumen.core.http:ctx-set! req :ua (%user-agent req))
    
    ;; 3. JWT Hydration (Si auth-middleware est passé avant)
    (let ((jwt (lumen.core.http:ctx-get req :jwt)))
      (when jwt
        (lumen.core.http:ctx-set! req :actor-id (or (cdr (assoc :sub jwt)) (cdr (assoc :user-id jwt))))
        (lumen.core.http:ctx-set! req :role (cdr (assoc :role jwt)))
        (lumen.core.http:ctx-set! req :scopes (%normalize-scopes (cdr (assoc :scopes jwt))))
        
        (let ((tid (or (cdr (assoc :tenant-id jwt)) (cdr (assoc :tenant jwt)))))
          (when tid (lumen.core.http:ctx-set! req :tenant-id tid)))))
    
    ;; 4. Audit Flag
    (lumen.core.http:ctx-set! req :audit? audit-enabled-p)
    
    (funcall next req)))

;;; ---------------------------------------------------------------------------
;;; 24. ACCESS LOG JSON MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %rfc3339-now ()
  (local-time:format-timestring nil (local-time:now) 
                                :format '(:year "-" (:month 2) "-" (:day 2) "T" (:hour 2) ":" (:min 2) ":" (:sec 2) "Z")))

(defun %normalize-fields (fields req resp ms bytes)
  (cond ((null fields) nil)
        ((functionp fields) (funcall fields req resp ms bytes))
        ((listp fields) fields)
        (t nil)))

(defun %open-log-file (path)
  (open path :direction :output :if-exists :append :if-does-not-exist :create :external-format :utf-8))

(defmiddleware access-log-middleware
    ((stream :initarg :stream :initform *standard-output*)
     (to-file :initarg :to-file :initform nil)
     (fields :initarg :fields :initform nil)
     (lock :initform (bt:make-lock "access-log")))
    (req next)
  
  ;; Init output stream (Lazy file open ?)
  ;; Pour simplifier, on suppose que le stream est passé ou géré en amont.
  ;; Si to-file est set, on pourrait ouvrir/fermer, mais c'est lent.
  ;; Mieux : ouvrir à l'init.
  
  (let* ((t0 (get-internal-real-time))
         (resp (funcall next req))
         (ms (round (/ (- (get-internal-real-time) t0) (/ internal-time-units-per-second 1000.0))))
         (body (lumen.core.http:resp-body resp))
         (bytes (cond ((stringp body) (length (trivial-utf-8:string-to-utf-8-bytes body)))
                      ((typep body '(simple-array (unsigned-byte 8) (*))) (length body))
                      (t nil)))
         (base `((:ts . ,(%rfc3339-now))
                 (:request_id . ,(or (lumen.core.http:ctx-get req :request-id) ""))
                 (:ip . ,(lumen.core.http:ctx-get req :ip))
                 (:method . ,(lumen.core.http:req-method req))
                 (:path . ,(lumen.core.http:req-path req))
                 (:status . ,(lumen.core.http:resp-status resp))
                 (:ms . ,ms)
                 ,@(when bytes `((:bytes . ,bytes)))))
         (extra (%normalize-fields (slot-value mw 'fields) req resp ms bytes))
         (log-entry (append base extra))
         (out (slot-value mw 'stream)))
    
    ;; Gestion fichier (réouverture simple ou stream dédié)
    (when (slot-value mw 'to-file)
      (setf out (%open-log-file (slot-value mw 'to-file))))
    
    (ignore-errors
      (bt:with-lock-held ((slot-value mw 'lock))
        (write-string (cl-json:encode-json-to-string log-entry) out)
        (terpri out)
        (finish-output out)))
    
    (when (and (slot-value mw 'to-file) (streamp out))
      (close out))
    
    resp))

;;; ---------------------------------------------------------------------------
;;; ROUTER-DISPATCH MIDDLEWARE
;;; ---------------------------------------------------------------------------
(defmiddleware router-middleware
    ;; Pas de config spéciale pour l'instant, le routeur utilise *routes* global
    ;; Idée d'évolution : ajouter un slot 'routes' pour avoir des routeurs isolés !
    ()
    (req next)
  
  (let ((response (dispatch req)))
    ;; LOGIQUE DE PASS-THROUGH
    ;; Si le routeur renvoie 404 "Not Found" (le défaut de dispatch),
    ;; cela signifie qu'aucune route n'a matché.
    ;; Dans ce cas, on appelle 'next' pour laisser une chance à la suite 
    ;; (ex: un gestionnaire de 404 custom ou un autre routeur).
    
    (if (and (= (lumen.core.http:resp-status response) 404)
             (equal (lumen.core.http:resp-body response) "Not Found"))
        (funcall next req)
        
        ;; Sinon (Match 200, ou 405 Method Not Allowed, ou 404 explicite d'une route)
        ;; On retourne la réponse, on arrête la chaîne ici.
        response)))

(defun %maybe-real-host-from-proxy (req)
  "Si un mw 'trust-proxy' alimente ctx [:real-host], l’utiliser en priorité."
  (or (ctx-get req :real-host)
      (cdr (assoc "x-forwarded-host" (req-headers req) :test #'string-equal))
      (cdr (assoc "x-real-host"      (req-headers req) :test #'string-equal))))

(defun %host-header (req)
  (or (%maybe-real-host-from-proxy req)
      (cdr (assoc "host" (req-headers req) :test #'string-equal))))


;;; ---------------------------------------------------------------------------
;;; 26. TRUST PROXY MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %split (s ch) (uiop:split-string s :separator (string ch)))
(defun %trim (s) (string-trim '(#\Space #\Tab #\Return #\Newline) s))
(defun %down (s) (string-downcase s))

(defun %parse-ipv4 (s)
  (let* ((p (mapcar (lambda (x) (parse-integer x :junk-allowed t)) (%split s #\.))))
    (when (and (= (length p) 4) (every (lambda (n) (and n (<= 0 n 255))) p))
      (reduce (lambda (a n) (+ (ash a 8) n)) p :initial-value 0))))

(defun %cidr-range (cidr)
  (let* ((pos (position #\/ cidr))
         (ip (subseq cidr 0 pos))
         (mask (parse-integer (subseq cidr (1+ pos)) :junk-allowed t))
         (n (%parse-ipv4 ip))
         (host-bits (- 32 mask))
         (lo (logand n (ldb (byte 32 host-bits) #xffffffff)))
         (hi (+ lo (1- (ash 1 host-bits)))))
    (cons lo hi)))

(defun %ip-in-cidr-p (ip cidr)
  (let* ((n (%parse-ipv4 ip)) (rg (%cidr-range cidr)))
    (and n (<= (car rg) n) (<= n (cdr rg)))))

(defun %trusted-p (src specs)
  (etypecase specs
    (symbol (cond ((eq specs :none) nil) ((eq specs :always) t) (t nil)))
    (list (some (lambda (x) (if (find #\/ x) (%ip-in-cidr-p src x) (eql (%parse-ipv4 x) (%parse-ipv4 src)))) specs))))

(defun %parse-forwarded (s)
  (let* ((first (car (uiop:split-string s :separator ",")))
         (pairs (mapcar #'%trim (uiop:split-string first :separator ";"))))
    (loop with out = '()
          for p in pairs
          for pos = (position #\= p)
          when pos do (let ((k (%down (%trim (subseq p 0 pos)))) (v (%trim (subseq p (1+ pos)))))
                        (setf out (list* (intern (string-upcase k) :keyword) (string-trim '(#\") v) out)))
          finally (return out))))

(defmiddleware trust-proxy-middleware
    ((trusted :initarg :trusted 
              :initform nil ;; nil = lecture depuis config, sinon :always, :none, ou list
              :accessor mw-trusted))
    (req next)
  
  (with-slots (trusted) mw
    (let* ((specs (or trusted 
                      (lumen.core.config:cfg-get-list :http/trusted-proxies)
                      :none))
           (src (or (lumen.core.http:ctx-get req :remote-addr) "-")))
      
      (if (not (%trusted-p src specs))
          (funcall next req)
          (let* ((h (lumen.core.http:req-headers req))
                 (fwd (%parse-forwarded (cdr (assoc "forwarded" h :test #'string-equal))))
                 (xff (cdr (assoc "x-forwarded-for" h :test #'string-equal)))
                 (xfh (cdr (assoc "x-forwarded-host" h :test #'string-equal)))
                 (xfp (cdr (assoc "x-forwarded-proto" h :test #'string-equal)))
                 
                 (real-ip   (or (getf fwd :|FOR|) (and xff (%trim (car (uiop:split-string xff :separator ",")))) src))
                 (real-prot (or (getf fwd :|PROTO|) xfp (if (lumen.core.http:ctx-get req :secure) "https" "http")))
                 (real-host (or (getf fwd :|HOST|) xfh)))
            
            ;; Hydratation context
            (when real-ip   (lumen.core.http:ctx-set! req :client-ip real-ip))
            (when real-prot (lumen.core.http:ctx-set! req :scheme real-prot))
            (when real-host (lumen.core.http:ctx-set! req :host real-host)) ;; Utilisé par tenant-middleware ensuite
            
            (funcall next req))))))

;;; ---------------------------------------------------------------------------
;;; 26. INTROSPECTION
;;; ---------------------------------------------------------------------------
(defmiddleware inspector-middleware
    ;; On lui passe le pipeline qu'il doit inspecter
    ((target-pipeline :initarg :pipeline 
                      :initform nil 
                      :accessor inspector-target)
     (path :initarg :path :initform "/_pipeline")) 
    (req next)
  
  (if (and (string= (lumen.core.http:req-path req) (slot-value mw 'path))
           (string= (lumen.core.http:req-method req) "GET"))
      
      ;; 1. Interception : On renvoie le JSON du pipeline
      (let* ((p (slot-value mw 'target-pipeline))
             (data (if p 
                       (lumen.core.pipeline:pipeline-to-list p)
                       '((:error . "No pipeline attached to inspector")))))
        (lumen.core.http:respond-json 
         `((:meta . ((:version . "Lumen 2.0") (:env . ,(lumen.core.config:cfg-profile))))
           (:pipeline . ,data))))
      
      ;; 2. Sinon : Passe-plat
      (funcall next req)))

(defmiddleware trace-middleware
    ((threshold-ms :initarg :threshold-ms 
                   :initform 500 
                   :documentation "Log si plus lent que X ms")
     (force-header :initarg :force-header 
                   :initform "x-trace-debug"))
    (req next)
  
  ;; 1. Nettoyage préventif (au cas où le thread est réutilisé)
  (lumen.core.trace::%clear-thread-ctx)
  
  (lumen.core.trace:with-tracing ("HTTP Request" 
                                  :path (lumen.core.http:req-path req)
                                  :method (lumen.core.http:req-method req))
    
    (let ((resp (funcall next req)))
      
      ;; Récupération du contexte pour analyse
      (let* ((ctx (lumen.core.trace::%get-thread-ctx))
             (root (lumen.core.trace::ctx-root ctx))
             (now (get-internal-real-time))
             (start (if root (lumen.core.trace::trace-start root) now))
             (dur (lumen.core.trace::%ms (- now start)))
             (debug-requested-p 
              (cdr (assoc (slot-value mw 'force-header) 
                          (lumen.core.http:req-headers req) 
                          :test #'string-equal))))
        
        ;; On force la fin de la racine pour l'affichage correct
        (when root (setf (lumen.core.trace::trace-end root) now))

        (when (or (>= (lumen.core.http:resp-status resp) 500)
                  (> dur (slot-value mw 'threshold-ms))
                  debug-requested-p)
          
          (format t "~&[TRACE] Triggered by: ~A (Duration: ~,2Fms)~%" 
                  (cond ((>= (lumen.core.http:resp-status resp) 500) "Error 500")
                        (debug-requested-p "Header Request")
                        (t "Slow Request"))
                  dur)
          (lumen.core.trace:print-trace-waterfall))
        
        ;; Nettoyage final pour ne pas fuir de mémoire
        (lumen.core.trace::%clear-thread-ctx))
      
      resp)))
