(in-package :cl)

(defpackage :lumen.data.db
  (:use :cl)
  (:import-from :postmodern 
   :connect-toplevel :disconnect-toplevel :with-connection :execute :connected-p)
  (:import-from :cl-postgres :exec-query)
  (:import-from :lumen.data.config :db-config)
  (:import-from :lumen.data.prepare 
   :get-prepared-plan :reset-prepare-cache :*prepare-cache-ttl-ms*)
  (:import-from :lumen.data.errors 
   :translate-db-error :map-db-error :retryable-db-error-p)
  (:import-from :lumen.core.error 
   :application-error :application-error-message :application-error-code)
  (:import-from :lumen.data.metrics 
   :record-query-latency :record-slow-query)
  
  (:export :start! :stop! :with-tx :query-a :query-1a :exec
           :with-conn :ensure-connection :with-rollback
           :*connection-mode* :with-statement-timeout :run-in-transaction
           :*default-statement-timeout-ms* :*slow-query-ms*))

(in-package :lumen.data.db)

(defvar *started* nil)
(defvar *current-config* nil)
(defvar *connection-mode* :pooled-native)

;;; --- POOL INFRASTRUCTURE (Inchangé) ---
(defstruct pool
  (available-conns '())
  (lock (bt:make-lock "db-pool-lock"))
  (semaphore nil)
  (config nil))

(defvar *db-pool* nil)

(defun %create-connection (cfg)
  (handler-case
      (postmodern:connect 
       (getf cfg :database)
       (getf cfg :user)
       (getf cfg :password)
       (or (getf cfg :host) "localhost")
       :port (or (getf cfg :port) 5432)
       :use-ssl (case (getf cfg :sslmode) (:require :yes) (t :no))
       :pooled-p t)
    (error (c)
      (format t "~&[DB] Fatal: Failed to create connection: ~A~%" c)
      (error c))))

(defun init-pool! (config)
  (let ((size (or (getf config :pool-size) 10)))
    (setf *db-pool* (make-pool 
                     :semaphore (bt:make-semaphore :count size)
                     :config config))))

(defun destroy-pool! ()
  (when *db-pool*
    (bt:with-lock-held ((pool-lock *db-pool*))
      (dolist (c (pool-available-conns *db-pool*))
        (ignore-errors (postmodern:disconnect c))))
    (setf *db-pool* nil)))

(defun start! (&key (config (lumen.data.config:db-config)))
  (when *started* (stop!))
  (setf *current-config* config
        *connection-mode* (or (getf config :db-connection-mode) :pooled-native))
  (when (eq *connection-mode* :pooled-native)
    (init-pool! config)
    (format t "~&[DB] Pool started (size: ~A).~%" (getf config :pool-size 10)))
  (setf *started* t))

(defun stop! ()
  (when *started*
    (when *db-pool* (destroy-pool!))
    (setf *started* nil))
  t)

;;; --- CONNECTION MANAGEMENT (Inchangé) ---
(defvar *in-connection* nil)

(defun %checkout-connection (pool)
  (let ((conn nil))
    (bt:with-lock-held ((pool-lock pool))
      (setf conn (pop (pool-available-conns pool))))
    (if conn
        (if (postmodern:connected-p conn)
            conn
            (progn (ignore-errors (postmodern:disconnect conn))
                   (%create-connection (pool-config pool))))
        (%create-connection (pool-config pool)))))

(defun %checkin-connection (pool conn)
  (when (and pool conn (postmodern:connected-p conn))
    (bt:with-lock-held ((pool-lock pool))
      (push conn (pool-available-conns pool)))))

(defun call-with-conn (thunk &key (cfg (or *current-config* (lumen.data.config:db-config))))
  (if *in-connection*
      (funcall thunk)
      (if *db-pool*
          (let ((pool *db-pool*))
            (bt:wait-on-semaphore (pool-semaphore pool))
            (let ((conn nil))
              (unwind-protect
                   (progn
                     (setf conn (%checkout-connection pool))
                     (let ((postmodern:*database* conn) (*in-connection* t))
                       (funcall thunk)))
                (if conn (%checkin-connection pool conn) nil)
                (bt:signal-semaphore (pool-semaphore pool)))))
          (let ((conn (%create-connection cfg)))
            (unwind-protect
                 (let ((postmodern:*database* conn) (*in-connection* t))
                   (funcall thunk))
              (ignore-errors (postmodern:disconnect conn)))))))

(defmacro ensure-connection (&body body)
  `(call-with-conn (lambda () ,@body)))

(defmacro with-conn (&optional opts &body body)
  (if (and opts (listp opts) (or (null opts) (keywordp (first opts))))
      `(call-with-conn (lambda () ,@body) ,@opts)
      `(call-with-conn (lambda () ,opts ,@body))))

;;; --- HELPERS ---
(defvar *default-statement-timeout-ms* nil)
(defvar *slow-query-ms* 500.0)

(defun %raw-exec (sql)
  (cl-postgres:exec-query postmodern:*database* sql))

(defmacro with-statement-timeout ((ms) &body body)
  `(let ((%ms ,ms))
     (if (and %ms (> %ms 0))
         (progn (ignore-errors (%raw-exec (format nil "SET LOCAL statement_timeout = ~d" (truncate %ms))))
                ,@body)
         (progn ,@body))))

;;; --- EXEC AVEC DEBUG LOGS ET PATCH ---

(defun exec (sql &rest params)
  "Execute INSERT/UPDATE/DELETE. 
   NE CAPTURE PAS LES ERREURS : laisse run-in-transaction gérer le Rollback."
  
  (format t "~&EXEC:SQL: ~A~%" sql)
  
  (let* ((kpos (position-if (lambda (x)
                              (and (keywordp x)
                                   (not (member x '(:null :default) :test #'eq))))
                            params))
         (args (if kpos (subseq params 0 kpos) params))
         (opts (if kpos (subseq params kpos) '()))
         (timeout-ms (getf opts :timeout-ms (or *default-statement-timeout-ms* nil)))
         (t0 (get-internal-real-time)))

    ;;(format t "~&EXEC:ARGS: ~A~%" args)
    (format t "~&EXEC:OPTS: ~A~%" opts)

    ;; On utilise unwind-protect ou simplement rien pour laisser l'erreur passer
    (with-statement-timeout (timeout-ms)
      (let* ((lower (string-downcase sql))
             (has-returning (search "returning" lower))
             affected ret)
            
        (if has-returning
            ;; RETURNING
            (let* ((fn  (get-prepared-plan sql :format :alist))
                   (row (apply fn args)))
              (setf affected (if row 1 0)
                    ret row))
            
            ;; NO RETURNING
            (let* ((fn (get-prepared-plan sql :format :none))
                   (n  (or (apply fn args) 0)))
              (setf affected n
                    ret nil)))
            
        ;; Métriques
        (let ((elapsed-ms (* 1000.0 (/ (- (get-internal-real-time) t0)
                                       cl:internal-time-units-per-second))))
          (record-query-latency sql elapsed-ms (or affected 0))
          (when (and *slow-query-ms* (>= elapsed-ms *slow-query-ms*))
            (lumen.data.metrics:record-slow-query
             sql elapsed-ms :params args :affected affected)))
	
        (format t "~&[EXEC DEBUG] SQL executed.~%Affected: ~A~%Ret: ~A~%" affected  ret)    
        (values affected ret)))))

(defun query-a (sql &rest params)
  "Execute SELECT. Return alist."
  (ensure-connection
    (let* ((t0 (get-internal-real-time))
           (fn (get-prepared-plan sql :format :alists)))
      
      ;; Patch identique pour query-a, par sécurité
      (let ((real-params
             (if (and (= 1 (length params))
                      (listp (first params))
                      (> (cl-ppcre:count-matches "\\$\\d+" sql) 1))
                 (first params)
                 params)))
        
        (handler-case
            (let ((rows (apply fn real-params)))
              (values rows (length rows)))
          (error (c)
            (error (translate-db-error c))))))))

(defun query-1a (sql &rest params)
  (print sql)
  (print params)
  (first (apply #'query-a sql params)))

;;; --- TRANSACTION MANAGEMENT ---

(defun run-in-transaction (thunk &key (retries 0) (sleep-ms 50))
  (let ((attempt 0))
    (loop
      (let ((result 
             (ensure-connection
               (handler-case
                   (progn
                     (%raw-exec "BEGIN")
                     (let ((res (funcall thunk)))
		       (print "YYYYYYYY")
                       (%raw-exec "COMMIT")
                       (cons :ok res)))
                 (error (c)
                   (ignore-errors (%raw-exec "ROLLBACK"))
                   (cons :error c))))))
        
        (if (eq (car result) :ok)
            (return (cdr result))
            
            (let* ((err (cdr result))
                   (is-app (typep err 'lumen.core.error:application-error))
                   (mapped (unless is-app (lumen.data.errors:map-db-error err))))
	      (format t "~&[TX] Analysis: App? ~A Mapped? ~A~%" is-app mapped)
              (when is-app (error err))
              (if (and mapped (< attempt retries) (lumen.data.errors:retryable-db-error-p mapped))
                  (progn
                    (incf attempt)
                    (format t "~&[DB] Retry TX (~A/~A) due to ~A...~%" attempt retries mapped)
                    (sleep (/ (max 50 sleep-ms) 1000.0)))
                  (error (or mapped err)))))))))

;;; ----------------------------------------------------------------------------
;;; TRANSACTION MANAGEMENT (BLOCK/LABELS - SAFE FLOW)
;;; ----------------------------------------------------------------------------
(defun run-in-transaction (thunk &key (retries 0) (sleep-ms 50))
  "Transaction avec flux de contrôle explicite (pas de loop/return implicite)."
  (let ((attempt 0))
    (block txn-block
      (labels ((retry-loop ()
                 ;; 1. Exécution
                 (let ((result 
                        (ensure-connection
                          (handler-case
                              (progn
                                (%raw-exec "BEGIN")
                                (let ((res (funcall thunk)))
                                  (%raw-exec "COMMIT")
                                  (list :ok res)))
                            (error (c)
                              (ignore-errors (%raw-exec "ROLLBACK"))
                              (list :error c))))))
                   
                   ;; 2. Analyse
                   (if (eq (first result) :ok)
                       (return-from txn-block (second result))
                       
                       (let* ((err (second result))
                              (is-app (typep err 'lumen.core.error:application-error))
                              (mapped (unless is-app (lumen.data.errors:map-db-error err))))
                         
                         (when is-app (error err))
                         
                         (if (and mapped 
                                  (< attempt retries) 
                                  (lumen.data.errors:retryable-db-error-p mapped))
                             (progn
                               (incf attempt)
                               (format t "~&[DB] Retry TX (~A/~A)...~%" attempt retries)
                               (sleep (/ (max 50 sleep-ms) 1000.0))
                               (retry-loop)) ;; Appel récursif sûr
                             
                             (error (or mapped err))))))))
        
        (retry-loop)))))

(defmacro with-tx ((&key retries) &body body)
  `(run-in-transaction (lambda () ,@body) :retries ,(or retries 0)))
