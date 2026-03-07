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
  (:import-from :lumen.core.trace :with-tracing)
  
  (:export :start! :stop! :with-tx :query-a :query-1a :exec
           :with-conn :ensure-connection :with-rollback
           :*connection-mode* :with-statement-timeout :run-in-transaction
           :*default-statement-timeout-ms* :*slow-query-ms*
	   :db-session-middleware :connection-per-request-middleware :with-db-app))

(in-package :lumen.data.db)

;; --- REGISTRE DES POOLS ---
(defvar *pools-registry* (make-hash-table :test 'equal)
  "Stocke les configurations et pools par nom d'application (ou :default).")

;; On garde ces variables pour la compatibilité et le binding dynamique
;; *db-pool* et *current-config* deviendront "Thread-Local" grâce au middleware
;; Remontées dans lumen.core.context
;;(defvar *db-pool* nil)
;;(defvar *current-config* nil)

(defvar *started* nil)
(defvar *connection-mode* :pooled-native)
(defvar *in-transaction* nil "Indique si le thread courant est déjà dans une transaction SQL.")
;; Variable globale pour suivre la profondeur d'imbrication
(defparameter *tx-depth* 0)

;;; --- POOL INFRASTRUCTURE (Inchangé) ---
(defstruct pool
  (available-conns '())
  (lock (bt:make-lock "db-pool-lock"))
  (semaphore nil)
  (config nil))

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

;; helper pour récupérer le couple pool/config
(defun get-db-context (name)
  (gethash name *pools-registry*))

(defun init-pool! (config)
  (let ((size (or (getf config :pool-size) 10)))
    (make-pool 
     :semaphore (bt:make-semaphore :count size)
     :config config)))

(defun destroy-pool! ()
  (when lumen.core.context:*db-pool*
    (bt:with-lock-held ((pool-lock lumen.core.context:*db-pool*))
      (dolist (c (pool-available-conns lumen.core.context:*db-pool*))
        (ignore-errors (postmodern:disconnect c))))
    (setf lumen.core.context:*db-pool* nil)))

(defun start! (&key (config (lumen.data.config:db-config)) (name :default))
  "Démarre une pool pour un contexte donné (NAME).
   Si NAME est :default, on initialise aussi les variables globales."
  
  ;; 1. Arrêter si une pool existe déjà pour ce nom
  (stop! :name name)

  (let ((connection-mode (or (getf config :db-connection-mode) :pooled-native)))
    
    (if (eq connection-mode :pooled-native)
        (let ((new-pool (init-pool! config)))
          ;; On stocke dans le registre : (POOL . CONFIG)
          (setf (gethash name *pools-registry*) (cons new-pool config))
          (format t "~&[DB] Pool started for app ~S (size: ~A).~%" name (getf config :pool-size 10))
          
          ;; Si c'est le default, on set les globales pour rétro-compatibilité
          (when (eq name :default)
            (setf lumen.core.context:*db-pool* new-pool
                  lumen.core.context:*current-db-config* config)))
        
        ;; Mode sans pool (juste config)
        (progn
          (setf (gethash name *pools-registry*) (cons nil config))
          (when (eq name :default)
             (setf lumen.core.context:*current-db-config* config)))))
  
  t)

(defun stop! (&key (name :default))
  (let ((entry (gethash name *pools-registry*)))
    (when entry
      (let ((pool (car entry)))
        (when pool
          (bt:with-lock-held ((pool-lock pool))
            (dolist (c (pool-available-conns pool))
              (ignore-errors (postmodern:disconnect c))))))
      (remhash name *pools-registry*)
      (format t "~&[DB] Pool stopped for app ~S.~%" name)))
  
  ;; Nettoyage global si c'est default
  (when (eq name :default)
    (setf lumen.core.context:*db-pool* nil
          lumen.core.context:*current-db-config* nil))
  t)
;;; --- CONNECTION MANAGEMENT (Inchangé) ---
(defvar *in-connection* nil)

(defun %checkout-connection (pool)
  ;; On trace l'acquisition de connexion
  (lumen.core.trace:with-tracing ("DB:AcquireConn" :pool-size (length (pool-available-conns pool)))
  (let ((conn nil))
    (bt:with-lock-held ((pool-lock pool))
      (setf conn (pop (pool-available-conns pool))))
    (if conn
        (if (postmodern:connected-p conn)
            conn
            (progn (ignore-errors (postmodern:disconnect conn))
                   (%create-connection (pool-config pool))))
        (%create-connection (pool-config pool))))))

(defun %checkin-connection (pool conn)
  (when (and pool conn (postmodern:connected-p conn))
    (bt:with-lock-held ((pool-lock pool))
      (push conn (pool-available-conns pool)))))

(defun call-with-conn (thunk &key (cfg (or lumen.core.context:*current-db-config* (lumen.data.config:db-config))))
  (if *in-connection*
      (funcall thunk)
      (if lumen.core.context:*db-pool*
          (let ((pool lumen.core.context:*db-pool*))
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
  "Execute INSERT/UPDATE/DELETE."

  (lumen.core.trace:with-tracing ("DB:Exec" 
                                  :sql (subseq sql 0 (min 100 (length sql)))
                                  :params-count (length params))
    (format t "~&[EXEC] SQL: ~A~%" sql)
  
    (let* ((kpos (position-if (lambda (x)
				(and (keywordp x)
                                     (not (member x '(:null :default) :test #'eq))))
                              params))
           (args (if kpos (subseq params 0 kpos) params))
           (opts (if kpos (subseq params kpos) '()))
           (timeout-ms (getf opts :timeout-ms (or *default-statement-timeout-ms* nil)))
           (t0 (get-internal-real-time)))

      ;;(format t "~&EXEC:ARGS: ~A~%" args)
      (format t "~&[EXEC] OPTS: ~A~%" opts)
      ;;(format t "~&[EXEC] ARGS: ~A~%" args)

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
          (values affected ret))))))

(defun query-a (sql &rest params)
  "Execute SELECT. Return alist."
  (lumen.core.trace:with-tracing ("DB:Query" 
                                  :sql (subseq sql 0 (min 100 (length sql))))
    (ensure-connection
      (let* ((t0 (get-internal-real-time))
             (fn (get-prepared-plan sql :format :alists)))
      
	;; Patch identique pour query-a, par sécurité
	(let ((real-params
		(if (and (= 1 (length params))
			 (listp (first params))
			 ;; Correction ici : on compte la longueur de la liste des résultats
			 (> (length (cl-ppcre:all-matches-as-strings "\\$\\d+" sql)) 1))
		    (first params)
		    params)))
        
          (handler-case
              (let ((rows (apply fn real-params)))
		(values rows (length rows)))
            (error (c)
              (error (translate-db-error c)))))))))

(defun query-1a (sql &rest params)
  (first (apply #'query-a sql params)))

;;; ----------------------------------------------------------------------------
;;; TRANSACTION MANAGEMENT (BLOCK/LABELS - SAFE FLOW)
;;; ----------------------------------------------------------------------------
#|
(defun run-in-transaction (thunk &key (retries 0) (sleep-ms 50))
  "Transaction avec flux de contrôle explicite (pas de loop/return implicite)."
  (lumen.core.trace:with-tracing ("DB:Transaction" :retries-max retries)
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
        
          (retry-loop))))))
|#

(defun run-in-transaction (thunk &key (retries 0) (sleep-ms 50))
  "Transaction avec flux de contrôle explicite et support des valeurs multiples."
  (lumen.core.trace:with-tracing ("DB:Transaction" :retries-max retries)
    (let ((attempt 0))
      (block txn-block
        (labels ((retry-loop ()
                   ;; 1. Exécution
                   (let ((result			   
                          (ensure-connection
                            (handler-case
                                (progn
                                  (%raw-exec "BEGIN")
                                  ;; --- CHANGEMENT 1 : Capture de TOUTES les valeurs ---
                                  ;; On transforme (values a b) en (list a b) pour le stocker
                                  (let ((vals (multiple-value-list (funcall thunk))))
                                    (%raw-exec "COMMIT")
                                    ;; On retourne la liste dans notre wrapper interne
                                    (list :ok vals)))
                              
                              (error (c)
                                (ignore-errors (%raw-exec "ROLLBACK"))
                                (list :error c))))))
                   
                   ;; 2. Analyse
                   (if (eq (first result) :ok)
                       ;; --- CHANGEMENT 2 : Restitution des valeurs ---
                       ;; On transforme (list a b) en (values a b) pour sortir du bloc
                       (return-from txn-block (values-list (second result)))
                       
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
          
          (retry-loop))))))

(defmacro with-tx ((&key retries) &body body)
  `(run-in-transaction (lambda () ,@body) :retries ,(or retries 0)))

;;; Pour forcer une config spécifique à l'intérieur d'un controlleur (par exemple pour qu'une App A aille lire exceptionnellement dans la DB de l'App B)
(defmacro with-db-app ((app-name) &body body)
  "Permet d'exécuter un bloc de code avec le contexte DB d'une autre application."
  `(let* ((ctx (get-db-context ,app-name))
          (lumen.core.context:*db-pool* (car ctx))
          (lumen.core.context:*current-db-config* (cdr ctx)))
     ,@body))

;;; Middleware qui lie dynamiquement *db-pool* et *current-config* au contexte de l'application spécifiée par APP-NAME.
(lumen.core.pipeline:defmiddleware db-session-middleware
    ((app-name :initarg :app-name :initform :default :reader mw-app-name))
    (req next)
  
  (let* ((name (slot-value mw 'app-name))
         ;; On récupère le couple (pool . config) depuis le registre
         (context (get-db-context name)))
    
    ;; Sécurité : on vérifie que l'app a bien été démarrée via (start! :name ...)
    (unless context
      (error "Lumen DB: Aucune configuration trouvée pour l'application ~S. Avez-vous appelé (lumen.data.db:start! :name ~S ...) ?" name name))
    
    ;; --- LE CŒUR DU SYSTÈME : DYNAMIC BINDING ---
    ;; On écrase temporairement les variables globales pour la durée de (funcall next req)
    ;; Toutes les fonctions appelées plus bas (query-a, exec, repo-*, etc.) verront CES valeurs.
    (let ((lumen.core.context:*db-pool* (car context))
          (lumen.core.context:*current-db-config* (cdr context)))
      
      ;; On passe la main à la requête avec le bon contexte DB chargé
      (funcall next req))))

(lumen.core.pipeline:defmiddleware connection-per-request-middleware ()
    (req next)
  "Emprunte une connexion DB unique pour toute la durée du traitement de la requête HTTP."
  ;; On s'assure qu'on est bien dans un contexte d'application DB (db-session-middleware a dû passer avant)
  (if lumen.core.context:*db-pool*
      ;; On utilise call-with-conn qui va checker la connexion, binder *in-connection*, 
      ;; exécuter NEXT (tout le reste de l'app), puis rendre la connexion proprement grâce à unwind-protect.
      (lumen.data.db::call-with-conn (lambda () (funcall next req)))
      
      ;; Si pas de DB (routes statiques par ex), on passe la main
      (funcall next req)))
