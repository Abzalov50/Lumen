(in-package :cl)

(defpackage :lumen.data.db
  (:use :cl)
  
  (:import-from :postmodern
   :connect-toplevel :disconnect-toplevel :with-transaction)
  (:import-from :lumen.data.config
   :db-config)
  (:import-from :lumen.data.prepare
   :get-prepared-plan :reset-prepare-cache :*prepare-cache-ttl-ms*)
  (:import-from :lumen.data.errors :translate-db-error :map-db-error :retryable-db-error-p)
  (:import-from :lumen.data.metrics :record-query-latency :record-slow-query)
  
  (:export :start! :stop! :with-tx
           :query-a :query-1a :exec
           :with-conn :ensure-connection :with-rollback
           :*connection-mode* :with-statement-timeout
	   :*default-statement-timeout-ms* :*slow-query-ms*))

(in-package :lumen.data.db)

;;;; --------------------------------------------------------------------------
;;;; Pool counters & lock (portables, pas de semaphore-count)
;;;; --------------------------------------------------------------------------
(defvar *pool-lock* (bt:make-lock "lumen.db.pool"))
(defvar *pool-inflight* 0)  ; nb de connexions actuellement “tenues” par with-conn
(defvar *pool-waiters*  0)  ; nb de threads en attente d’un slot

(defvar *pool-sema* nil)
(defvar *pool-size* 0)
(defvar *pool-wait-ms* 2000)

(defun %pool-incf! (var delta)
  (bt:with-lock-held (*pool-lock*)
    (incf (symbol-value var) delta)))

(defun %pool-snapshot ()
  (bt:with-lock-held (*pool-lock*)
    (values *pool-inflight* *pool-waiters*)))

(defun %ensure-pool-sema! (cfg)
  (let* ((sz   (or (getf cfg :pool-size)
                   (lumen.core.config:cfg-get-int :db/pool-size :default 10)))
         (wait (or (getf cfg :pool-wait-ms)
                   (lumen.core.config:cfg-get-int :db/pool-wait-ms :default 2000))))
    (setf *pool-wait-ms* (max 1 wait))
    (when (or (null *pool-sema*) (<= sz 0) (/= sz *pool-size*))
      (setf *pool-sema* (bt:make-semaphore :count (max 1 sz))
            *pool-size* (max 1 sz)))))

;; ----------------------------------------------------------------------------
;; State
;; ----------------------------------------------------------------------------
(defvar *started* nil)
(defvar *current-config* nil)
;; :toplevel | :scoped | :pooled-native
(defvar *connection-mode* :toplevel)

(defvar *default-statement-timeout-ms* nil
  "Timeout par défaut (ms) appliqué aux requêtes si fourni. NIL = pas de timeout.")

(defvar *slow-query-ms* 500.0
  "Seuil (ms) au-delà duquel on loggue la requête comme lente.")

;; ----------------------------------------------------------------------------
;; Start / Stop
;; ----------------------------------------------------------------------------
(defun start! (&key (config (lumen.data.config:db-config)))
  "Initialise selon :connection-mode.
:toplevel → connect-toplevel
:scoped   → rien (ouverture par bloc)
:pooled-native → rien ici (piscine Postmodern via :pooled-p dans with-conn).
Tu peux régler postmodern:*max-pool-size* en amont."
  (print "IN START!")
  (print config)
  (setf *current-config* config
        *connection-mode* (or (getf config :db-connection-mode)
			      (lumen.core.config:cfg-get "DB_CONNECTION_MODE"
							 :default :toplevel)))
  (when (stringp *connection-mode*)
    (setf *connection-mode* (intern (string-upcase *connection-mode*) :keyword)))
  (print *connection-mode*)

  (ecase *connection-mode*
    (:toplevel
     #+postmodern
     (let* ((db   (getf config :database))
            (user (getf config :user))
            (pass (getf config :password))
            (host (or (getf config :host) "localhost"))
            (port (or (getf config :port) 5432))
            (app  (or (getf config :application-name) ""))
            (use-ssl (%sslmode->use-ssl (getf config :sslmode))))
       (handler-case
           (progn
             (postmodern:disconnect-toplevel)
             (postmodern:connect-toplevel db user pass host
                                          :port port :use-ssl use-ssl
                                          :application-name app)
             (setf *started* t))
         (condition (c)
           (setf *started* nil)
           (error "DB start! failed: ~a" c))))
     #-postmodern
     (setf *started* t))
    ((:scoped :pooled-native)
     ;; Pas de connexion globale ; on laisse with-conn piloter. On initialise le pool si besoin.
     (%ensure-pool-sema! config)
     (setf *started* t)))
  *started*)

(defun stop! ()
  (when *started*
    (when (eq *connection-mode* :toplevel)
      #+postmodern (ignore-errors (postmodern:disconnect-toplevel)))
    (setf *started* nil))
  t)

;; ----------------------------------------------------------------------------
;; Connections & Transactions
;; ----------------------------------------------------------------------------
(defun call-with-conn (thunk
		       &key (cfg (or *current-config* (lumen.data.config:db-config))))
  "Exécute THUNK dans une connexion Postgres selon CONFIG (ou provider). Priorité à *current-config* (set par start!) sinon config par défaut. Supporte :connection-mode = :toplevel | :scoped | :pooled-native."
  (let* ((mode (intern
                (string-upcase
                 ;; On regarde aussi dans la cfg passée en priorité
                 (or (getf cfg :db-connection-mode)
                     (lumen.core.config:cfg-get "DB_CONNECTION_MODE" :default :pooled-native)))
                :keyword)))
    (ecase mode
      (:toplevel
       ;; Connexion globale déjà ouverte par start! ; on exécute directement.
       (funcall thunk))

      (:scoped
       #+postmodern
       (let* ((db   (getf cfg :database))
              (user (getf cfg :user))
              (pass (getf cfg :password))
              (host (or (getf cfg :host) "localhost"))
              (port (or (getf cfg :port) 5432))
              (app  (or (getf cfg :application-name) ""))
              (use-ssl (%sslmode->use-ssl (getf cfg :sslmode))))
         (postmodern:with-connection (list db user pass host
                                           :port port :use-ssl use-ssl
                                           :application-name app)
           (funcall thunk)))
       #-postmodern
       (funcall thunk))

      (:pooled-native
       ;; Contrainte de concurrence via sémaphore local
       (%ensure-pool-sema! cfg)
       (let* ((deadline (+ (get-internal-real-time)
                           (* internal-time-units-per-second
                              (/ (max 1 *pool-wait-ms*) 1000.0))))
              (t0 (get-internal-real-time)))
         ;; entrer en file d’attente
         (%pool-incf! '*pool-waiters* 1)
         (unwind-protect
              (progn
                (labels ((acquire-slot ()
                           (or (bt:wait-on-semaphore *pool-sema*
                                                     :timeout (/ *pool-wait-ms* 1000.0))
                               (if (< (get-internal-real-time) deadline)
                                   (acquire-slot)
                                   (error "DB pool: timeout after ~A ms" *pool-wait-ms*)))))
                  (acquire-slot))
                ;; slot acquis → MAJ compteurs + métriques d’attente
                (%pool-incf! '*pool-waiters*  -1)
                (%pool-incf! '*pool-inflight* 1)
                (let* ((t1 (get-internal-real-time))
                       (wait-ms (* 1000.0 (/ (- t1 t0) internal-time-units-per-second))))
                  (multiple-value-bind (in-use _waiters) (%pool-snapshot)
                    (declare (ignore _waiters))
                    (ignore-errors
                      (lumen.data.metrics:record-pool-stats in-use wait-ms))))
                ;; ouvrir une connexion poolée Postmodern pour le corps
                #+postmodern		
                (let* ((db   (getf cfg :database))
                       (user (getf cfg :user))
                       (pass (getf cfg :password))
                       (host (or (getf cfg :host) "localhost"))
                       (port (or (getf cfg :port) 5432))
                       (use-ssl (%sslmode->use-ssl (getf cfg :sslmode))))
                  (unwind-protect		       
                       (postmodern:with-connection (list db user pass host
                                                         :port port :use-ssl use-ssl
                                                         :pooled-p t)
			 (ignore-errors (postmodern:query "set client_encoding = 'UTF8'"))
                         (funcall thunk))
                    ;; libération du slot
                    (%pool-incf! '*pool-inflight* -1)
                    (bt:signal-semaphore *pool-sema*)))
                #-postmodern
                (unwind-protect
                     (funcall thunk)
                  (%pool-incf! '*pool-inflight* -1)
                  (bt:signal-semaphore *pool-sema*)))
           ;; si erreur avant slot → on enlève l’attente
           (when (> *pool-waiters* 0)
             (%pool-incf! '*pool-waiters* -1))))))))

(defmacro with-conn (&optional opts &body body)
  "Usage:
  (with-conn               ...body...)          ; config via provider (ENV)
  (with-conn (:config cfg) ...body...)          ; surcharge ponctuelle"
  (if (and opts (listp opts) (or (null opts) (keywordp (first opts))))
      `(call-with-conn (lambda () ,@body) ,@opts)
      ;; Pas d'opts : `opts` est en fait la 1ère forme du body (compat)
      `(call-with-conn (lambda () ,opts ,@body))))

(defmacro ensure-connection (&optional opts &body body)
  `(with-conn ,opts ,@body))

;; Ajoutez cette fonction AVANT la macro with-tx
(defun call-with-tx-logic (thunk retries sleep-ms)
  "Logique d'exécution transactionnelle avec retries.
   Isole le contexte pour éviter les erreurs de pile (CONTROL-ERROR)."
  (let ((attempt 0))
    (loop
      (multiple-value-bind (outcome payload)
          (handler-case
              ;; Essai d'exécution
              (values :success
                      (multiple-value-list
                       (ensure-connection
                         #+postmodern
                         (postmodern:with-transaction ()
                           (funcall thunk))
                         #-postmodern
                         (funcall thunk))))
            
            ;; 1. Erreur Métier : On capture UNIQUEMENT le message (String)
            ;; Cela détruit tout lien avec l'objet condition problématique.
            (lumen.core.error:application-error (c)
              (values :app-error (princ-to-string c)))
            
            ;; 2. Erreur DB : On capture l'objet mappé
            (condition (c)
              (let ((mapped (lumen.data.errors:map-db-error c)))
                (if (and (< attempt retries)
                         (lumen.data.errors:retryable-db-error-p mapped))
                    (values :retry nil)
                    (values :db-error mapped)))))
        
        ;; ANALYSE DU RÉSULTAT (Hors du contexte protégé)
        (ecase outcome
          (:success
           (return (values-list payload))) ;; Retour simple du LOOP (safe)
          
          (:retry
           (incf attempt)
           (when (plusp sleep-ms)
             (sleep (/ (max 0 sleep-ms) 1000.0)))) ;; On boucle
          
          (:app-error
           ;; On recrée une erreur vierge ici
           ;; Note: On utilise find-symbol pour ne pas dépendre de QALM à la compilation
           (let* ((sym (or (find-symbol "BUSINESS-ERROR" "QALM.BACKEND.CORE.ERROR")
                           'lumen.core.error:application-error))
                  (msg payload)
                  ;; Nettoyage du préfixe si présent
                  (clean-msg (if (search "Business Error: " msg)
                                 (subseq msg 16)
                                 msg)))
             (error (make-instance sym :message clean-msg))))
          
          (:db-error
           (error payload)))))))

;; Redéfinition de la macro pour utiliser le helper
(defmacro with-tx ((&key (isolation :read-committed) (read-only nil)
                         (retries 0) (sleep-ms 50))
                   &body body)
  (declare (ignore isolation read-only))
  ;; On enveloppe le body dans une lambda et on passe au helper
  `(call-with-tx-logic (lambda () ,@body) ,retries ,sleep-ms))

(defmacro with-statement-timeout ((ms) &body body)
  "Applique SET LOCAL statement_timeout = MS à l’intérieur d’une transaction existante.
Ne crée pas de transaction : à utiliser sous with-conn + BEGIN, ou quand un bloc
ouvrira de toute façon une transaction (ex: via ton code applicatif)."
  `(let ((%ms ,ms))
     (if (and %ms (> %ms 0))
         (progn
           ;; SET LOCAL n’a d’effet que dans une transaction *déjà* ouverte.
           ;; Dans les tests, on est sous with-rollback (BEGIN/ROLLBACK).
           (ignore-errors
             (postmodern:query (format nil "set local statement_timeout = ~d" (truncate %ms))))
           ,@body)
         (progn ,@body))))
;; ----------------------------------------------------------------------------
;; Internal helpers
;; ----------------------------------------------------------------------------
;; Postmodern attend :use-ssl = :no | :try | :require | :yes | :full
(defun %sslmode->use-ssl (sslmode)
  (case sslmode
    ((:disable nil) :no)
    ((:allow :prefer) :try)
    (:require :require)
    (:verify-ca :yes)
    (:verify-full :full)
    (t :no)))

(defun %rows->alists (rows)
  "Best-effort conversion to alists. If ROWS already look like alists, return as-is."
  (if (and rows (consp (first rows)) (keywordp (caar rows)))
      rows
      ;; Fallback – leave as-is (caller may adapt). Improve with column metadata if needed.
      rows))

(defun %now-ms ()
  (round (* 1000.0 (/ (get-internal-real-time) internal-time-units-per-second))))

(defun %elapsed-ms (t0)
  (* 1000.0 (/ (- (get-internal-real-time) t0) internal-time-units-per-second)))

;; ----------------------------------------------------------------------------
;; Query wrappers
;; ----------------------------------------------------------------------------
(defun query-a (sql &rest params)
  "Execute a SELECT-like statement, return (values rows-alist count).
Options en fin de params (keywords) :
  :timeout-ms <int>  — timeout Postgres via SET LOCAL statement_timeout (ms)."
  (let* ((kpos (position-if #'keywordp params))
         (args (if kpos (subseq params 0 kpos) params))
         (opts (if kpos (subseq params kpos) '()))
         (timeout-ms (getf opts :timeout-ms (or *default-statement-timeout-ms* nil)))
         (t0 (get-internal-real-time)))
    (handler-case
        (with-statement-timeout (timeout-ms)
          (let* ((fn   (lumen.data.prepare:get-prepared-plan sql :format :alists))
                 (rows (apply fn args))
                 (elapsed-ms (* 1000.0 (/ (- (get-internal-real-time) t0)
                                          cl:internal-time-units-per-second))))
            (lumen.data.metrics:record-query-latency sql elapsed-ms (length rows))
            (when (and *slow-query-ms* (>= elapsed-ms *slow-query-ms*))
              (lumen.data.metrics:record-slow-query
	       sql elapsed-ms :params args :rows (length rows)))
            (values rows (length rows))))
      (condition (c)
        (translate-db-error c)))))

(defun query-1a (sql &rest params)
  "Like query-a but expect 0/1 row; return alist or NIL.
Options :
  :timeout-ms <int>"
  (let* ((kpos (position-if #'keywordp params))
         (args (if kpos (subseq params 0 kpos) params))
         (opts (if kpos (subseq params kpos) '()))
         (timeout-ms (getf opts :timeout-ms (or *default-statement-timeout-ms* nil)))
         (t0 (get-internal-real-time)))
    (handler-case
        (with-statement-timeout (timeout-ms)
          (let* ((fn  (get-prepared-plan sql :format :alist))
                 (row (apply fn args))
                 (elapsed-ms (* 1000.0 (/ (- (get-internal-real-time) t0)
                                          cl:internal-time-units-per-second))))
            (record-query-latency sql elapsed-ms (if row 1 0))
            (when (and *slow-query-ms* (>= elapsed-ms *slow-query-ms*))
              (lumen.data.metrics:record-slow-query
	       sql elapsed-ms :params args :rows (if row 1 0)))
            row))
      (condition (c)
        (translate-db-error c)))))

(defun exec (sql &rest params)
  "Execute INSERT/UPDATE/DELETE. Returns (values affected returning?).
   Gère correctement les arguments :NULL dans les paramètres SQL."
  
  (format t "~&EXEC:SQL: ~A~%" sql)
  
  (let* (;; On cherche le début des options (:timeout-ms, etc.)
         ;; CORRECTION : On ignore :NULL et :DEFAULT qui sont des valeurs SQL, pas des options.
         (kpos (position-if (lambda (x)
                              (and (keywordp x)
                                   (not (member x '(:null :default) :test #'eq))))
                            params))
         
         ;; Séparation propre Arguments SQL / Options Lumen
         (args (if kpos (subseq params 0 kpos) params))
         (opts (if kpos (subseq params kpos) '()))
         
         ;; Extraction des options
         (timeout-ms (getf opts :timeout-ms (or *default-statement-timeout-ms* nil)))
         (t0 (get-internal-real-time)))

    (format t "~&EXEC:ARGS: ~A~%" args)
    (format t "~&EXEC:OPTS: ~A~%" opts)
    
    (handler-case
        (with-statement-timeout (timeout-ms)
          (let* ((lower (string-downcase sql))
                 (has-returning (search "returning" lower))
                 affected ret)
            
            (if has-returning
                ;; RETURNING -> On veut le résultat (alist)
                (let* ((fn  (get-prepared-plan sql :format :alist))
                       (row (apply fn args)))
                  (setf affected (if row 1 0)
                        ret row))
                
                ;; Pas de RETURNING -> On veut juste le nombre de lignes affectées
                (let* ((fn (get-prepared-plan sql :format :none))
                       (n  (or (apply fn args) 0)))
                  (setf affected n
                        ret nil)))
            
            ;; Métriques (Optionnel)
            (let ((elapsed-ms (* 1000.0 (/ (- (get-internal-real-time) t0)
                                           cl:internal-time-units-per-second))))
              (record-query-latency sql elapsed-ms (or affected 0))
              (when (and *slow-query-ms* (>= elapsed-ms *slow-query-ms*))
                (lumen.data.metrics:record-slow-query
                 sql elapsed-ms :params args :affected affected)))
            
            (values affected ret)))

      ;; Si c'est une erreur applicative, on la re-signale telle quelle
      ;; sans l'emballer dans une db-error.
      (lumen.core.error:application-error (c)
        (error c))
      
      (condition (c)
        (translate-db-error c)))))

(defmacro with-rollback ((&optional opts) &body body)
  "Exécute BODY dans une transaction qui sera annulée.
   Accepte des options comme (:config my-config)."
  `(lumen.data.db:with-conn ,opts
     (lumen.data.db:exec "BEGIN")
     (unwind-protect
          (progn ,@body)
       (ignore-errors (lumen.data.db:exec "ROLLBACK")))))
