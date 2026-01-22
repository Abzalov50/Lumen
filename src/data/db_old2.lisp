(in-package :cl)

(defpackage :lumen.data.db
  (:use :cl)
  (:import-from :postmodern 
   :connect-toplevel :disconnect-toplevel :with-connection :execute)
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
(defvar *connection-mode* :toplevel)

(defun start! (&key (config (lumen.data.config:db-config)))
  ;; Sécurité : Si déjà démarré, on arrête proprement avant de relancer
  ;; pour éviter d'orpheliner un pool existant.
  (when *started*
    (stop!))

  (setf *current-config* config
        *connection-mode* (or (getf config :db-connection-mode) :pooled-native))

  ;; Normalisation du mode en keyword
  (when (stringp *connection-mode*)
    (setf *connection-mode* (intern (string-upcase *connection-mode*) :keyword)))

  (ecase *connection-mode*
    ;; Mode Toplevel (REPL / Debugging interactif)
    (:toplevel
     #+postmodern
     (let ((db (getf config :database)) 
           (user (getf config :user))
           (pass (getf config :password)) 
           (host (or (getf config :host) "localhost"))
           (port (or (getf config :port) 5432)) 
           (use-ssl (case (getf config :sslmode) (:require :yes) (t :no))))
       (postmodern:connect-toplevel db user pass host :port port :use-ssl use-ssl))
     (setf *started* t))

    ;; Mode Pool (Production / Web Server)
    ((:scoped :pooled-native)
     ;; REMPLACEMENT : On initialise le Vrai Pool (LIFO Stack)
     (init-pool! config)
     (format t "~&[DB] Pool initialized with size ~A.~%" (getf config :pool-size 10))
     (setf *started* t)))

  *started*)

(defun stop! ()
  (when *started*
    (case *connection-mode*
      ;; Nettoyage Mode Toplevel
      (:toplevel
       #+postmodern (ignore-errors (postmodern:disconnect-toplevel)))
      
      ;; Nettoyage Mode Pool
      ((:scoped :pooled-native)
       ;; REMPLACEMENT : On ferme toutes les connexions physiques du pool
       (destroy-pool!)
       (format t "~&[DB] Pool destroyed.~%")))
    
    (setf *started* nil))
  t)

;; ----------------------------------------------------------------------------
;; CONNEXIONS & RÉENTRANCE (FIX CRITIQUE)
;; ----------------------------------------------------------------------------
;; --- Structures du Pool ---
(defstruct pool
  (available-conns '())       ; Liste des connexions libres (LIFO)
  (lock (bt:make-lock))       ; Pour protéger la liste
  (semaphore nil)             ; Pour attendre une place (Resource limiting)
  (config nil))               ; Config pour recréer si besoin

(defvar *db-pool* nil)

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

;; --- Gestion des connexions ---
(defun %checkout-connection (pool)
  "Récupère une connexion existante ou en crée une nouvelle."
  (let ((conn nil))
    (bt:with-lock-held ((pool-lock pool))
      (setf conn (pop (pool-available-conns pool))))
    
    (if conn
        ;; Vérifier si la connexion est toujours vivante (optionnel mais recommandé)
        (if (postmodern:connected-p conn)
            conn
            ;; Morte ? On réessaie récursivement ou on recrée
            (%create-connection (pool-config pool)))
        ;; Pas de connexion libre ? On en crée une
        (%create-connection (pool-config pool)))))

(defun %checkin-connection (pool conn)
  "Rend la connexion au pool."
  (when (and pool conn (postmodern:connected-p conn))
    (bt:with-lock-held ((pool-lock pool))
      (push conn (pool-available-conns pool)))))

(defun %create-connection (cfg)
  "Crée une connexion physique Postmodern."
  (postmodern:connect 
   (getf cfg :database)
   (getf cfg :user)
   (getf cfg :password)
   (getf cfg :host)
   :port (or (getf cfg :port) 5432)
   :use-ssl (case (getf cfg :sslmode) (:require :yes) (t :no))
   :pooled-p t)) ;; Marqueur pour Postmodern

;; --- Refonte de call-with-conn ---
(defvar *in-connection* nil)

(defun call-with-conn (thunk &key (cfg (or *current-config* (lumen.data.config:db-config))))
  ;; 1. Cas Réentrance : On a déjà une connexion active dans ce thread
  (if *in-connection*
      (funcall thunk)
      
      ;; 2. Cas Nouvelle demande : On tape dans le pool
      (let* ((pool *db-pool*)
             ;; Si pas de pool (ex: script one-shot), on crée temporaire
             (temp-conn (unless pool (%create-connection cfg))))
        
        (if temp-conn
            ;; Mode sans pool
            (let ((postmodern:*database* temp-conn)
                  (*in-connection* t))
              (unwind-protect (funcall thunk)
                (postmodern:disconnect temp-conn)))
            
            ;; Mode Pool
            (progn
              ;; A. On attend une place (Rate Limiting)
              (bt:wait-on-semaphore (pool-semaphore pool))
              (let ((conn nil))
                (unwind-protect
                     (progn
                       ;; B. On récupère une connexion physique
                       (setf conn (%checkout-connection pool))
                       (let ((postmodern:*database* conn)
                             (*in-connection* t))
                         (funcall thunk)))
                  ;; C. Cleanup
                  (progn
                    (if conn
                        (%checkin-connection pool conn)
                        ;; Si erreur à la création, on ne rend rien mais on signale
                        nil)
                    (bt:signal-semaphore (pool-semaphore pool))))))))))

(defmacro ensure-connection (&body body)
  `(call-with-conn (lambda () ,@body)))

(defmacro with-conn (&optional opts &body body)
  ;; Logique "intelligente" pour parser les options
  (if (and opts (listp opts) (or (null opts) (keywordp (first opts))))
      `(call-with-conn (lambda () ,@body) ,@opts)
      `(call-with-conn (lambda () ,opts ,@body))))

;; CORRECTION: On passe simplement tous les arguments à with-conn
;; Cela préserve les options si elles sont fournies.
;;(defmacro ensure-connection (&body body)
;;  `(with-conn ,@body))

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
    (format t "~&[DB:QUERY-1A] ARGS: ~A~%" args)
    (format t "~&[DB:QUERY-1A] PARAMS: ~A~%" params)
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
  (first (apply #'query-a sql params)))

;; ----------------------------------------------------------------------------
;; EXEC & HELPERS
;; ----------------------------------------------------------------------------
(defvar *default-statement-timeout-ms* nil)
(defvar *slow-query-ms* 500.0)

(defun %raw-exec (sql)
  #+postmodern (cl-postgres:exec-query postmodern:*database* sql)
  #-postmodern (error "Not implemented"))

(defmacro with-statement-timeout ((ms) &body body)
  `(let ((%ms ,ms))
     (if (and %ms (> %ms 0))
         (progn (ignore-errors (%raw-exec (format nil "set local statement_timeout = ~d" (truncate %ms))))
                ,@body)
         (progn ,@body))))

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

;; ----------------------------------------------------------------------------
;; WITH-TX (INLINE) : Gère call-next-method + Erreurs Métier
;; ----------------------------------------------------------------------------
(defmacro with-tx ((&key (isolation :read-committed) (read-only nil) 
                         (retries 0) (sleep-ms 50)) 
                   &body body)
  (declare (ignore isolation read-only))
  
  (let ((attempt (gensym "ATTEMPT"))
        (result  (gensym "RESULT"))
        (err     (gensym "ERR"))
        (msg     (gensym "MSG")))
    
    `(loop :for ,attempt :from 0 :to ,retries :do
       ;; 1. EXÉCUTION DANS LA CONNEXION
       ;; On stocke le résultat dans 'result'.
       ;; On ne fait JAMAIS de (return) ou (error) ici qui sortirait du bloc ensure-connection.
       (let ((,result
              (ensure-connection
                ;; On utilise handler-case pour capturer tout ce qui se passe
                ;; et le transformer en une valeur de retour simple.
                (handler-case
                    (progn
                      (%raw-exec "BEGIN")
                      (let ((val (progn ,@body)))
                        (%raw-exec "COMMIT")
                        ;; TÉMOIN DE SUCCÈS
                        (cons :success val)))
                  
                  ;; TÉMOIN D'ERREUR
                  (error (c)
                    (ignore-errors (%raw-exec "ROLLBACK"))
                    (cons :error c))))))
         
         ;; 2. ANALYSE (HORS DE LA CONNEXION)
         ;; Nous sommes sortis de la lambda ensure-connection. La pile est propre.
         ;; Nous pouvons maintenant utiliser return ou error sans crasher.
         
         (if (eq (car ,result) :success)
             ;; SUCCÈS : On retourne la valeur
             (return (cdr ,result))
             
             ;; ÉCHEC : On analyse l'erreur
             (let* ((,err (cdr ,result))
                    (,msg (format nil "~A" ,err)) ;; Sanitisation string
                    (is-app (typep ,err 'lumen.core.error:application-error))
                    (mapped (unless is-app (lumen.data.errors:map-db-error ,err))))
               
               (cond
                 ;; A) ERREUR MÉTIER -> ON LANCE L'ERREUR POUR LE MIDDLEWARE
                 (is-app
                  ;; C'est ici que le middleware va enfin pouvoir attraper l'erreur
                  (error 'lumen.core.error:application-error :message ,msg))
                 
                 ;; B) RETRY
                 ((and mapped (< ,attempt ,retries) (lumen.data.errors:retryable-db-error-p mapped))
                  (when (plusp ,sleep-ms) (sleep (/ (max 0 ,sleep-ms) 1000.0)))
                  ;; On laisse la boucle continuer
                  )
                 
                 ;; C) FATAL
                 (t
                  (error "Database Error: ~A" ,msg)))))))))

;; ----------------------------------------------------------------------------
;; RUN-IN-TRANSACTION (LA SOLUTION "NUCLÉAIRE")
;; ----------------------------------------------------------------------------
(defun read-slot-dynamically (instance slot-name-string)
  "Cherche un slot nommé SLOT-NAME-STRING dans le package de la classe de l'instance.
   Permet de lire 'CODE' même si c'est QALM:CODE et qu'on est dans LUMEN:DB."
  (ignore-errors
    (let* ((class (class-of instance))
           (pkg   (symbol-package (class-name class))))
      (when pkg
        (let ((sym (find-symbol slot-name-string pkg)))
          (when (and sym (slot-boundp instance sym))
            (slot-value instance sym)))))))

(defun read-slot-loose (instance slot-names)
  "Cherche la valeur d'un slot par nom dans le package de l'instance."
  (let* ((class (class-of instance))
         (pkg   (symbol-package (class-name class))))
    (when pkg
      (dolist (name slot-names)
        (let ((sym (find-symbol name pkg)))
          (when (and sym (ignore-errors (slot-boundp instance sym)))
            (return-from read-slot-loose (slot-value instance sym))))))))

(defun read-lumen-slot (instance slot-name)
  "Cherche spécifiquement un slot dans le package LUMEN.CORE.ERROR.
   Indispensable pour les classes qui héritent de application-error."
  (let ((sym (find-symbol slot-name :lumen.core.error)))
    (when (and sym (ignore-errors (slot-boundp instance sym)))
      (slot-value instance sym))))

;; --- EXTRACTEUR FINAL ---
(defun extract-error-details (c)
  ;; --- DEBUG: ON AFFICHE L'OBJET ---
  (format t "~&-----------------------------------------------------")
  (format t "~&[DB DEBUG] Inspection de l'erreur métier : ~A" (type-of c))
  (ignore-errors (describe c)) ;; Affiche tous les slots dans la console !
  (format t "~&-----------------------------------------------------~%")
  
  (let ((msg 
         (or
          (ignore-errors (lumen.core.error:application-error-message c))
          (read-lumen-slot c "MESSAGE") ;; Check explicite Lumen
          (ignore-errors (format nil "~A" c))
          "Erreur inconnue"))
        
        (code
         (or 
          ;; 1. Accesseur officiel
          (ignore-errors (lumen.core.error:application-error-code c))
          
          ;; 2. Slot hérité (LUMEN::CODE) - C'est celui-ci qui va marcher !
          (read-lumen-slot c "CODE")
          
          ;; 3. Slot local (QALM::CODE) - Pour les anciennes classes
          (read-slot-loose c '("CODE" "STATUS" "HTTP-CODE"))
          
          ;; 4. Défaut
          400)))

    (format t "~&[DB] Extracted Code: ~A from ~A~%" code (type-of c))
    
    (unless (integerp code) (setf code 400))
    (values msg code)))

(defun run-in-transaction (thunk &key (retries 0) (sleep-ms 50))
  ;; On récupère la config une seule fois
  (let* ((cfg (lumen.data.config:db-config))
         (db-name (getf cfg :database))
         (user (getf cfg :user))
         (pass (getf cfg :password))
         (host (getf cfg :host))
         (attempt 0))
    
    (loop
      (let ((outcome
             (block safe-zone
               ;; ---------------------------------------------------------
               ;; APPROCHE NUCLÉAIRE : CONNEXION DÉDIÉE HORS POOL
               ;; ---------------------------------------------------------
               (let ((conn (postmodern:connect db-name user pass host :pooled-p nil)))
                 (unwind-protect
                      (let ((postmodern:*database* conn)) ;; On lie la connexion active
                        (handler-case
                            (progn
                              ;;(format t "~&[TX NUCLEAR] Connexion dédiée ouverte.~%")
                              
                              ;; On utilise la macro standard, sur notre connexion vierge
                              (postmodern:with-transaction ()
                                ;;(format t "~&[TX NUCLEAR] Transaction Level: ~A~%" postmodern::*transaction-level*)
                                (let ((res (funcall thunk)))
                                  ;; Le COMMIT se fait automatiquement ici
                                  (cons :ok res))))
                          
                          (error (c)
                            (format t "~&[TX NUCLEAR ERROR] ~A~%" c)
                            (cons :error c))))
                   
                   ;; Nettoyage impératif : on ferme la connexion TCP
                   (postmodern:disconnect conn)
                   ;;(format t "~&[TX NUCLEAR] Connexion fermée.~%")
		   )))))
        
        ;; ... (Logique de retry inchangée) ...
        (if (eq (car outcome) :ok)
            (return (cdr outcome))
            (let* ((err (cdr outcome))
                   (is-app (typep err 'lumen.core.error:application-error))
                   (mapped (unless is-app (lumen.data.errors:map-db-error err))))
              (multiple-value-bind (msg code) (extract-error-details err)
                (cond
                  ((or is-app (not mapped)) (return (list :business-error msg code)))
                  ((and mapped (< attempt retries) (lumen.data.errors:retryable-db-error-p mapped))
                   (when (plusp sleep-ms) (sleep (/ (max 0 sleep-ms) 1000.0)))
                   (incf attempt))
                  (t (return (list :fatal-error msg)))))))))))
