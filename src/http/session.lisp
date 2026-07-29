(in-package :cl)

(defpackage :lumen.http.session
  (:use :cl :alexandria)
  (:import-from :lumen.core.pipeline :middleware :handle :defmiddleware)  
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :ctx-get :ctx-set!)
  (:import-from :lumen.utils :str-prefix-p :ensure-header :alist-set :hmac-sha256
		:db-network-error-p :reset-current-db-connection :run-db-with-reconnect)
  (:import-from :ironclad :mac-digest :make-hmac)
  (:import-from :lumen.core.scheduler :defjob)
  (:export :session-id :session-data :session-get :session-set! :session-del!
   :verify-signed-sid :make-session-id :store-get :store-put! :store-del!
	   :sign-sid :rand-bytes :*session-ttl* :*session-cookie* :*secure-cookie*
   :*session-store* :session-gc :session-middleware :csrf-middleware
   :auth-middleware :auth-required :roles-allowed
	   :clear-session-read-cache
	   :*session-read-cache-ttl-seconds*
:session-read-cache-stats))

(in-package :lumen.http.session)

(defparameter *session-cookie* "lumen.sid")
(defparameter *session-ttl*    300) ; secondes
(defparameter *secure-cookie*  nil)  ; mettre T en prod (HTTPS)
(defparameter *session-store*  (make-hash-table :test #'equal)) ; id -> (alist :data … :exp …)

(defstruct session-cache-entry
  data
  expires-at)

(defparameter *session-read-cache*
  (make-hash-table :test #'equal))

(defparameter *session-read-cache-lock*
  (bt:make-lock "lumen-session-read-cache"))

(defparameter *session-read-cache-ttl-seconds*
  300)

(defun %session-cache-get (sid)
  "Retourne les données de session et FOUND-P.

L'expiration en cache ne dépasse jamais l'expiration autoritative en base."
  (let ((now (%session-cache-now)))

    (bt:with-lock-held (*session-read-cache-lock*)
      (let ((entry
              (gethash sid *session-read-cache*)))

        (cond
          ((null entry)
           (incf *session-cache-misses*)
           (values nil nil))

          ((<=
            (session-cache-entry-expires-at entry)
            now)

           (remhash sid *session-read-cache*)
           (incf *session-cache-misses*)

           (values nil nil))

          (t
           (incf *session-cache-hits*)

           (values
            (copy-tree
             (session-cache-entry-data entry))
            t)))))))

(defun %session-cache-put (sid data &optional session-expires-at)
  (bt:with-lock-held (*session-read-cache-lock*)
    (setf
     (gethash sid *session-read-cache*)
     (make-session-cache-entry
      :data (copy-tree data)
      :expires-at
      (min (+ (%session-cache-now)
              *session-read-cache-ttl-seconds*)
           (or session-expires-at
               most-positive-fixnum)))))

  data)

(defun %session-cache-delete (sid)
  (bt:with-lock-held (*session-read-cache-lock*)
    (remhash sid *session-read-cache*))

  t)

(defun clear-session-read-cache ()
  (bt:with-lock-held (*session-read-cache-lock*)
    (clrhash *session-read-cache*)
    (setf *session-cache-hits* 0
          *session-cache-misses* 0))

  t)

(defun session-read-cache-stats ()
  (bt:with-lock-held (*session-read-cache-lock*)
    (list
     :entries (hash-table-count *session-read-cache*)
     :hits *session-cache-hits*
     :misses *session-cache-misses*
     :ttl-seconds *session-read-cache-ttl-seconds*)))

(defvar *session-cache-hits* 0)
(defvar *session-cache-misses* 0)

(defun %session-cache-now ()
  (get-universal-time))

(defun hex (u8)
  (with-output-to-string (s)
    (dotimes (i (length u8))
      (format s "~2,'0X" (aref u8 i)))))

(defun rand-bytes (n)
  (let ((v (make-array n :element-type '(unsigned-byte 8))))
    (dotimes (i n) (setf (aref v i) (random 256))) v))

(defun make-session-id ()
  (hex (rand-bytes 16))) ; 128 bits

;; ---------- store mémoire ----------
(defun %now () (get-universal-time))

;; --- STORE SQL (Postmodern) ---
(defun store-put! (sid data ttl)
  "Sauvegarde la session en base et actualise le cache mémoire."
  (let ((exp
          (+ (get-universal-time)
             (or ttl *session-ttl*)))

        (json-data
          (cl-json:encode-json-to-string data)))

    (lumen.data.db:ensure-connection
      (pomo:query
       "INSERT INTO sessions (id, data, expires_at)
        VALUES ($1, $2::jsonb, $3)
        ON CONFLICT (id)
        DO UPDATE SET
          data = EXCLUDED.data,
          expires_at = EXCLUDED.expires_at"
       sid
       json-data
       exp))

    (%session-cache-put sid data exp)

    data))

(defun store-get (sid)
  "Récupère une session depuis le cache ou PostgreSQL.

Retourne deux valeurs :
- les données ;
- T lorsque la session existe."
  (multiple-value-bind (cached-data cached-p)
      (%session-cache-get sid)

    (when cached-p
      (format t "~&[SESSION CACHE] HIT SID: ~A~%" sid)
      (return-from store-get
        (values cached-data t)))

    (format t "~&[SESSION CACHE] MISS SID: ~A~%" sid)

    (lumen.data.db:ensure-connection
      (let* ((now
               (get-universal-time))

             (row
               (first
                (pomo:query
                 "SELECT data, expires_at
                   FROM sessions
                  WHERE id = $1
                    AND expires_at > $2"
                 sid
                 now
                 :alists)))

             (raw-data
               (and row
                    (cdr (assoc :data row))))

             (expires-at
               (and row
                    (cdr (assoc :expires-at row)))))

        (if raw-data
            (let ((data
                    (if (stringp raw-data)
                        (cl-json:decode-json-from-string raw-data)
                        raw-data)))

              (%session-cache-put sid data expires-at)

              (values
               (copy-tree data)
               t))

            (values nil nil))))))

(defun store-del! (sid)
  "Supprime la session de PostgreSQL et du cache."
  (%session-cache-delete sid)

  (lumen.data.db:ensure-connection
    (pomo:query
     "DELETE FROM sessions WHERE id = $1"
     sid))

  t)

;; ---------- cookie signé ----------
(defun sign-sid (sid secret)
  (let* ((bytes (trivial-utf-8:string-to-utf-8-bytes sid))
         (sig   (hmac-sha256 (trivial-utf-8:string-to-utf-8-bytes secret) bytes)))
    (format nil "~A.~A" sid (hex sig))))

(defun verify-signed-sid (cookie-value secret)
  "Retourne SID si cookie-value est bien signé avec SECRET, sinon NIL."
  (let ((pos (position #\. cookie-value)))
    (when pos
      (let* ((sid (subseq cookie-value 0 pos))
             (expected (sign-sid sid secret)))
        (when (string= expected cookie-value)
          sid)))))

;; ---------- API session ----------
(defun session-id (req) (lumen.core.http:ctx-get req :session-id))
(defun session-data (req) (lumen.core.http:ctx-get req :session))

(defun %normalized-session-key (value)
  (typecase value
    (null nil)
    (string (string-downcase value))
    (symbol (string-downcase (symbol-name value)))
    (t nil)))

(defun session-get (req key)
  "Récupère une valeur de session sans échouer sur une alist historique mal formée."
  (let ((target-key (%normalized-session-key key)))
    (when target-key
      (dolist (entry (session-data req))
        (when (consp entry)
          (let ((candidate (%normalized-session-key (car entry))))
            (when (and candidate (string= target-key candidate))
              (return (cdr entry)))))))))

(defun session-set! (req key value)
  "Définit une valeur en session en forçant la clé en String minuscule."
  (let* ((k (or (%normalized-session-key key)
                (error "La clé de session doit être une chaîne ou un symbole.")))
         (old (session-data req))
         (clean
           (loop for entry in old
                 when (and (consp entry)
                           (let ((candidate
                                   (%normalized-session-key (car entry))))
                             (and candidate
                                  (not (string= k candidate)))))
                   collect entry))
         (new (acons k value clean)))
    (lumen.core.http:ctx-set! req :session new)
    new))

(defun session-del! (req key &key (test #'eq))
  "Supprime KEY de façon idempotente, insensible à la casse et au type."
  (declare (ignore test))
  (let* ((target-key (%normalized-session-key key))
         (alist
           (loop for entry in (session-data req)
                 when (and (consp entry)
                           (let ((candidate
                                   (%normalized-session-key (car entry))))
                             (and candidate
                                  (or (null target-key)
                                      (not (string= target-key candidate))))))
                   collect entry)))
    (lumen.core.http:ctx-set! req :session alist) alist))

;; --- GARBAGE COLLECTOR ---
(defun gc-sessions (app-name)
  "Nettoie les sessions expirées pour l'application spécifiée.

Version robuste pour BD distante : si la socket PostgreSQL est morte,
on ferme la connexion courante, on rouvre, puis on retente.
"
  (format t "~&[Session GC] Worker réveillé pour l'app: ~S~%" app-name)

  (let* ((actual-app-name
           (if (lumen.data.db::get-db-context app-name)
               app-name
               (progn
                 (format t "~&[Session GC] App ~S non trouvée dans le registre DB. Fallback sur :DEFAULT.~%"
                         app-name)
                 :default))))

    (lumen.data.db:with-db-app (actual-app-name)

      (format t "~&[Session GC] Config DB injectée dans le thread: ~S~%"
              (list :user (getf lumen.core.context:*current-db-config* :user)
                    :has-password
                    (not (null (getf lumen.core.context:*current-db-config*
                                      :password)))))

      (handler-case
          (run-db-with-reconnect
           (lambda ()
             (lumen.data.db:run-in-transaction
              (lambda ()
                (let* ((now (get-universal-time))
                       (affected
                         (lumen.data.db:exec
                          "DELETE FROM sessions WHERE expires_at < $1"
                          now)))
                  affected))
              :retries 1
              :sleep-ms 100))
           :retries 3
           :sleep-ms 500)

        (error (e)
          ;; Pour le GC de sessions, on ne veut pas casser l'app.
          ;; Si la BD distante est indisponible quelques secondes,
          ;; le prochain passage cron nettoiera.
          (if (db-network-error-p e)
              (progn
                (format t "~&[Session GC] DB indisponible après retries. Nettoyage reporté : ~A~%"
                        e)
                (reset-current-db-connection)
                0)
              (error e)))))))

;;; Scheduling du GC
(lumen.core.scheduler:defjob session-gc (payload)
  (let ((app-name (if payload payload :default)))
    (handler-case
        (let ((count (gc-sessions app-name)))
          (when (and count (plusp count))
            (format t "~&[Session GC:~A] Cleaned ~A expired sessions.~%"
                    app-name count)))

      (error (e)
        (if (db-network-error-p e)
            (progn
              (format t "~&[Session GC:~A] Erreur réseau DB ignorée pour ce passage : ~A~%"
                      app-name e)
              (reset-current-db-connection)
              nil)
            (error e))))))

;;; ---------------------------------------------------------------------------
;;; SESSION MIDDLEWARE (Cookie Signed)
;;; ---------------------------------------------------------------------------
(defmiddleware session-middleware
    ((secret :initarg :secret :initform nil)
     (ttl :initarg :ttl :initform (* 24 3600))
     (cookie-domain :initarg :cookie-domain :initform nil)
     (cookie-name :initarg :cookie-name :initform "lumen_sid")
     (http-only :initarg :http-only :initform t)
     (secure :initarg :secure :initform nil)
     (path :initarg :path :initform "/"))
    (req next)

  (with-slots
      (secret ttl cookie-domain cookie-name http-only secure path)
      mw

    (assert
     (and secret
          (plusp (length secret)))
     ()
     "session-middleware: :secret is required.")

    (let* ((raw
             (or
              (cdr
               (assoc
                cookie-name
                (lumen.core.http:req-cookies req)
                :test #'string=))
              ""))

           (verified-sid
             (and
              (plusp (length raw))
              (verify-signed-sid raw secret))))

      (multiple-value-bind (loaded-data session-found-p)
          (if verified-sid
              (store-get verified-sid)
              (values nil nil))

        ;; Un cookie signé dont la session n'existe plus en base
        ;; ne doit pas être réutilisé.
        (let* ((sid
                 (if session-found-p
                     verified-sid
                     (make-session-id)))

               (data
                 (if session-found-p
                     loaded-data
                     '()))

               ;; Copie indépendante permettant de détecter les
               ;; mutations destructives de l'alist.
               (initial-data
                 (copy-tree data)))

          ;; Injection dans le contexte de la requête.
          (lumen.core.http:ctx-set! req :session-id sid)
          (lumen.core.http:ctx-set! req :session data)

          ;; Exécution de la suite de la pile.
          (let ((resp (funcall next req)))
 
            (let* ((sid* (session-id req))
                   (dat* (session-data req))
                   (sid-changed-p (not (equal sid sid*)))
                   (data-changed-p (not (equal initial-data dat*)))
                   (save-required-p
		     (or
		      (not session-found-p)
		      sid-changed-p
		      data-changed-p)))

              ;; On écrit uniquement lorsque la route a réellement
              ;; créé ou modifié la session.
              (when save-required-p
                (store-put! sid* dat* ttl)

                ;; Ne jamais journaliser les données complètes
                ;; de session.
                (format t
                        "~&[SESSION SAVE] SID: ~A | NEW: ~A | DATA-CHANGED: ~A~%"
                        sid*
                        (not session-found-p)
                        data-changed-p)

                ;; Le cookie n'est renouvelé que lorsque la session
                ;; est effectivement sauvegardée.
                (lumen.core.http:add-set-cookie
                 resp
                 (lumen.core.http:format-set-cookie
                  cookie-name
                  (sign-sid sid* secret)
                  :domain cookie-domain
                  :path path
                  :http-only http-only
                  :secure secure
                  :max-age ttl)))

              ;; Les pages dépendant de la session ne doivent pas être
              ;; partagées entre plusieurs cookies par un cache HTTP.
              (let ((headers (lumen.core.http:resp-headers resp)))

                (setf headers
                      (lumen.utils:ensure-header
                       headers
                       "Vary"
                       "Cookie"))

                (setf headers
                      (lumen.utils:ensure-header
                       headers
                       "Cache-Control"
                       "no-store, no-cache, must-revalidate, max-age=0"))

                (setf
                 (lumen.core.http:resp-headers resp)
                 headers))

              resp)))))))

;;; ---------------------------------------------------------------------------
;;; 16. CSRF MIDDLEWARE
;;; ---------------------------------------------------------------------------

(defun %random-token ()
  (cl-base64:usb8-array-to-base64-string (rand-bytes 32) :uri t))

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
           (tok (or (session-get req :csrf)
                    (let ((tkn (%random-token)))
                      (session-set! req :csrf tkn) 
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

(defmiddleware auth-middleware
    ((secret :initarg :secret :initform nil)
     (required-p :initarg :required-p :initform nil)
     (roles-allow :initarg :roles-allow :initform nil)
     (scopes-allow :initarg :scopes-allow :initform nil)
     (scopes-mode :initarg :scopes-mode :initform :any)
     (leeway :initarg :leeway :initform 60)
     (admin-roles :initarg :admin-roles :initform '("admin"))
     (bypass-admin :initarg :bypass-admin :initform t)
     (public-paths :initarg :public-paths
		   :initform '("/auth/" "/assets/" "/public/" "/favicon.ico")))
    (req next)

  (block auth-middleware
    (with-slots (secret required-p roles-allow scopes-allow scopes-mode leeway admin-roles bypass-admin public-paths) mw

      ;; ---------------------------------------------------------
      ;; 0. WHITELIST CHECK (Arrêt immédiat si route publique)
      ;; ---------------------------------------------------------
      (let ((path (lumen.core.http:req-path req)))
	(when (some (lambda (prefix) 
                      ;; On vérifie si l'URL commence par un des préfixes publics
                      (and (>= (length path) (length prefix))
                           (string= prefix (subseq path 0 (length prefix)))))
                    public-paths)
          ;; C'est une route publique, on laisse passer sans auth
          (return-from auth-middleware (funcall next req))))
    
      ;; ---------------------------------------------------------
      ;; 1. STRATÉGIE D'HYDRATATION DU CONTEXTE (Session vs JWT)
      ;; ---------------------------------------------------------
      (let* ((session-uid (session-get req "user-id"))
            (jwt-token   (or (%bearer-token (lumen.core.http:req-headers req))
                             (%query-token req '("access_token" "token"))))
	     (scopes-raw (session-get req "scopes"))
	     (scopes (if (and scopes-raw (stringp scopes-raw)
			      (str:starts-with? "[" scopes-raw))
			     (json:decode-json-from-string scopes-raw)
			     scopes-raw)))

	(cond
          ;; CAS A : Authentifié via SESSION
          (session-uid
           (lumen.core.http:ctx-set! req :user-id session-uid)
	   (lumen.core.http:ctx-set! req :user-role (session-get req "role"))
	   (lumen.core.http:ctx-set! req :user-scopes scopes)
	   (lumen.core.http:ctx-set! req :tenant-id (or (ctx-get req :tenant-id)
							(session-get req :tid))))

          ;; CAS B : Authentifié via JWT (API externe, Mobile)
          ((and jwt-token secret)
           (multiple-value-bind (payload ok) 
               (ignore-errors (lumen.core.jwt:jwt-decode jwt-token :secret secret :verify t :leeway leeway))
             (when ok
               (lumen.core.http:ctx-set! req :user-id (cdr (assoc :sub payload)))
               (lumen.core.http:ctx-set! req :user-role (cdr (assoc :role payload)))
               (lumen.core.http:ctx-set! req :user-scopes (cdr (assoc :scopes payload)))
               ;; Tenant
               (let ((tid (or (cdr (assoc :tenant-id payload)) (cdr (assoc :tenant payload)))))
		 (when tid (lumen.core.http:ctx-set! req :tenant-id tid))))))))

      ;; ---------------------------------------------------------
      ;; 2. LOGIQUE D'AUTORISATION (Inchangée)
      ;; ---------------------------------------------------------
      (let* ((uid (lumen.core.http:ctx-get req :user-id))
	     (role (lumen.core.http:ctx-get req :user-role))
	     (scopes (lumen.core.http:normalize-scopes (lumen.core.http:ctx-get req :user-scopes)))
	     (is-admin (and role (member role admin-roles :test #'string=)))
           
	     (roles-ok (or (null roles-allow)
			   (member role roles-allow :test #'string=)
			   (and is-admin bypass-admin)))
           
	     (scopes-ok (or (null scopes-allow)
			    (if (eq scopes-mode :all)
				(every (lambda (s) (member s scopes :test #'string=)) scopes-allow)
				(some (lambda (s) (member s scopes :test #'string=)) scopes-allow))
			    (and is-admin bypass-admin))))
	(lumen.utils:log-msg "AUTH MW" :uid uid :role role :scopes scopes
				       :scopes-ok scopes-ok :roles-ok roles-ok)
	(cond
	  ;; Non connecté alors que requis
	  ((and required-p (null uid))
	   ;; Pour une app Web, on redirige souvent vers /login au lieu de renvoyer du JSON 401
	   ;; Mais gardons le comportement standard pour l'instant
	   ;; Est-ce une requête HTMX ?
           (let ((is-htmx (assoc "hx-request" (lumen.core.http:req-headers req) :test #'string-equal)))
           
             (cond
               ;; A. Requête HTMX -> On force le rechargement client vers Login
               (is-htmx
		(lumen.core.http:respond-htmx-redirect "/auth/login"))
             
               ;; B. Requête Navigateur Standard (HTML) -> Redirection 302 classique
               ((member "text/html" (lumen.core.http:get-accepted-types req) :test #'search)
		(lumen.core.http:redirect-to "/auth/login"))
             
               ;; C. API JSON (App mobile, etc.) -> Erreur 401
               (t
		(lumen.core.http:respond-json '((:error . "Unauthorized")) :status 401)))))
        
	  ;; Connecté mais Rôle insuffisant
	  ((not roles-ok)
	   (lumen.core.http:respond-json '((:error . "Forbidden (Role)")) :status 403))
        
	  ;; Connecté mais Scope insuffisant
	  ((not scopes-ok)
	   (lumen.core.http:respond-json '((:error . "Forbidden (Scope)")) :status 403))
        
	  ;; Autorisé
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
