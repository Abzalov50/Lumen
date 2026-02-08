(in-package :cl)

(defpackage :lumen.http.session
  (:use :cl :alexandria)
  (:import-from :lumen.core.pipeline :middleware :handle :defmiddleware)
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :ctx-get :ctx-set!)
  (:import-from :lumen.utils :str-prefix-p :ensure-header :alist-set :hmac-sha256)
  (:import-from :ironclad :mac-digest :make-hmac)
  (:import-from :lumen.core.scheduler :defjob)
  (:export :session-id :session-data :session-get :session-set! :session-del!
   :verify-signed-sid :make-session-id :store-get :store-put! :store-del!
	   :sign-sid :rand-bytes :*session-ttl* :*session-cookie* :*secure-cookie*
   :*session-store* :session-gc :session-middleware :csrf-middleware))

(in-package :lumen.http.session)

(defparameter *session-cookie* "lumen.sid")
(defparameter *session-ttl*    3600) ; secondes
(defparameter *secure-cookie*  nil)  ; mettre T en prod (HTTPS)
(defparameter *session-store*  (make-hash-table :test #'equal)) ; id -> (alist :data … :exp …)

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
  "Sauvegarde la session en base (Upsert)."
  (let ((exp (+ (get-universal-time) (or ttl *session-ttl*)))
        ;; On sérialise l'alist Lisp en JSON String pour le stockage
        (json-data (cl-json:encode-json-to-string data)))
    
    (lumen.data.db:ensure-connection
      (pomo:query 
       "INSERT INTO sessions (id, data, expires_at) 
        VALUES ($1, $2::jsonb, $3)
        ON CONFLICT (id) 
        DO UPDATE SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at"
       sid 
       json-data 
       exp))))

(defun store-get (sid)
  "Récupère la session si elle n'est pas expirée."
  (lumen.data.db:ensure-connection
    (let* ((now (get-universal-time))
           ;; On cherche l'ID et on vérifie que la date d'expiration est future
           (raw-data (pomo:query 
                      "SELECT data FROM sessions WHERE id = $1 AND expires_at > $2"
                      sid 
                      now
		      :single)))
      
      (when raw-data
	(if (stringp raw-data)
            (cl-json:decode-json-from-string raw-data)
            raw-data)))))

(defun store-del! (sid)
  "Supprime la session."
  (lumen.data.db:ensure-connection
    (pomo:query "DELETE FROM sessions WHERE id = $1" sid)))

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

(defun session-get (req key)
  "Récupère une valeur de session de manière insensible à la casse et au type (String/Symbol)."
  (let* ((target-key (string key)) ;; On convertit ce qu'on cherche en string
         (data (session-data req)))
    
    (cdr (assoc target-key data 
                :test (lambda (target candidate)
                        ;; STRING-EQUAL est insensible à la casse (UID == uid)
                        ;; STRING convertit les Symboles en Strings automatiquement
                        (string-equal target (string candidate)))))))

(defun session-set! (req key value)
  "Définit une valeur en session en forçant la clé en String minuscule."
  (let* ((k (string-downcase (string key))) ;; On normalise la nouvelle clé
         (old (session-data req))
         (clean (remove k old 
                        ;; CORRECTION ICI : on prend le (car x) pour avoir la clé
                        :key (lambda (x) (string-downcase (string (car x)))) 
                        :test #'equal))
         ;; On ajoute la nouvelle paire (String . Valeur)
         (new (acons k value clean)))
    
    (lumen.core.http:ctx-set! req :session new)
    new))

(defun session-del! (req key &key (test #'eq))
  (let ((alist (remove key (session-data req) :key #'car :test test)))
    (lumen.core.http:ctx-set! req :session alist) alist))

;; --- GARBAGE COLLECTOR ---
(defun gc-sessions ()
  "Nettoie les sessions expirées (à appeler via un cron ou un timer)."
  (lumen.data.db:ensure-connection
    (let ((now (get-universal-time)))
      (pomo:query "DELETE FROM sessions WHERE expires_at < $1" now))))

;;; Scheduling du GC
(lumen.core.scheduler:defjob session-gc (payload)
  (declare (ignore payload)) ;; Pas d'argument nécessaire
  
  ;; On appelle la fonction de nettoyage SQL existante
  ;; Note: gc-sessions gère déjà ensure-connection
  (let ((count (gc-sessions)))
    (when (and count (plusp count))
      (format t "~&[Session GC] Cleaned ~A expired sessions.~%" count))))

;;; ---------------------------------------------------------------------------
;;; SESSION MIDDLEWARE (Cookie Signed)
;;; ---------------------------------------------------------------------------
(defmiddleware session-middleware
    ((secret :initarg :secret :initform nil) ;; Requis
     (ttl :initarg :ttl :initform (* 24 3600))
     (cookie-domain :initarg :cookie-domain :initform nil)
     (cookie-name :initarg :cookie-name :initform "lumen_sid")
     (http-only :initarg :http-only :initform t)
     (secure :initarg :secure :initform nil)
     (path :initarg :path :initform "/"))
    (req next)
  
  (with-slots (secret ttl cookie-domain cookie-name http-only secure path) mw
    (assert (and secret (plusp (length secret))) () "session-middleware: :secret is required.")
    
    ;; 1. Lecture
    (let* ((raw (or (cdr (assoc cookie-name (lumen.core.http:req-cookies req) :test #'string=)) ""))
           (sid (and (> (length raw) 0) (verify-signed-sid raw secret)))
           (data (and sid (store-get sid))))
      
      (unless sid
        (setf sid (make-session-id))
        (setf data '()))
      
      ;; Injection Context
      (lumen.core.http:ctx-set! req :session-id sid)
      (lumen.core.http:ctx-set! req :session data)
      
      ;; 2. Exécution
      (let ((resp (funcall next req)))
        
        ;; 3. Persistance
        (let ((sid* (session-id req))
              (dat* (session-data req)))
	  (format t "~&[SESSION SAVE] SID: ~A | DATA: ~A~%" sid* dat*)
          (store-put! sid* dat* ttl)

	  ;; --- AJOUT ICI : Headers Anti-Cache ---
        ;; On force le navigateur à ne pas utiliser le cache si un utilisateur est connecté
        ;; ou pour éviter le problème du 304 au login.
        
        (let ((headers (lumen.core.http:resp-headers resp)))
           ;; On ajoute Vary: Cookie
           (setf headers (lumen.utils:ensure-header headers "Vary" "Cookie"))
           ;; On ajoute Cache-Control
           (setf headers (lumen.utils:ensure-header headers "Cache-Control" "no-store, no-cache, must-revalidate, max-age=0"))
           ;; On met à jour la réponse
           (setf (lumen.core.http:resp-headers resp) headers))
          
          ;; Cookie Refresh
          (lumen.core.http:add-set-cookie 
           resp 
           (lumen.core.http:format-set-cookie
	    cookie-name
	    (sign-sid sid* secret)
	    :domain cookie-domain             
            
            :path path :http-only http-only :secure secure :max-age ttl)))
        resp))))

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
