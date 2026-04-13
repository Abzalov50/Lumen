(in-package :cl)

(defpackage :lumen.core.http
  (:use :cl :alexandria)
  (:import-from :lumen.utils :%trim :url-decode-qs :alist-get :alist-set)
  (:import-from :lumen.core.body :parse-json)
  (:export
   :*request*
   :request :response :url-encode :make-content-disposition
   :req-method :req-path :req-headers :req-query :req-cookies :req-params
   :req-body-stream :req-ctx :ctx-from-req :resp-status :resp-headers :resp-body
   :respond-text :respond-json :respond-404 :respond-401 :respond-500 :respond-422
   :respond-413 :respond-400 :respond-403 :respond-405 :normalize-json
   :respond-html :respond-redirect :respond-htmx-redirect :respond-ok :redirect-to
   ;; Contexte (Files, fields)
   :ctx-get :ctx-set!
   ;; Cookies
   :parse-cookie-header :format-set-cookie :add-set-cookie
   ;; HTTP Dates
   :set-last-modified! :cache-control
   ;; JWT, login et roles
   :current-jwt :current-role :current-user-id :current-scopes :current-tenant-id
   ;; SSE et Websockets
   :respond-sse
   :halt :http-halt :halt-response
   
   :res-set-header! :add-header :request-payload-unified :get-accepted-types :normalize-scopes))
 
(in-package :lumen.core.http)

(defvar *request* nil
  "La requête HTTP en cours de traitement.
   Valeur liée dynamiquement par `current-request-middleware`.
   ATTENTION : Cette variable n'est valide que dans le thread de la requête.")

(defclass request ()
  ((method :initarg :method :accessor req-method)
   (path :initarg :path :accessor req-path)
   (headers :initarg :headers :accessor req-headers)
   (query :initarg :query :accessor req-query)
   (cookies :initarg :cookies :accessor req-cookies)
   (params :initarg :params :accessor req-params)
   (body-stream :initarg :body-stream :accessor req-body-stream)
   (context :initarg :context :accessor req-ctx)))

(defclass response ()
  ((status :initarg :status :accessor resp-status :initform 200)
   (headers :initarg :headers :accessor resp-headers :initform nil)
   (body :initarg :body :accessor resp-body :initform "")
   (cookies :initarg :cookies :initform nil :accessor resp-cookies)))

;;; ------------------------------------------------------------
;;; Conditions
;;; ------------------------------------------------------------

(define-condition validation-error (error)
  ((message :initarg :message :reader validation-message
            :initform "Unprocessable Entity")
   (errors  :initarg :errors  :reader validation-errors
            :initform nil))  ; alist/plist champ -> messages
  (:report (lambda (c s)
             (format s "Validation error: ~a~@[ (~s)~]"
                     (validation-message c) (validation-errors c)))))

(defun signal-validation-error (&key (message "Unprocessable Entity") errors)
  "Helper pratique pour lever une erreur de validation."
  (error 'validation-error :message message :errors errors))

(define-condition precondition-failed (error)
  ((message :initarg :message :reader precondition-message
            :initform "Precondition Failed")
   (header  :initarg :header  :reader precondition-header
            :initform "If-Match"))  ; ex: If-Match / If-Unmodified-Since
  (:report (lambda (c s)
             (format s "Precondition failed (~a): ~a"
                     (precondition-header c) (precondition-message c)))))

(defun signal-precondition-failed (&key (message "Precondition Failed")
                                        (header "If-Match"))
  "Helper pour lever une erreur 412 liée aux préconditions."
  (error 'precondition-failed :message message :header header))

;;; ------------------------------------------------------------
;;; Helpers
;;; ------------------------------------------------------------
(defun normalize-scopes (v)
  (handler-case
      (let ((lst (cond ((null v) nil)
                       ((listp v) v)
                       ((stringp v) (cl-ppcre:split "\\s+" v)) ;; Space separated scopes
                       (t (list (princ-to-string v))))))
        (remove-duplicates (mapcar #'string lst) :test #'string=))
    (error () nil)))

(defun %merge-headers (base extra)
  (append base extra))

(defun plist->alist (pl)
  (loop for (k v) on pl by #'cddr collect (cons k v)))

(defun normalize-json (data)
  ;;(print "normalize-json")
  ;;(print data)
  (etypecase data
    (hash-table data)
    (list
     (if (every #'consp data)          ; alist ?
         data
         (plist->alist data)))         ; sinon, plist
    (t (list (cons :data data)))))

;; Encode RFC3986 (UTF-8 -> %HH). Par défaut, espace => %20 (pas '+').
(defun url-encode (s &key (space-as-plus nil))
  (let* ((text (or s ""))
         (bytes (trivial-utf-8:string-to-utf-8-bytes text)))
    (with-output-to-string (out)
      (loop for b across bytes
            for ch = (code-char b)
            do (cond
                 ;; A-Z, a-z, 0-9, - . _ ~ (RFC 3986 Unreserved)
                 ((or (and (>= b 65) (<= b 90))
                      (and (>= b 97) (<= b 122))
                      (and (>= b 48) (<= b 57))
                      (member b '(45 46 95 126)))
                  (write-char ch out))
                 ;; Espace
                 ((and space-as-plus (= b 32))
                  (write-char #\+ out))
                 ;; Tout le reste -> %HH
                 (t 
                  ;; CORRECTION ICI : Syntaxe Common Lisp (~2,'0X)
                  ;; % -> littéral
                  ;; ~2 -> largeur 2
                  ;; ,'0 -> padding avec des zéros
                  ;; X -> Hexadécimal
                  (format out "%~2,'0X" b)))))))

;; Échappe un paramètre quoted-string HTTP (\", \\)
(defun %header-quote (s)
  (with-output-to-string (out)
    (loop for ch across (or s "") do
      (case ch
        (#\\ (write-string "\\\\" out))
        (#\" (write-string "\\\"" out))
        (t   (write-char ch out))))))

;; Remplace les non-ASCII par '?' pour le fallback filename="..."
(defun %ascii-fallback (s)
  (coerce
   (loop for ch across (or s "") collect
         (if (<= (char-code ch) 127) ch #\?))
   'string))

;; Pour commodité, construire une valeur Content-Disposition avec les 2 variantes
(defun make-content-disposition (filename &key (disposition "attachment"))
  "Retourne une valeur Content-Disposition robuste :
   attachment; filename=\"ascii\"; filename*=UTF-8''percent-encoded"
  (let* ((fn (or filename "file"))
         (fallback (%header-quote (%ascii-fallback fn)))
         (rfc5987 (url-encode fn)))
    (format nil "~A; filename=\"~A\"; filename*=UTF-8''~A"
            disposition fallback rfc5987)))

;;; ------------------------------------------------------------
;;; Réponses
;;; ------------------------------------------------------------
(defun respond-text (txt &key (status 200) (headers nil))
  (make-instance 'response :status status
			   :headers (append '(("Content-Type" . "text/plain; charset=utf-8")) headers)
			   :body txt))

(defun respond-html (html &key (status 200) (headers nil))
  "Crée une réponse HTML en fusionnant les headers fournis et ceux du contexte."
  (let* ((req lumen.core.http:*request*)
         (ctx (when (boundp 'req) (ctx-from-req req)))
         (pending (getf ctx :pending-headers))
         
         ;; NETTOYAGE : On s'assure que chaque élément est bien une CONS (paire)
         ;; Si un intrus comme "valider" est présent, il est ignoré.
         (safe-pending (remove-if-not #'consp (if (listp pending) pending nil)))
         
         ;; FUSION : On concatène tout
         (all-headers (append (if (listp headers) headers nil)
                              safe-pending
                              '(("Content-Type" . "text/html; charset=utf-8")))))
    
    ;; --- LE LOG DE DÉBOGAGE ---
    (lumen.utils:log-msg "HTTP-RESPONSE" 
                         :status status 
                         :headers all-headers
                         :corrupted-detected (not (eq (length pending) (length safe-pending))))

    (make-instance 'response
                   :status status
                   :headers all-headers
                   :body html)))

(defun respond-ok (target-url &key (msg "Opération réussie"))
  (let ((resp (lumen.core.http:respond-html "")))
             
    ;; 1. Redirection HTMX
    (setf (lumen.core.http:resp-headers resp)
          (lumen.utils:ensure-header 
           (lumen.core.http:resp-headers resp) 
           "HX-Redirect" 
           target-url))
             
    ;; 2. Toast (Optionnel)
    (setf (lumen.core.http:resp-headers resp)
          (lumen.utils:ensure-header 
           (lumen.core.http:resp-headers resp) 
           "HX-Trigger" 
           (cl-json:encode-json-to-string 
            `((:show-message . ((:type . "success") (:message . ,msg)))))))
             
    resp))

(defun respond-json (data &key (status 200) (headers nil))
  (let* ((normalized (normalize-json data))
         (json (cl-json:encode-json-to-string normalized)))
    ;;(print normalized)
    ;;(print json)
    (make-instance 'response
                   :status status
                   :headers (append '(("Content-Type" . "application/json; charset=utf-8")
                                      ("Cache-Control" . "no-store")
                                      ("X-Content-Type-Options" . "nosniff"))
                                    headers)
                   :body json)))

(defun respond-201 (data &key location (headers nil))
  "201 Created + payload JSON.
Optionnel: header Location si fourni."
  (let ((h (if location
               (%merge-headers `(("Location" . ,location)) headers)
               headers)))
    (respond-json data :status 201 :headers h)))

(defun respond-422 (&key (message "Unprocessable Entity") (errors nil) (headers nil))
  "422 Validation / Semantics error. ERRORS peut être un alist/plist mappant champ → messages."
  (respond-json
   `((:error . ((:type . "validation")
                (:message . ,message)
                ,@(when errors `((:errors . ,(normalize-json errors)))))))
   :status 422 :headers headers))

(defun respond-413 (&key (message "Unprocessable Entity") (errors nil) (headers nil))
  "413 Validation / Semantics error. ERRORS peut être un alist/plist mappant champ → messages."
  (respond-json
   `((:error . ((:type . "validation")
                (:message . ,message)
                ,@(when errors `((:errors . ,(normalize-json errors)))))))
   :status 413 :headers headers))

(defun respond-415 (&key (message "Unprocessable Entity") (errors nil) (headers nil))
  "415 Validation / Semantics error. ERRORS peut être un alist/plist mappant champ → messages."
  (respond-json
   `((:error . ((:type . "validation")
                (:message . ,message)
                ,@(when errors `((:errors . ,(normalize-json errors)))))))
   :status 415 :headers headers))

(defun respond-404 (&optional (message "Not Found"))
  (respond-json `((:error . ((:type . "not_found")
			     (:message . ,message))))
		:status 404))

(defun respond-400 (&optional (message "Bad Request"))
  (respond-json `((:error . ((:type . "bad_request")
			     (:message . ,message))))
		:status 400))

(defun respond-401 (&optional (message "Unauthorized"))
  (respond-json `((:error . ((:type . "unauthorized")
			     (:message . ,message))))
		:status 401))

(defun respond-403 (&optional (message "Forbidden"))
  (respond-json `((:error . ((:type . "forbidden")
			     (:message . ,message))))
		:status 403))

(defun respond-500 (&optional (message "Internal Error"))
  (respond-json `((:error . ((:type . "internal")
			     (:message . ,message))))
		:status 500))

(defun respond-redirect (location &key (status 302) (headers nil))
  "Réponse de redirection (Location + corps vide)."
  (make-instance 'response
                 :status status
                 :headers (append (list (cons "location" location))
                                  headers)
                 :body ""))

(defun respond-htmx-redirect (url)
  "Force une redirection côté client via HTMX (changement complet de page)."
  (make-instance 'response
                 :status 200
                 :headers `(("HX-Redirect" . ,url))
                 :body ""))

(defun redirect-to (location &key (status 302))
  "Alias pratique pour une redirection standard HTTP."
  (respond-redirect location :status status))

(defun respond-204 (&key (headers nil))
  "204 No Content, sans corps (body vide)."
  (make-instance 'response
                 :status 204
                 :headers headers
                 :body ""))

;; --- Cookies (helpers indépendants du middleware) ---------------------------
(defun parse-cookie-header (cookie-header)
  "Retourne une alist (\"name\" . \"value\"). Ignore les attributs (Path, Secure…)."
  (when (and cookie-header (plusp (length cookie-header)))
    (loop for part in (uiop:split-string cookie-header :separator ";")
          for trimmed = (%trim part)
          for pos = (position #\= trimmed)
          for k = (%trim (if pos (subseq trimmed 0 pos) trimmed))
          for v = (lumen.utils:url-decode-qs (and pos (%trim (subseq trimmed (1+ pos)))))
          when (plusp (length k))
          collect (cons k (or v "")))))

(defun format-set-cookie (name value &key domain path max-age expires secure http-only same-site)
  "Construit la valeur d'un header Set-Cookie *unique* (une chaîne)."
  (with-output-to-string (s)
    (format s "~A=~A" name value)
    (when domain (format s "; Domain=~A" domain))
    (when path   (format s "; Path=~A" path))
    (when (and (integerp max-age)) (format s "; Max-Age=~D" max-age))
    (when expires (format s "; Expires=~A" expires)) ; RFC1123 string si tu veux
    (when secure (format s "; Secure"))
    (when http-only (format s "; HttpOnly"))
    (when same-site (format s "; SameSite=~A" same-site))))

(defun add-set-cookie (resp cookie-string)
  "Ajoute *un* header Set-Cookie (dupliqué autorisé) à la réponse."
  (let* ((hdrs (resp-headers resp))
         (new  (append hdrs (list (cons "set-cookie" cookie-string)))))
    (setf (resp-headers resp) new)
    resp))

(defun ctx-get (req key &key (test #'eq) default)
  "Lire une clé KEY (souvent un keyword) depuis l’alist req-ctx."
  (lumen.utils:alist-get (req-ctx req) key :test test :default default))

(defun ctx-set! (req key value &key (test #'eq))
  "Muter req-ctx en y (re)mettant KEY=VALUE (retourne la nouvelle alist)."
  (setf (req-ctx req)
        (lumen.utils:alist-set (req-ctx req) key value :test test)))

;;; HTTP Dates
(defun cache-control (resp &key public private no-store no-cache
                                max-age s-maxage must-revalidate proxy-revalidate
                                stale-while-revalidate stale-if-error immutable)
  "Construit et pose Cache-Control sur RESP (retourne RESP)."
  (let ((tokens '()))
    (when public             (push "public" tokens))
    (when private            (push "private" tokens))
    (when no-store           (push "no-store" tokens))
    (when no-cache           (push "no-cache" tokens))
    (when immutable          (push "immutable" tokens))
    (when must-revalidate    (push "must-revalidate" tokens))
    (when proxy-revalidate   (push "proxy-revalidate" tokens))
    (when max-age            (push (format nil "max-age=~D" max-age) tokens))
    (when s-maxage           (push (format nil "s-maxage=~D" s-maxage) tokens))
    (when stale-while-revalidate
      (push (format nil "stale-while-revalidate=~D" stale-while-revalidate) tokens))
    (when stale-if-error
      (push (format nil "stale-if-error=~D" stale-if-error) tokens))
    (let* ((val (map 'string (lambda (c) c)
                     (with-output-to-string (s)
                       (format s "~{~A~^, ~}" (nreverse tokens)))))
           (hdrs (resp-headers resp)))
      (setf (resp-headers resp)
            (lumen.utils:ensure-header hdrs "cache-control" val)))
    resp))

(defun set-last-modified! (resp universal-time)
  "Ajoute ou remplace Last-Modified (IMF-fixdate) sur RESP. Retourne RESP."
  (let* ((val (lumen.utils:format-http-date universal-time))
         (hdrs (resp-headers resp)))
    (setf (resp-headers resp)
          (lumen.utils:ensure-header hdrs "last-modified" val))
    resp))

(defun current-jwt (req) (ctx-get req :jwt))

(defun current-user-id (req)
  "Retourne l'ID utilisateur, qu'il vienne de la Session ou d'un Token."
  (ctx-get req :user-id))

(defun current-tenant-id (req)
  "Retourne le Tenant ID courant."
  (ctx-get req :tenant-id))

(defun current-role (req)
  "Retourne le Rôle (admin, user, etc.)."
  (ctx-get req :user-role))

(defun current-scopes (req)
  "Retourne la liste des permissions/scopes."
  (ctx-get req :user-scopes))

;;; SSE
(defun %split-lines (s)
  (let ((acc '()) (start 0) (len (length s)))
    (labels ((push-line (end) (push (subseq s start end) acc)))
      (dotimes (i len)
        (when (char= #\Newline (char s i))
          (push-line i)
          (setf start (1+ i))))
      (push-line len))
    (nreverse acc)))

(defun %sse-writer (handler &key retry-ms heartbeat-sec)
  "Wrappe HANDLER pour écrire au format SSE. Le SEND fourni à HANDLER :
   (send :event \"...\" :data \"...\") retourne T si écrit, NIL si client fermé.
   NOTE: heartbeat-sec est laissé au handler (comment \"hb\")."
  (declare (ignore heartbeat-sec))
  (lambda (send-chunk)
    (labels
        ((emit (&key data event id comment)
           ;; Respect strict du format SSE
           (when comment
             (funcall send-chunk (format nil ": ~A~%~%" comment)))
           (when data
             (when event (funcall send-chunk (format nil "event: ~A~%" event)))
             (when id    (funcall send-chunk (format nil "id: ~A~%" id)))
             ;; data peut contenir des \n → une ligne "data: ..." par ligne
             (dolist (line (%split-lines data))
               (funcall send-chunk (format nil "data: ~A~%" line)))
             ;; séparateur d'event
             (funcall send-chunk (format nil "~%"))))
         (safe-send (&rest args)
           "T si écrit, NIL si déconnexion/erreur I/O."
           (handler-case
               (progn
                 ;; Autoriser (funcall send \"texte\") pour un envoi brut (dev)
                 (cond
                   ((and (= (length args) 1) (stringp (first args)))
                    (emit :data (first args)))
                   ;; Liste &key impaire → ne PAS écrire côté client (socket peut être KO).
                   ((oddp (length args))
                    (format *error-output*
                            "~&[SSE] odd number of &KEY args dropped: ~S~%" args))
                   (t
                    (apply #'emit args)))
                 t)
             ;; le client a coupé / erreur TLS → signaler échec (NIL)
             ((or usocket:connection-aborted-error
                  usocket:connection-reset-error
                  sb-bsd-sockets:socket-error
                  stream-error
                  #+cl+ssl cl+ssl::ssl-error
                  #+cl+ssl cl+ssl::ssl-error-syscall)
              (e)
              (declare (ignore e))
              ;; Pas d'écriture d'un message d'erreur ici : le flux est mort.
              nil)
             (error (e)
               ;; Erreur applicative (format, etc.) → on log et on considère l'écriture échouée.
               (format *error-output* "~&[SSE] handler error: ~A~%" e)
               nil))))
      ;; Annonce du délai de reconnexion (client EventSource)
      (when retry-ms
        (funcall send-chunk (format nil "retry: ~D~%~%" (truncate retry-ms))))
      ;; Exécuter le handler avec un SEND “safe”
      (funcall handler #'safe-send))))

(defun respond-sse (req handler &key retry-ms (heartbeat-sec 15) headers)
  (declare (ignore req))
  (make-instance 'response
                 :status 200
                 :headers (append '(("Content-Type"      . "text/event-stream; charset=utf-8")
                                    ("Cache-Control"     . "no-cache")
                                    ("X-Accel-Buffering" . "no")
                                    ("Pragma"            . "no-cache"))
                                  headers)
                 :body (%sse-writer handler :retry-ms retry-ms :heartbeat-sec heartbeat-sec)))

(defun ctx-from-req (req &key (audit? nil audit?-p))
  "Construit un ctx plist depuis req.ctx pour passer aux repo-ops.
Dépend des middlewares (tenant-from-host, auth, request-id) qui posent ces clés dans le req."
  (let* ((tenant-id (or (lumen.core.http:ctx-get req :tenant-id)
                        (lumen.core.http:ctx-get req :tenant)))
         (actor-id  (or (lumen.core.http:ctx-get req :actor-id)
                        (lumen.core.http:current-user-id req)))
         (role      (or (lumen.core.http:ctx-get req :user-role)
                        (lumen.core.http:current-role req)))
         (scopes    (lumen.core.http:ctx-get req :user-scopes))
         (req-id    (lumen.core.http:ctx-get req :request-id))
         (ip        (lumen.core.http:ctx-get req :ip))
         (ua        (lumen.core.http:ctx-get req :ua))
         (auditflag (if audit?-p audit? (or (lumen.core.http:ctx-get req :audit?) t)))
	 (json      (lumen.core.http:ctx-get req :json))
	 (json-str      (lumen.core.http:ctx-get req :json-str))
	 (form      (lumen.core.http:ctx-get req :form))
	 (fields      (lumen.core.http:ctx-get req :fields))
	 (files      (lumen.core.http:ctx-get req :files)))
    (list :req req
          :tenant-id tenant-id
          :actor-id  actor-id
          :role      role
          :scopes    scopes
          :request-id req-id
          :ip ip :ua ua
          :audit? auditflag
	  :json json :json-str json-str :form form
	  :fields fields :files files)))

;;; ------------------------------------------------------------
;;; Interruption de flux (Halt)
;;; ------------------------------------------------------------

;; Cette condition transporte la réponse finale
(define-condition http-halt (condition)
  ((response :initarg :response :reader halt-response))
  (:report (lambda (c s) (declare (ignore c)) (format s "HTTP Halt triggered"))))

(defun halt (status &optional (body "") (headers nil))
  "Interrompt immédiatement le traitement de la requête.
   STATUS peut être un entier (ex: 404) ou directement un objet RESPONSE."
  (let ((resp (cond
                ;; Si on passe déjà un objet response, on l'utilise tel quel
                ((typep status 'response) status)
                
                ;; Si c'est un code erreur standard, on utilise les helpers JSON existants
                ((= status 404) (respond-404 (if (string= body "") "Not Found" body)))
                ((= status 403) (respond-json `((:error . ((:type . "forbidden") (:message . ,body)))) :status 403))
                ((= status 500) (respond-500 (if (string= body "") "Internal Error" body)))
                
                ;; Sinon, réponse texte brute par défaut
                (t (respond-text (format nil "~A" body) :status status :headers headers)))))
    
    ;; On signale la condition. Le routeur (lumen.core.router) DOIT l'attraper.
    (error 'http-halt :response resp)))

(defun res-set-header! (resp name value)
  "Ajoute ou remplace le header NAME par VALUE dans l'objet réponse RESP.
   Retourne l'objet RESP modifié."
  (check-type resp response) ;; Sécurité : on s'assure qu'on touche bien une Réponse
  (setf (resp-headers resp)
        (lumen.utils:alist-set (resp-headers resp) 
                               name 
                               value 
                               :test #'string-equal)) ;; Insensible à la casse (HTTP standard)
  resp)

(defun add-header (name value)
  "Ajoute un header au contexte. Force le format (name . value)."
  (when (boundp 'lumen.core.http:*request*)
    (let* ((req lumen.core.http:*request*)
           ;; 1. On récupère proprement le contexte (alist)
           (ctx (req-ctx req))
           ;; 2. On récupère les headers en attente (alist)
           (pending (lumen.utils:lookup ctx :pending-headers))
           ;; 3. On nettoie 'pending' au cas où il serait corrompu
           (clean-pending (if (and (listp pending) (every #'consp pending))
                              pending
                              nil))
           
           (existing (lumen.utils:alist-get clean-pending name :test #'string-equal))
           (new-val (if (and existing (not (string= (format nil "~A" existing) "")))
                        (format nil "~A, ~A" existing value)
                        value))
           ;; 4. On utilise ALIST-SET pour garantir une paire (name . new-val)
           (updated-pending (lumen.utils:alist-set clean-pending name new-val)))
      
      ;; 5. On sauve l'ALIST PROPRE dans le contexte
      (ctx-set! req :pending-headers updated-pending))))

(defun finalize-response (req resp)
  "Transfère les headers accumulés dans le contexte vers l'objet réponse final."
  (let* ((ctx (ctx-from-req req))
	 (pending (getf ctx :pending-headers)))
    (dolist (h pending)
      (res-set-header! resp (car h) (cdr h)))
    resp))

(defun request-payload-unified (req)
  "Récupère les données (Alist) quelle que soit la source (JSON Body ou Form Data)."
  (let* ((h (req-headers req))
         (len-str (cdr (assoc "content-length" h :test #'string-equal)))
         (len (and len-str (parse-integer len-str :junk-allowed t))))
    (or 
     ;; 1. Essayer le Formulaire (application/x-www-form-urlencoded)
     ;; Parsé par form-parser-middleware
     (ctx-get req :form)
   
     ;; 2. Essayer le JSON
     ;; Parsé par un body-parser-middleware
     (ctx-get req :json)
   
     ;; 3. Fallback : Parser le JSON à la volée si pas fait
     (ignore-errors (lumen.core.body:parse-json (req-body-stream req) len)))))

(defun get-accepted-types (req)
  "Retourne une liste des types MIME acceptés par le client (basé sur le header Accept).
   Exemple: '(\"text/html\" \"application/xhtml+xml\" ...)"
  (let* ((headers (req-headers req))
         ;; Recherche insensible à la casse du header "Accept"
         (accept-str (cdr (assoc "accept" headers :test #'string-equal))))
    
    (if accept-str
        ;; On découpe la chaîne "text/html, application/json;q=0.9, */*"
        (loop for item in (uiop:split-string accept-str :separator ",")
              ;; On nettoie chaque item (on enlève les espaces et les paramètres ;q=...)
              collect (string-trim " " (car (uiop:split-string item :separator ";"))))
        ;; Si pas de header, on suppose tout
        '("*/*"))))
