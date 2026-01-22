(in-package :cl)

(defpackage :lumen.core.middleware
  (:use :cl :alexandria)
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :ctx-get :ctx-set!)
  (:import-from :lumen.utils :-> :->> :str-prefix-p :ensure-header :parse-http-date
		:format-http-date)
  (:import-from :lumen.core.mime :guess-content-type)
  (:import-from :lumen.core.body :parse-urlencoded)
  (:import-from :lumen.core.session :session-id :session-data :session-get :session-set! :session-del! :verify-signed-sid :sign-sid :store-get :store-put! :store-del! :rand-bytes :*session-ttl* :*session-cookie* :*secure-cookie*)
  (:import-from :lumen.core.jwt :*jwt-secret* :jwt-encode :jwt-decode)
  (:import-from :lumen.core.ratelimit :allow?)
  (:import-from :lumen.core.http-range :respond-file)
  (:export :define-middleware :my-compose :*app* :logger :json-only :query-parser
	   :parse-query-string-to-alist :*debug* :set-app! :*pipeline-signature*
	   :named :->mw :static :cors :cors-auto :static-many
	   :form-parser :multipart-parser
	   ;; Erreur Handlers
   :*error-handler* :error-wrapper
   ;; Cookies
   :request-id :request-context :set-cookie! :cookie :cookies-parser
   ;; ETag
   :etag
   ;; Compression
   :compression
   :last-modified-conditional
   ;; Session
	   :session
   :csrf
	   :auth-jwt
   :https-redirect
	   :auth-required :roles-allowed :rate-limit
	   :request-timeout :max-body-size :access-log-json))

(in-package :lumen.core.middleware)

(defparameter *debug* nil)  ;; active les prints si T

(defvar *app* (lambda (req)
                (declare (ignore req))
                (lumen.core.http:respond-404 "No handler"))
  "La pile de middlewares active. Toujours mise à jour via (set-app!).")

(defvar *pipeline-signature* nil
  "Liste symbolique des middlewares dans l'ordre actuel.")

(defstruct mw-entry
  name   ;; string (affichage)
  fn)    ;; le factory: (next) -> (lambda (req) ...)

(defun %entry-name (x)
  (etypecase x
    (mw-entry (mw-entry-name x))
    (function "<fn>")))  ;; fallback lisible

(defun %entry-fn (x)
  (etypecase x
    (mw-entry (mw-entry-fn x))
    (function x)))

(defun named (name fn-factory)
  "Wrappe un middleware factory avec un nom (string)."
  (make-mw-entry :name (string name) :fn fn-factory))

(defmacro ->mw (name form)
  `(lumen.core.middleware:named ,name ,form))

(defun set-app! (&rest middlewares)
  "Compose la pile. Chaque élément peut être:
   - un FUNCTION (factory classique),
   - ou (named \"Nom\" FUNCTION)."
  (let* ((fns   (mapcar #'%entry-fn   middlewares))
         (names (mapcar #'%entry-name middlewares)))
    (setf *app* (apply #'my-compose fns)
          *pipeline-signature* names)))

(defun header-ref (headers name)
  (cdr (assoc (string-downcase name) headers :test #'string=)))

(defun my-compose (&rest middlewares)
  "Construit (req -> resp) en enchaînant les middlewares dans l’ordre donné."
  (let ((terminal (lambda (req)
                    (declare (ignore req))
                    (respond-404 "No handler"))))
    (dolist (mw (reverse middlewares) terminal)
      (setf terminal (funcall mw terminal)))))  ; <-- APPELLE le mw AVEC `next`

(defmacro define-middleware (name (req-sym next-sym) &body body)
  `(defun ,name (,next-sym)
     (lambda (,req-sym)
       ,@body)))

#|
(define-middleware logger (req next)
  (let* ((start (get-internal-real-time))
         (resp (funcall next req))
         (end  (get-internal-real-time))
         (ms   (floor (* 1000.0 (/ (- end start)
                                   internal-time-units-per-second)))))
    (when *debug*
      (format t "~&[lumen] ~A ~A -> ~A (~A ms)~%"
              (slot-value req 'lumen.core.http::method)
              (slot-value req 'lumen.core.http::path)
              (lumen.core.http:resp-status resp)
              ms))
    resp))
|#

(defun %status-color (status)
  (cond
    ((<= 200 status 299) "32")  ; vert
    ((<= 300 status 399) "36")  ; cyan
    ((<= 400 status 499) "33")  ; jaune
    (t                          "31"))) ; rouge

(defun %emacs-repl-p ()
  "Vrai si on est dans un REPL Emacs (SLIME/SLY) ou INSIDE_EMACS."
  (or (member :swank *features*)
      (member :slynk *features*)
      (uiop:getenv "INSIDE_EMACS")))

(defun %supports-ansi-p (stream)
  (declare (ignore stream))
  (and (not (%emacs-repl-p))
       (not (member :win32 *features*))
       (let ((term (uiop:getenv "TERM")))
         (and term (not (string= term "")) (not (string-equal term "dumb"))))))

(defun logger (&key (stream *terminal-io*) (color :auto))
  "Logger concis: [ts] ip METHOD host path -> status (ms).
COLOR ∈ (:auto T NIL) — par défaut :auto (désactivé sur consoles sans ANSI)."
  (let ((color-enabled (if (eq color :auto) (%supports-ansi-p stream) (not (null color)))))
    (lambda (next)
      (lambda (req)
        (labels ((h (name)
                   (cdr (assoc name (lumen.core.http:req-headers req) :test #'string-equal))))
          (let* ((t0   (get-internal-real-time))
                 ;; IP: XFF → X-Real-IP → ctx[:remote-addr] → "-"
                 (ip   (or (let* ((xff (h "x-forwarded-for")))
                             (and xff (string-trim " " (car (uiop:split-string xff :separator ",")))))
                           (h "x-real-ip")
                           (lumen.core.http:ctx-get req :remote-addr)
                           "-"))
                 (host (or (h "host") ""))
                 (meth (or (lumen.core.http:req-method req) "GET"))
                 (path (or (lumen.core.http:req-path req) "/"))
                 (resp (funcall next req))
                 (ms   (/ (- (get-internal-real-time) t0)
                          (/ internal-time-units-per-second 1000.0)))
                 (msi  (round ms))
                 (st   (lumen.core.http:resp-status resp))
                 (ts   (local-time:format-timestring
                        nil (local-time:now)
                        :format '(:year "-" (:month 2) "-" (:day 2) " "
                                  (:hour 2) ":" (:min 2) ":" (:sec 2)))))
            (if color-enabled
                ;; Placeholders: ts ip meth host path ESC color st ESC msi
                (format stream "~&[~A] ~A ~A ~A ~A -> ~C[0;~am~D~C[0m (~D ms)~%"
                        ts ip meth host path
                        #\Esc (%status-color st) st #\Esc msi)
              (format stream "~&[~A] ~A ~A ~A ~A -> ~D (~D ms)~%"
                      ts ip meth host path st msi))
            (finish-output stream)
            resp))))))

;; --- Query parser -----------------------------------------------------------
(defun %url-decode (s)
  "Decode minimal: %HH et '+' -> espace."
  (when s
    (with-output-to-string (out)
      (loop for i from 0 below (length s) do
            (let ((c (char s i)))
              (cond
                ((char= c #\+) (write-char #\Space out))
                ((and (char= c #\%)
                      (<= (+ i 2) (1- (length s))))
                 (let* ((h1 (digit-char-p (char s (1+ i)) 16))
                        (h2 (digit-char-p (char s (+ i 2)) 16)))
                   (if (and h1 h2)
                       (progn
                         (write-char (code-char (+ (* h1 16) h2)) out)
                         (incf i 2))
                       (write-char c out))))
                (t (write-char c out))))))))

(defun parse-query-string-to-alist (qs)
  "Transforme \"a=1&q[s][!=]=done\" -> ((\"a\" . \"1\") (\"q[s][!=]\" . \"done\")).
   Gère intelligemment les opérateurs !=, >=, <= dans les clés."
  (if (or (null qs) (zerop (length qs)))
      nil
      (loop for pair in (uiop:split-string qs :separator "&")
            ;; On cherche la position du séparateur '='
            for p = (let ((len (length pair)))
                      (loop for i from 0 below len
                            ;; On a trouvé un '='
                            when (char= (char pair i) #\=)
                              ;; EST-CE LE VRAI SÉPARATEUR ?
                              ;; Oui si c'est le tout premier caractère (clé vide)
                              ;; OU si le caractère précédent n'est PAS un morceau d'opérateur (!, >, <)
                              when (or (zerop i)
                                       (not (member (char pair (1- i)) '(#\! #\> #\<))))
                                return i))
            
            ;; Extraction Clé / Valeur
            for k = (%url-decode (subseq pair 0 (or p (length pair))))
            for v = (%url-decode (if p (subseq pair (1+ p)) ""))
            collect (cons k v))))

(define-middleware query-parser (req next)
  ;; Si req-query est une string, on la remplace par une alist.
  (let* ((q (lumen.core.http:req-query req)))
    (when (stringp q)
      (setf (lumen.core.http:req-query req) (parse-query-string-to-alist q)))
    (funcall next req)))

(defun path-matches? (path paths prefixes)
  (or (and paths (member path paths :test #'string=))
      (and prefixes
           (some (lambda (pfx)
                   (and (<= (length pfx) (length path))
                        (string= pfx path :end2 (length pfx))))
                 prefixes))))

(defun json-only (&key paths prefixes exclude-paths exclude-prefixes (respect-existing t))
  "Forcer JSON uniquement pour les endpoints API ciblés,
   sans écraser les assets (MIME issu de lumen.core.mime)."
  (lambda (next)
    (lambda (req)
      (let* ((resp (funcall next req))
             (path (lumen.core.http:req-path req))
             ;; MIME déduit de l’URL (utile pour URLs statiques /app/...):
             (guess (lumen.core.mime:content-type-for path))
             (ct (cdr (assoc "content-type" (lumen.core.http:resp-headers resp)
                             :test #'string-equal)))
             (should-apply (and (path-matches? path paths prefixes)
                                (not (path-matches? path exclude-paths exclude-prefixes))
                                ;; si l’URL ressemble à un asset connu (js, css, …), on n’applique pas
                                (not (string/= guess "application/octet-stream")) ; => connu
                                ;; on veut **forcer** JSON uniquement si guess n'est PAS un type d’asset
                                ;; donc on inverse : n'appliquer que si guess est octet-stream (inconnu)
                                )))
        ;; Si la route est explicitement API (ex: /api, /auth, /openapi.json),
        ;; passe plutôt paths/prefixes pour matcher celles-ci. Exemple d’usage plus bas.
        (cond
          ;; si l’URL est un asset (guess ≠ octet-stream), NE RIEN FAIRE
          ((and guess (not (string= (string-downcase guess) "application/octet-stream")))
           resp)
          ;; si on respecte un type existant non JSON (SSE, CSV, etc.), NE RIEN FAIRE
          ((and respect-existing ct
                (not (cl-ppcre:scan "^application/json" (string-downcase ct))))
           resp)
          ;; si SSE, NE RIEN FAIRE
          ((and ct (string= (string-downcase ct) "text/event-stream"))
           resp)
          ;; pour les endpoints API sans CT explicite : forcer JSON
          ((path-matches? path paths prefixes)
           (setf (lumen.core.http:resp-headers resp)
                 (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                            "content-type"
                                            "application/json; charset=utf-8"))
           resp)
          (t resp))))))

#|
(defun cors (&key (origins '("*")) (methods '("GET" "POST" "PUT" "PATCH" "DELETE" "OPTIONS"))
                  (headers '("Content-Type" "Authorization"))
                  (expose '()) (credentials nil) (max-age 600))
  "CORS middleware. Répond aux préflights (OPTIONS) et enrichit toutes les réponses."
  (lambda (next)
    (lambda (req)
      (let* ((req-h (lumen.core.http:req-headers req))
             (origin (cdr (assoc "origin" req-h :test #'string-equal)))
             (req-method (cdr (assoc "access-control-request-method" req-h :test #'string-equal)))
             (preflight (and (stringp req-method)
                             (string= (slot-value req 'lumen.core.http::method) "OPTIONS")))
             (allow-origin (cond
                             ((or (null origin) (member "*" origins :test #'string=)) "*")
                             ((member origin origins :test #'string=) origin)
                             (t nil))))
        (labels ((decorate (resp)
                   (when allow-origin
                     (setf (lumen.core.http:resp-headers resp)
                           (-> (lumen.core.http:resp-headers resp)
                               (lumen.utils:ensure-header "access-control-allow-origin" allow-origin)
                               (lumen.utils:ensure-header "vary" "Origin"))))
                   (when credentials
                     (setf (lumen.core.http:resp-headers resp)
                           (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                          "access-control-allow-credentials" "true")))
                   (when expose
                     (setf (lumen.core.http:resp-headers resp)
                           (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                          "access-control-expose-headers"
                                          (format nil "~{~A~^, ~}" expose))))
                   resp))
          (if preflight
              (let ((resp (make-instance 'lumen.core.http:response
                                         :status 204 :headers nil :body "")))
                (when allow-origin
                  (setf (lumen.core.http:resp-headers resp)
                        (-> (lumen.core.http:resp-headers resp)
                            (lumen.utils:ensure-header "access-control-allow-origin" allow-origin)
                            (lumen.utils:ensure-header "access-control-allow-methods" (format nil "~{~A~^, ~}" methods))
                            (lumen.utils:ensure-header "access-control-allow-headers" (format nil "~{~A~^, ~}" headers))
                            (lumen.utils:ensure-header "access-control-max-age" (princ-to-string max-age)))))
                (when credentials
                  (setf (lumen.core.http:resp-headers resp)
                        (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                       "access-control-allow-credentials" "true")))
                resp)
(decorate (funcall next req))))))))
|#

;;;; ----------------------------------------------------------------------------
;;;; CORS dual-mode (dev permissif / prod strict) + factory via lumen.core.config
;;;; ----------------------------------------------------------------------------
(defun cors (&key (mode :dev)                       ; :dev | :prod
                  (origins '("*"))                  ; whitelist en :prod, ou '("*") en :dev
                  (methods '("GET" "POST" "PUT" "PATCH" "DELETE" "OPTIONS"))
                  (headers '("Content-Type" "Authorization"))
                  (expose '()) (credentials nil) (max-age 600))
  "CORS middleware dual-mode (dev permissif / prod strict).
- MODE       : :dev (permissif) | :prod (strict whitelist)
- ORIGINS    : liste d'origines autorisées (prod) ou '(\"*\") (dev)
- CREDENTIALS: si T, on n’émet jamais \"*\" ; on renvoie l’Origin effectif si autorisé
- MAX-AGE    : secondes de cache préflight"
  (lambda (next)
    (lambda (req)
      (let* ((req-h (lumen.core.http:req-headers req))
             (origin (cdr (assoc "origin" req-h :test #'string-equal)))
             (acr-method (cdr (assoc "access-control-request-method" req-h :test #'string-equal)))
             (acr-headers-req (cdr (assoc "access-control-request-headers" req-h :test #'string-equal)))
             (is-options (string= (slot-value req 'lumen.core.http::method) "OPTIONS"))
             (preflight (and is-options (stringp acr-method)))
             (allow-origin
               (cond
                 ((eq mode :dev)
                  ;; En dev: permissif ; si credentials, renvoyer origin (si présent) plutôt que "*"
                  (or (and credentials origin) "*"))
                 ((eq mode :prod)
                  (cond
                    ((and origin (member origin origins :test #'string=)) origin)
                    ((member "*" origins :test #'string=) "*") ; si on veut forcer permissif en prod (déconseillé)
                    (t nil)))
                 (t nil))))
        (labels
            ((%ensure (resp name value)
               (setf (lumen.core.http:resp-headers resp)
                     (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) name value)))
             (decorate (resp)
               ;; Toujours Vary: Origin (CDN-safe)
               (%ensure resp "vary" "Origin")
               ;; Allow-Origin (+ credentials si applicable)
               (when allow-origin
                 (let ((effective
                         (if (and credentials (string= allow-origin "*") (stringp origin))
                             origin
                             allow-origin)))
                   (%ensure resp "access-control-allow-origin" effective)
                   (when (and credentials (not (string= effective "*")))
                     (%ensure resp "access-control-allow-credentials" "true"))))
               ;; Expose-Headers
               (when expose
                 (%ensure resp "access-control-expose-headers" (format nil "~{~A~^, ~}" expose)))
               resp))
          (if preflight
              (let ((resp (make-instance 'lumen.core.http:response
                                         :status 204 :headers nil :body "")))
                ;; Vary complet pour préflight
                (dolist (h '("Origin" "Access-Control-Request-Method" "Access-Control-Request-Headers"))
                  (%ensure resp "vary" h))
                (when allow-origin
                  (let* ((effective
                           (if (and credentials (string= allow-origin "*") (stringp origin))
                               origin
                               allow-origin))
                         (allow-headers
                           ;; écho des headers demandés par le client + base configurée
                           (let ((base (format nil "~{~A~^, ~}" headers)))
                             (if (and acr-headers-req (plusp (length acr-headers-req)))
                                 (format nil "~A, ~A" base acr-headers-req)
                                 base))))
                    (%ensure resp "access-control-allow-origin" effective)
                    (%ensure resp "access-control-allow-methods" (format nil "~{~A~^, ~}" methods))
                    (%ensure resp "access-control-allow-headers" allow-headers)
                    (%ensure resp "access-control-max-age" (princ-to-string max-age))
                    (when (and credentials (not (string= effective "*")))
                      (%ensure resp "access-control-allow-credentials" "true"))))
                resp)
              (decorate (funcall next req))))))))

;;; --------------------------------------------------------------------------
;;; Factory branchée sur lumen.core.config
;;; --------------------------------------------------------------------------

(eval-when (:compile-toplevel :load-toplevel :execute)
  (unless (find-package :lumen.core.config)
    (error "Le package :lumen.core.config doit être chargé avant cors-auto.")))

(defun %prod-profile-p (profile)
  "Renvoie T si le profil indique un environnement de prod/staging."
  (member profile '(:prod :production :staging) :test #'eq))

(defun cors-auto ()
  "Construit un mw CORS depuis lumen.core.config.
Clés supportées (profilées par ENV grâce à lumen.core.config):
  - :cors/origins        → liste (CSV ou multiple) d’origines autorisées
  - :cors/credentials    → bool
  - :cors/max-age        → durée (ex: \"600s\", \"10m\")
  - :cors/headers        → liste d’en-têtes acceptés au préflight
  - :cors/methods        → liste de méthodes autorisées
  - :cors/expose         → liste d’en-têtes exposés côté client"
  (let* ((profile (lumen.core.config:cfg-profile))
         (mode    (if (%prod-profile-p profile) :prod :dev))
         (origins (or (lumen.core.config:cfg-get-list :cors/origins) (if (eq mode :prod) '() '("*"))))
         (creds   (or (lumen.core.config:cfg-get-bool :cors/credentials :default nil) nil))
         (max-age (or (lumen.core.config:cfg-get-duration :cors/max-age :default "600s") 600))
         (hdrs    (or (lumen.core.config:cfg-get-list :cors/headers) '("Content-Type" "Authorization")))
         (meths   (or (lumen.core.config:cfg-get-list :cors/methods)
                      '("GET" "POST" "PUT" "PATCH" "DELETE" "OPTIONS")))
         (xpose   (or (lumen.core.config:cfg-get-list :cors/expose) '())))
    (cors :mode mode
          :origins origins
          :credentials creds
          :max-age max-age
          :headers hdrs
          :methods meths
          :expose xpose)))

#|
(defun guess-content-type (pathname)
  (let* ((s (string-downcase (or (pathname-type pathname) ""))))
    (cond
      ((string= s "html") "text/html; charset=utf-8")
      ((string= s "css")  "text/css; charset=utf-8")
      ((string= s "js")   "application/javascript; charset=utf-8")
      ((member s '("png" "jpg" "jpeg" "gif" "webp") :test #'string=) (format nil "image/~A" s))
      ((string= s "svg")  "image/svg+xml")
      ((string= s "json") "application/json; charset=utf-8")
      (t "application/octet-stream"))))
|#

(defun read-file-bytes (pathname)
  (with-open-file (in pathname :element-type '(unsigned-byte 8))
    (let* ((size (file-length in))
           (buf  (make-array size :element-type '(unsigned-byte 8))))
      (read-sequence buf in)
      buf)))
#|
(defun safe-join (root rel)
  "Concatène ROOT et REL en empêchant tout échappement (.., backslashes, drive, etc.)."
  (print "IN SAFE JOIN")
  (let* ((rel* (or rel ""))
         ;; remplace backslashes et retire .. :
         (rel1 (substitute #\/ #\\ rel*))
         (rel2 (with-output-to-string (s)
                 (dolist (part (remove-if #'(lambda (p) (or (string= p "")
                                                            (string= p ".")))
                                          (uiop:split-string rel1 :separator "/")))
                   (unless (string= part "..")
                     (format s "~A/" part)))))
         ;; retire le slash final ajouté si rel ne devait pas en avoir
         (rel3 (if (and (> (length rel2) 0)
                        (char= (char rel2 (1- (length rel2))) #\/)
                        (not (and (> (length rel1) 0)
                                  (char= (char rel1 (1- (length rel1))) #\/))))
                  (subseq rel2 0 (1- (length rel2)))
                  rel2))
         (abs (merge-pathnames rel3 (uiop:ensure-directory-pathname (truename root)))))
    ;; sécurité : l’abs doit rester sous root
    (let* ((root* (namestring (uiop:ensure-directory-pathname (truename root))))
           (abs*  (namestring (truename abs))))
      (if (and (>= (length abs*) (length root*))
               (string= root* abs* :end2 (length root*)))
          abs
          (uiop:merge-pathnames* #P"__forbidden__" (truename root))))))
|#
(defun safe-join (root rel)
  "Concatène ROOT et REL en empêchant tout échappement (.., backslashes, drive, etc.).
   Ne signale pas d'erreur si la cible n'existe pas ; renvoie un pathname sous ROOT."
  (labels ((forbidden ()
             ;; retourne une cible sûre sous root qui ne matche rien
             (merge-pathnames #P"__forbidden__" (uiop:ensure-directory-pathname (truename root)))))
    (handler-case
        (let* ((rel* (or rel ""))
               ;; normalise séparateurs et purifie les segments
               (rel1 (substitute #\/ #\\ rel*))
               ;; refuse une notation drive Windows "C:" en tête
               (drive-pos (position #\: rel1))
               (starts-with-drive (and drive-pos (= drive-pos 1))) ; ex: "C:/..."
               ;; reconstruit sans "." ni ".."
               (rel2 (with-output-to-string (s)
                       (dolist (part
                                (remove-if (lambda (p) (or (string= p "")
                                                           (string= p ".")
                                                           (string= p "..")))
                                           (uiop:split-string rel1 :separator "/")))
                         (when (> (length part) 0)
                           (format s "~A/" part)))))
               ;; retire éventuellement le slash final ajouté si l’original n’en avait pas
               (rel3 (if (and (> (length rel2) 0)
                              (char= (char rel2 (1- (length rel2))) #\/)
                              (not (and (> (length rel1) 0)
                                        (char= (char rel1 (1- (length rel1))) #\/))))
                         (subseq rel2 0 (1- (length rel2)))
                         rel2))
               (root-dir (uiop:ensure-directory-pathname (truename root)))
               ;; IMPORTANT : ne pas faire (truename abs) ici !
               (abs (merge-pathnames rel3 root-dir)))
          (if starts-with-drive
              (forbidden)
              ;; Vérification simple de confinement (au cas où)
              (let* ((root-str (namestring root-dir))
                     (abs-str  (namestring (uiop:ensure-pathname abs))))
                (if (and (>= (length abs-str) (length root-str))
                         (string= root-str abs-str :end2 (length root-str)))
                    abs
                    (forbidden)))))
      (error () (forbidden)))))

(defun directory-index-html (dir rel-url &key (hide-dotfiles t))
  "Génère un index HTML simple pour DIR.
REL-URL doit commencer par / et préciser le chemin demandé (avec / final pour les dossiers)."
  (let* ((rel (or rel-url "/"))
         (rel* (if (char= (char rel (1- (length rel))) #\/) rel (concatenate 'string rel "/")))
         (files (uiop:directory-files dir))
         (subs  (uiop:subdirectories dir)))
    (labels ((visible-p (pn)
               (let ((name (car (last (uiop:split-string (namestring pn) :separator "/")))))
                 (or (not hide-dotfiles)
                     (and name (not (and (> (length name) 0) (char= (char name 0) #\.))))))))
      (with-output-to-string (s)
        (format s "<!doctype html><meta charset='utf-8'><title>Index of ~A</title>" rel*)
        (format s "<style>body{font:14px system-ui,Segoe UI,Roboto,sans-serif;margin:24px}ul{list-style:none;padding-left:0}li{margin:2px 0}</style>")
        (format s "<h1>Index of ~A</h1><ul>" rel*)
        (when (> (length rel*) 1)
          ;; lien vers le parent
          (let* ((trim (if (char= (char rel* (1- (length rel*))) #\/)
                           (subseq rel* 0 (1- (length rel*)))
                           rel*))
                 (pidx (or (position #\/ trim :from-end t) 0))
                 (up (if (= pidx 0) "/" (subseq trim 0 pidx))))
            (format s "<li>↩︎ <a href='~A'>..</a></li>" up)))
        (dolist (sd (sort (remove-if-not #'visible-p subs) #'string< :key #'namestring))
          (let* ((nm (car (last (uiop:split-string (namestring sd) :separator "/")))))
            (format s "<li>📁 <a href='~A/'>~A/</a></li>" nm nm)))
        (dolist (f (sort (remove-if-not #'visible-p files) #'string< :key #'namestring))
          (let* ((nm (car (last (uiop:split-string (namestring f) :separator "/")))))
            (format s "<li>📄 <a href='~A'>~A</a></li>" nm nm)))
        (format s "</ul>")))))

(defun method-get-or-head-p (req)
  (let ((m (slot-value req 'lumen.core.http::method)))
    (or (string= m "GET") (string= m "HEAD"))))

(defun %file-rfc1123 (pn)
  (let ((ut (ignore-errors (file-write-date pn))))
    (and ut (lumen.utils:format-http-date ut))))

(defun %weak-etag-from-file (pn)
  "Weak ETag sans lecture du contenu: taille+mtime."
  (handler-case
      (let* ((size (with-open-file (in pn :direction :input :element-type '(unsigned-byte 8))
                     (file-length in)))
             (mt   (file-write-date pn)))
        (format nil "W/\"~X-~X\"" size mt))
    (error () nil)))

;;; ---------- Static middleware ------------------------------------------------
#|
(defun static (&key prefix dir
                 (auto-index t)
                 (try-index "index.html")
                 (redirect-dir t)
                 (hide-dotfiles t)
                 (file-cache-secs 3600)
                 (dir-cache-secs 300)
		 (spa-fallback nil))
  "Servez des fichiers ET des dossiers sous PREFIX à partir du dossier DIR.

Options:
- AUTO-INDEX      : T → génère un listing HTML si pas d'index.
- TRY-INDEX       : nom d'index à servir (string) ou NIL pour désactiver.
- REDIRECT-DIR    : T → redirige /chemin → /chemin/ (301) pour les dossiers.
- HIDE-DOTFILES   : T → masque fichiers/dirs commençant par '.' dans le listing.
- FILE-CACHE-SECS : « Cache-Control » pour fichiers (par défaut 3600).
- DIR-CACHE-SECS  : « Cache-Control » pour index/dir (par défaut 300).
- SPA-FALLBACK : string (ex. \"index.html\") servi quand *aucun fichier*
  ne correspond sous PREFIX, pour GET/HEAD uniquement (idéal pour SPA).

NB: Le corps est envoyé en octets (vecteur (unsigned-byte 8))."
  (let* ((prefix* (or prefix "/"))
         (root*   (truename dir))
         (plen    (length prefix*)))
    (lambda (next)
      (lambda (req)
        (let ((path (lumen.core.http:req-path req)))
          (if (lumen.utils:str-prefix-p prefix* path)	      
              (let* ((rel    (subseq path plen)) ; peut être "" ou "foo/bar"
                     (target (safe-join root* rel))
                     (dir-truename (ignore-errors (lumen.utils:probe-directory
						   target)))
		     (ut (file-write-date pathname)))
                (handler-case
                    (cond
                      ;; -------- Dossier ----------
                      (dir-truename
                       (let* ((has-slash (and (> (length path) 0)
                                              (char= (char path (1- (length path))) #\/)))
                              (dir* (uiop:ensure-directory-pathname dir-truename)))
                         (cond
                           ;; /dir → /dir/
                           ((and redirect-dir (not has-slash))
                            (make-instance 'lumen.core.http:response
                                           :status 301
                                           :headers (list (cons "location" (concatenate 'string path "/"))
                                                          (cons "cache-control" (format nil "public, max-age=~D" dir-cache-secs)))
                                           :body ""))
                           ;; index.html si présent
                           ((and try-index (probe-file (merge-pathnames try-index dir*)))
                            (let* ((pn (merge-pathnames try-index dir*))
                                   (bytes (read-file-bytes pn)))
                              (make-instance 'lumen.core.http:response
                                             :status 200
                                             :headers (list (cons "content-type" (guess-content-type pn))
                                                            (cons "cache-control" (format nil "public, max-age=~D" dir-cache-secs)))
                                             :body bytes)))
                           ;; listing auto
                           (auto-index
                            (let* ((html (directory-index-html dir* (if (plusp (length path)) path "/")
                                                               :hide-dotfiles hide-dotfiles)))
                              (make-instance 'lumen.core.http:response
                                             :status 200
                                             :headers (list (cons "content-type" "text/html; charset=utf-8")
                                                            (cons "cache-control" (format nil "public, max-age=~D" dir-cache-secs)))
                                             :body html)))
                           (t
			    ;; pas d'index ni listing → fallback SPA si demandé, sinon 404
                            (if (and spa-fallback (method-get-or-head-p req)
                                     (probe-file (merge-pathnames spa-fallback root*)))
                                (let* ((pn (merge-pathnames spa-fallback root*))
                                       (bytes (read-file-bytes pn)))
                                  (make-instance 'lumen.core.http:response
                                                 :status 200
                                                 :headers (list (cons "content-type" "text/html; charset=utf-8")
                                                                (cons "cache-control" "no-store"))
                                                 :body bytes))
                                (lumen.core.http:respond-404 "Directory index disabled"))))))
                      ;; -------- Fichier ----------
                      ((probe-file target)
		       (let* ((bytes (read-file-bytes target)))
                         (make-instance 'lumen.core.http:response
                                        :status 200
                                        :headers (list (cons "content-type" (guess-content-type target))
						       (cons "cache-control" (format nil "public, max-age=~D" file-cache-secs)))
                                        :body bytes)))
                      ;; -------- Rien pour ce prefix → passe au suivant ----------
                      (t
		       (if (and spa-fallback (method-get-or-head-p req)
                                (probe-file (merge-pathnames spa-fallback root*)))
                           (let* ((pn (merge-pathnames spa-fallback root*))
                                  (bytes (read-file-bytes pn)))
                             (make-instance 'lumen.core.http:response
                                            :status 200
                                            :headers (list (cons "content-type" "text/html; charset=utf-8")
                                                           (cons "cache-control" "no-store"))
                                            :body bytes))
                           (funcall next req))))
                  (error ()
		    (lumen.core.http:respond-404 "File error"))))
              ;; prefix ne matche pas → passe au suivant
(funcall next req)))))))
|#
#|
(defun static (&key prefix dir
                 (auto-index t)
                 (try-index "index.html")
                 (redirect-dir t)
                 (hide-dotfiles t)
                 (file-cache-secs 3600)
                 (dir-cache-secs 300)
                 (spa-fallback nil))
  "Servez des fichiers ET des dossiers sous PREFIX à partir du dossier DIR (optimisé streaming/Range).

Options:
- AUTO-INDEX      : T → génère un listing HTML si pas d'index.
- TRY-INDEX       : nom d'index à servir (string) ou NIL pour désactiver.
- REDIRECT-DIR    : T → redirige /chemin → /chemin/ (301) pour les dossiers.
- HIDE-DOTFILES   : T → masque fichiers/dirs commençant par '.' dans le listing.
- FILE-CACHE-SECS : « Cache-Control » pour fichiers (par défaut 3600).
- DIR-CACHE-SECS  : « Cache-Control » pour index/dir (par défaut 300).
- SPA-FALLBACK    : string (ex. \"index.html\") servi quand *aucun fichier*
  ne correspond sous PREFIX, pour GET/HEAD uniquement (idéal pour SPA)."
  (let* ((prefix* (or prefix "/"))
         (root*   (truename dir))
         (plen    (length prefix*)))
    (lambda (next)
      (lambda (req)
        (let ((path (lumen.core.http:req-path req)))
          (if (and (>= (length path) plen)
                   (string= prefix* path :end2 plen)) ; évite dépendre d’un util ici
              (let* ((rel    (subseq path plen))         ; "" ou "foo/bar"
                     (target (safe-join root* rel))
                     (dir-truename (ignore-errors (lumen.utils:probe-directory target))))
                (handler-case
                    (cond
                      ;; -------- Dossier ----------
                      (dir-truename
                       (let* ((has-slash (and (> (length path) 0)
                                              (char= (char path (1- (length path))) #\/)))
                              (dir* (uiop:ensure-directory-pathname dir-truename)))
                         (cond
                           ;; /dir → /dir/ (301)
                           ((and redirect-dir (not has-slash))
                            (make-instance 'lumen.core.http:response
                                           :status 301
                                           :headers (list (cons "location" (concatenate 'string path "/"))
                                                          (cons "cache-control" (format nil "public, max-age=~D" dir-cache-secs)))
                                           :body ""))
                           ;; index.html si présent → respond-file (streaming + Range)
                           ((and try-index (probe-file (merge-pathnames try-index dir*)))
                            (let* ((pn   (merge-pathnames try-index dir*))
                                   (ct   (guess-content-type pn))
                                   (lm   (%file-rfc1123 pn))
                                   (etag (%weak-etag-from-file pn))
                                   (resp (lumen.core.http-range:respond-file
                                          req pn
                                          :content-type ct
                                          :last-modified-rfc1123 lm
                                          :etag etag)))
                              (setf (lumen.core.http:resp-headers resp)
                                    (lumen.utils:ensure-header
				     (lumen.core.http:resp-headers resp)
                                     "cache-control"
                                     (format nil "public, max-age=~D" dir-cache-secs)))
                              resp))
                           ;; listing auto (génère HTML en mémoire)
                           (auto-index
                            (let* ((html (directory-index-html
					  dir* (if (plusp (length path)) path "/")
                                          :hide-dotfiles hide-dotfiles))
                                   (lm   (%file-rfc1123 dir*))
                                   (hdrs (list (cons "content-type" "text/html; charset=utf-8")
                                               (cons "cache-control"
						     (format nil "public, max-age=~D"
							     dir-cache-secs)))))
                              (when lm
                                (setf hdrs (lumen.utils:ensure-header
                                            hdrs "last-modified" lm)))
                              (make-instance 'lumen.core.http:response
                                             :status 200 :headers hdrs :body html)))
                           ;; pas d'index ni listing → SPA fallback si demandé, sinon 404
			   (t
                            (if (and spa-fallback (method-get-or-head-p req)
                                     (probe-file (merge-pathnames spa-fallback root*)))
                                (let* ((pn   (merge-pathnames spa-fallback root*))
                                       (lm   (%file-rfc1123 pn))
                                       (resp (lumen.core.http-range:respond-file
                                              req pn
                                              :content-type "text/html; charset=utf-8"
                                              :last-modified-rfc1123 lm)))
                                  ;; SPA: pas de cache persistant
                                  (setf (lumen.core.http:resp-headers resp)
                                        (lumen.utils:ensure-header
                                         (lumen.core.http:resp-headers resp)
                                         "cache-control" "no-store"))
                                  resp)
                                (lumen.core.http:respond-404 "Directory index disabled"))))))
		      
                      ;; -------- Fichier ----------
                      ((probe-file target)
                       (let* ((ct   (guess-content-type target))
                              (lm   (%file-rfc1123 target))
                              (etag (%weak-etag-from-file target))
                              (resp (lumen.core.http-range:respond-file
                                     req target
                                     :content-type ct
                                     :last-modified-rfc1123 lm
                                     :etag etag)))
                         (setf (lumen.core.http:resp-headers resp)
                               (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                                          "cache-control"
                                                          (format nil "public, max-age=~D" file-cache-secs)))
			 (setf (lumen.core.http:resp-headers resp)
                               (lumen.utils:ensure-header
				(lumen.core.http:resp-headers resp)
                                "Vary" "Accept-Encoding"))
                         resp))
		      
                      ;; -------- Rien pour ce prefix ----------
                      (t
                       (if (and spa-fallback (method-get-or-head-p req)
                                (probe-file (merge-pathnames spa-fallback root*)))
                           (let* ((pn   (merge-pathnames spa-fallback root*))
                                  (lm   (%file-rfc1123 pn))
                                  (resp (lumen.core.http-range:respond-file
                                         req pn
                                         :content-type "text/html; charset=utf-8"
                                         :last-modified-rfc1123 lm)))
                             (setf (lumen.core.http:resp-headers resp)
                                   (lumen.utils:ensure-header
                                    (lumen.core.http:resp-headers resp)
                                    "cache-control" "no-store"))
                             resp)
                           (funcall next req))))
                  (error ()
                    (lumen.core.http:respond-404 "File error"))))
              ;; prefix ne matche pas → passer au suivant
              (funcall next req)))))))
|#
(defun static (&key prefix dir
                 (auto-index t)
                 (try-index "index.html")
                 (redirect-dir t)
                 (hide-dotfiles t)
                 (file-cache-secs 3600)
                 (dir-cache-secs 300)
                 (spa-fallback nil))
  "Servez des fichiers ET des dossiers sous PREFIX à partir du dossier DIR (200 plein + Range/206)."
  (let* ((prefix* (or prefix "/"))
         (root*   (truename dir))
         (plen    (length prefix*)))
    (labels
        ;; ---------- Helpers ----------
        ((read-file-bytes (pn)
           (with-open-file (in pn :direction :input :element-type '(unsigned-byte 8))
             (let* ((len (file-length in))
                    (buf (make-array len :element-type '(unsigned-byte 8))))
               (read-sequence buf in)
               buf)))

         (file-size (pn)
           (with-open-file (in pn :direction :input :element-type '(unsigned-byte 8))
             (file-length in)))

         (header (req name)
           (cdr (assoc name (lumen.core.http:req-headers req) :test #'string-equal)))

         (ensure-h (hdrs k v)
           (lumen.utils:ensure-header hdrs k v))

         (vary-accept-encoding (resp)
           (setf (lumen.core.http:resp-headers resp)
                 (ensure-h (lumen.core.http:resp-headers resp) "Vary" "Accept-Encoding"))
           resp)

         ;; Parse simple “Range: bytes=start-end” (une seule plage). Retourne (values start end ok?)
         (parse-range (s total)
           (handler-case
               (progn
                 (cl-ppcre:register-groups-bind (a b)
                     ("^bytes=([0-9]*)-([0-9]*)$" (string-trim '(#\Space) s))
                   (let* ((sa (and a (> (length a) 0) (parse-integer a :junk-allowed t)))
                          (sb (and b (> (length b) 0) (parse-integer b :junk-allowed t))))
                     (cond
                       ;; bytes=START- (jusqu'à fin)
                       ((and sa (null sb) (>= sa 0) (< sa total))
                        (values sa (1- total) t))
                       ;; bytes=-SUFFIX  (les sb derniers octets)
                       ((and (null sa) sb (> sb 0))
                        (let* ((len (min sb total))
                               (st  (- total len)))
                          (values st (1- total) t)))
                       ;; bytes=START-END
                       ((and sa sb (>= sa 0) (>= sb sa) (< sa total))
                        (values sa (min sb (1- total)) t))
                       (t (values 0 0 nil))))))
             (error () (values 0 0 nil))))

         ;; 200 plein (octets) – HEAD renvoie juste les headers.
         (serve-file-200 (req pn &key content-type cache-secs etag last-modified)
           (let* ((ct   (or content-type (lumen.core.mime:guess-content-type pn)))
                  (lm   last-modified)
                  (etag etag)
                  (hdrs (list (cons "content-type" ct)
                              (cons "accept-ranges" "bytes")
                              (cons "cache-control" (format nil "public, max-age=~D" (or cache-secs 0))))))
             (when lm   (setf hdrs (ensure-h hdrs "last-modified" lm)))
             (when etag (setf hdrs (ensure-h hdrs "etag"           etag)))
             (if (string= (lumen.core.http:req-method req) "HEAD")
                 (make-instance 'lumen.core.http:response
                                :status 200 :headers hdrs :body "")
               (make-instance 'lumen.core.http:response
                              :status 200 :headers hdrs :body (read-file-bytes pn)))))

         ;; 206 Range – writer **borné** (retourne) + Content-Length exact.
         (serve-file-206 (req pn start end &key content-type cache-secs etag last-modified)
           (let* ((total (file-size pn))
                  (len   (max 0 (1+ (- end start))))
                  (ct    (or content-type (lumen.core.mime:guess-content-type pn)))
                  (hdrs  (list
                          (cons "content-type" ct)
                          (cons "accept-ranges" "bytes")
                          (cons "content-range" (format nil "bytes ~D-~D/~D" start end total))
                          (cons "content-length" (write-to-string len))
                          (cons "cache-control" (format nil "public, max-age=~D" (or cache-secs 0))))))
             (when last-modified (setf hdrs (ensure-h hdrs "last-modified" last-modified)))
             (when etag          (setf hdrs (ensure-h hdrs "etag"          etag)))
             (if (string= (lumen.core.http:req-method req) "HEAD")
                 (make-instance 'lumen.core.http:response
                                :status 206 :headers hdrs :body "")
               (make-instance 'lumen.core.http:response
                              :status 206
                              :headers hdrs
                              :body (lambda (send)
                                      (with-open-file (in pn :direction :input :element-type '(unsigned-byte 8))
                                        (file-position in start)
                                        (let ((buf (make-array (min len 65536)
                                                               :element-type '(unsigned-byte 8)))
                                              (left len))
                                          (loop while (> left 0) do
                                            (let* ((n (min left (length buf)))
                                                   (rd (read-sequence buf in :end n)))
                                              (when (or (null rd) (= rd 0)) (return))
                                              (funcall send (if (= rd (length buf))
                                                                buf
                                                                (subseq buf 0 rd)))
                                              (decf left rd))))))))))
         )

      (lambda (next)
        (lambda (req)
          (let ((path (lumen.core.http:req-path req)))
            (if (and (>= (length path) plen)
                     (string= prefix* path :end2 plen))
                (let* ((rel    (subseq path plen)) ; "" ou "foo/bar"
                       (target (safe-join root* rel))
                       (dir-truename (ignore-errors (lumen.utils:probe-directory target)))
                       (rng-h (header req "Range")))
                  (handler-case
                      (cond
                        ;; -------- Dossier ----------
                        (dir-truename
                         (let* ((has-slash (and (> (length path) 0)
                                                (char= (char path (1- (length path))) #\/)))
                                (dir* (uiop:ensure-directory-pathname dir-truename)))
                           (cond
                             ;; /dir → /dir/ (301)
                             ((and redirect-dir (not has-slash))
                              (make-instance 'lumen.core.http:response
                                             :status 301
                                             :headers (list (cons "location" (concatenate 'string path "/"))
                                                            (cons "cache-control" (format nil "public, max-age=~D" dir-cache-secs)))
                                             :body ""))

                             ;; index.html présent
                             ((and try-index (probe-file (merge-pathnames try-index dir*)))
                              (let* ((pn   (merge-pathnames try-index dir*))
                                     (ct   (lumen.core.mime:guess-content-type pn))
                                     (lm   (%file-rfc1123 pn))
                                     (etag (%weak-etag-from-file pn)))
                                (multiple-value-bind (st en ok)
                                    (and rng-h (parse-range rng-h (file-size pn)))
                                  (let ((resp (if ok
                                                  (serve-file-206 req pn st en
                                                                  :content-type ct
                                                                  :cache-secs dir-cache-secs
                                                                  :etag etag
                                                                  :last-modified lm)
                                                  (serve-file-200 req pn
                                                                  :content-type ct
                                                                  :cache-secs dir-cache-secs
                                                                  :etag etag
                                                                  :last-modified lm))))
                                    (vary-accept-encoding resp)))))

                             ;; listing auto
                             (auto-index
                              (let* ((html (directory-index-html
                                            dir* (if (plusp (length path)) path "/")
                                            :hide-dotfiles hide-dotfiles))
                                     (lm   (%file-rfc1123 dir*))
                                     (hdrs (list (cons "content-type" "text/html; charset=utf-8")
                                                 (cons "cache-control" (format nil "public, max-age=~D" dir-cache-secs)))))
                                (when lm
                                  (setf hdrs (ensure-h hdrs "last-modified" lm)))
                                (make-instance 'lumen.core.http:response
                                               :status 200 :headers hdrs :body html)))

                             ;; pas d'index → SPA fallback ?
                             (t
                              (if (and spa-fallback (method-get-or-head-p req)
                                       (probe-file (merge-pathnames spa-fallback root*)))
                                  (let* ((pn   (merge-pathnames spa-fallback root*))
                                         (lm   (%file-rfc1123 pn))
                                         (resp (serve-file-200 req pn
                                                               :content-type "text/html; charset=utf-8"
                                                               :cache-secs 0
                                                               :last-modified lm)))
                                    ;; SPA : no-store
                                    (setf (lumen.core.http:resp-headers resp)
                                          (ensure-h (lumen.core.http:resp-headers resp)
                                                    "cache-control" "no-store"))
                                    resp)
                                  (lumen.core.http:respond-404 "Directory index disabled"))))))

                        ;; -------- Fichier ----------
                        ((probe-file target)
                         (let* ((ct   (lumen.core.mime:guess-content-type target))
                                (lm   (%file-rfc1123 target))
                                (etag (%weak-etag-from-file target)))
                           (multiple-value-bind (st en ok)
                               (and rng-h (parse-range rng-h (file-size target)))
                             (let ((resp (if ok
                                             (serve-file-206 req target st en
                                                             :content-type ct
                                                             :cache-secs file-cache-secs
                                                             :etag etag
                                                             :last-modified lm)
                                             (serve-file-200 req target
                                                             :content-type ct
                                                             :cache-secs file-cache-secs
                                                             :etag etag
                                                             :last-modified lm))))
                               (vary-accept-encoding resp)))))

                        ;; -------- Rien pour ce prefix ----------
                        (t
                         (if (and spa-fallback (method-get-or-head-p req)
                                  (probe-file (merge-pathnames spa-fallback root*)))
                             (let* ((pn   (merge-pathnames spa-fallback root*))
                                    (lm   (%file-rfc1123 pn))
                                    (resp (serve-file-200 req pn
                                                          :content-type "text/html; charset=utf-8"
                                                          :cache-secs 0
                                                          :last-modified lm)))
                               (setf (lumen.core.http:resp-headers resp)
                                     (ensure-h (lumen.core.http:resp-headers resp)
                                               "cache-control" "no-store"))
                               resp)
                             (funcall next req))))
                    (error ()
                      (lumen.core.http:respond-404 "File error"))))
              ;; prefix ne matche pas → passer au suivant
              (funcall next req))))))))
 
(defun static-many (&rest mounts)
  "Monte plusieurs middlewares static en cascade.
MOUNTS: soit des PLISTs pour (static ...), soit des factories (next->handler)."
  (let* ((factories
           (mapcar (lambda (m)
                     (etypecase m
                       (function m)                 ; déjà une factory
                       (list     (apply #'static m)))) ; spec → factory
                   mounts)))
    (lambda (next)
      (let ((acc next))
        (dolist (f (reverse factories) acc)
          (let ((wrapped (funcall f acc)))
            (unless (functionp wrapped)
              (error "static-many: chaque element doit etre une FACTORY (next->handler). ~
                      Cet element a retourne ~S, pas une fonction. ~%~
                      Indice: ne passe pas un HANDLER (req->resp), mais une FACTORY."
                     wrapped))
            (setf acc wrapped)))))))

;;; ---------------- Cookies ----------------------------------------------------

(define-middleware cookies-parser (req next)
  "Parse Cookie: … → req-cookies (alist)."
  (let* ((h (lumen.core.http:req-headers req))
         (raw (cdr (assoc "cookie" h :test #'string-equal))))
    (when raw
      (setf (lumen.core.http:req-cookies req)
            (lumen.core.http:parse-cookie-header raw))))
  (funcall next req))

(defun cookie (req name)
  "Lit un cookie par nom (string), ou NIL."
  (cdr (assoc name (lumen.core.http:req-cookies req) :test #'string=)))

(defun set-cookie! (resp name value &rest opts)
  "Ajoute un header Set-Cookie formaté à la réponse."
  (lumen.core.http:add-set-cookie
   resp
   (apply #'lumen.core.http:format-set-cookie name value opts)))

;;; ---------------- Error wrapper ---------------------------------------------

(defparameter *error-handler*
  (lambda (e req)
    (declare (ignore req))
    ;; Rendu JSON par défaut ; en dev on imprime une backtrace
    (let* ((msg (princ-to-string e))
           (bt  (when *debug*
                  (with-output-to-string (s)
                    (ignore-errors (uiop:print-backtrace :stream s)))))
           (payload `((:error . ((:type . "internal")
                                  (:message . ,msg)
                                  ,@(when bt (list (cons :backtrace bt))))))))
      (lumen.core.http:respond-json payload :status 500))))

(define-middleware error-wrapper (req next)
  "Capture toute condition et rend une 500 propre via *error-handler*."
  (handler-case
      (funcall next req)
    (error (e)
      (funcall *error-handler* e req))))

;;; ---------------- Request-ID -------------------------------------------------

(defun %rand-hex (nbytes)
  (let ((hex "0123456789abcdef"))
    (with-output-to-string (s)
      (dotimes (i (* 2 nbytes))
        (write-char (char hex (random 16)) s)))))

(defun make-request-id ()
  "UUIDv4-ish (hex) simple, suffisant pour corrélation de logs."
  (let ((h (%rand-hex 16)))
    ;; 8-4-4-4-12 avec bits de version 4 (optionnel simplifié)
    (format nil "~A-~A-4~A-~A~A-~A"
            (subseq h 0 8) (subseq h 8 12) (subseq h 13 16)
            (subseq h 16 17) (subseq h 17 20) (subseq h 20 32))))

(define-middleware request-id (req next)
  "Injecte un X-Request-ID dans la réponse, disponible aussi via req-ctx."
  (let* ((rid (or (lumen.core.http:ctx-get req :request-id)
                  (make-request-id)))
         (resp (progn
                 (lumen.core.http:ctx-set! req :request-id rid)
                 (funcall next req))))
    (setf (lumen.core.http:resp-headers resp)
          (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                         "x-request-id" rid))
    resp))

;; Form parser
(define-middleware form-parser (req next)
  "Parse application/x-www-form-urlencoded → ctx[:form] (alist)."
  (unless (lumen.core.http:ctx-get req :body-consumed)
    (let* ((headers (lumen.core.http:req-headers req))
           (ct  (cdr (assoc "content-type" headers :test #'string-equal)))
           (len (cdr (assoc "content-length" headers :test #'string-equal)))
           (len* (when len (parse-integer len :junk-allowed t))))
      (when (and ct len* (> len* 0)
                 (search "application/x-www-form-urlencoded" (string-downcase ct)))
        (lumen.core.http:ctx-set! req :form
          (lumen.core.body:parse-urlencoded (lumen.core.http:req-body-stream req) len*))
        (lumen.core.http:ctx-set! req :body-consumed t))))
  (funcall next req))

(define-middleware multipart-parser (req next)
  "Parse multipart/form-data → ctx[:files], ctx[:fields]."
  (unless (lumen.core.http:ctx-get req :body-consumed)
    (let* ((h (lumen.core.http:req-headers req))
           (ct (cdr (assoc "content-type" h :test #'string-equal)))
           (len (cdr (assoc "content-length" h :test #'string-equal)))
           (len* (when len (parse-integer len :junk-allowed t))))
      (when (and ct len* (> len* 0)
                 (search "multipart/form-data" (string-downcase ct)))
        (let ((res (lumen.core.body:parse-multipart
		    (lumen.core.http:req-body-stream req) len* ct)))
	  ;;(print "FILES")
	  ;;(print res)
          (when res
            (lumen.core.http:ctx-set! req :fields (cdr (assoc :fields res)))
            (lumen.core.http:ctx-set! req :files  (cdr (assoc :files  res)))
            (lumen.core.http:ctx-set! req :body-consumed t))))))
  (funcall next req))

;;; ---------------- ETag ----------------
;; FNV-1a 64-bit (simple, sans dépendance)
(defun %fnv1a-64 (bytes)
  (let* ((hash #xCBF29CE484222325)
         (prime #x100000001B3))
    (dotimes (i (length bytes))
      (setf hash (logxor hash (aref bytes i)))
      (setf hash (mod (* hash prime) (expt 2 64))))
    hash))

(defun %hex64 (u64)
  (format nil "~16,'0X" u64))

(defun %body->bytes (body)
  (etypecase body
    (string (trivial-utf-8:string-to-utf-8-bytes body))
    ((vector (unsigned-byte 8)) body)
    (null (trivial-utf-8:string-to-utf-8-bytes ""))))

(defun compute-etag (body &key (weak t))
  "Retourne une chaîne ETag, ex: W/\"AB12...-1234\""
  (let* ((bytes (%body->bytes body))
         (h (%hex64 (%fnv1a-64 bytes)))
         (sig (format nil "~A-~D" h (length bytes))))
    (if weak
        (format nil "W/~S" sig)   ; "W/" + quoted-string
        (format nil "~S" sig)))   ; strong: juste quoted-string

  )

(defun %parse-if-none-match (h)
  "Retourne une liste de tags (strings) tels que reçus, ex: (\"W/\\\"SIG\\\"\" \"\\\"SIG2\\\"\")."
  (when (and h (plusp (length h)))
    (mapcar (lambda (s) (string-trim '(#\Space #\Tab) s))
            (uiop:split-string h :separator ","))))

(defun %strip-weak (tag)
  "Retire le préfixe W/ si présent."
  (let ((s (string-trim '(#\Space #\Tab) tag)))
    (if (and (>= (length s) 2)
             (char-equal (char s 0) #\W)
             (char=      (char s 1) #\/))
        (subseq s 2)
        s)))

(defun %weak-match-p (client-tag server-tag)
  "Règle de weak comparison (RFC 7232 §2.3.2).
   client-tag et server-tag incluent les guillemets."
  (string= (%strip-weak client-tag) (%strip-weak server-tag)))

;;; Middleware ETag rendu 'safe' :
;;; - Ne tente PAS de calculer un ETag si le corps est un writer (fonction).
;;; - Si un ETag est DÉJÀ présent (ex: respond-file), utilise-le pour 304.
(defun etag (&key (weak t) (only-status '(200)) (add-cache-control nil))
  "Ajoute/valide ETag et gère 304 si If-None-Match matche.
   - WEAK : si T (défaut), génère un ETag faible (W/\"...\").
   - ONLY-STATUS : liste de statuts concernés (par défaut (200)).
   - ADD-CACHE-CONTROL : string optionnelle, ex: \"public, max-age=300\"."
  (lambda (next)
    (lambda (req)
      (let* ((resp   (funcall next req))
             (status (lumen.core.http:resp-status resp)))
        (labels
            ((ensure-etag! (resp)
               (let* ((hdrs (lumen.core.http:resp-headers resp))
                      (existing (cdr (assoc "etag" hdrs :test #'string=))))
                 (cond
                   ;; ETag déjà posé (ex: respond-file) → on le laisse.
                   (existing
                    (when add-cache-control
                      (setf (lumen.core.http:resp-headers resp)
                            (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                                       "cache-control" add-cache-control)))
                    existing)
                   ;; Corps fonction (writer) → on ne peut pas calculer → on n'ajoute pas
                   ((functionp (lumen.core.http:resp-body resp))
                    (when add-cache-control
                      (setf (lumen.core.http:resp-headers resp)
                            (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                                       "cache-control" add-cache-control)))
                    nil)
                   ;; Corps string / octets → on calcule
                   (t
                    (let* ((val (compute-etag (lumen.core.http:resp-body resp) :weak weak)))
                      (setf (lumen.core.http:resp-headers resp)
                            (lumen.utils:ensure-header hdrs "etag" val))
                      (when add-cache-control
                        (setf (lumen.core.http:resp-headers resp)
                              (lumen.utils:ensure-header (lumen.core.http:resp-headers resp)
                                                         "cache-control" add-cache-control)))
                      val))))))
          (if (and (member status only-status)
                   (method-get-or-head-p req))
              (let* ((etag-val (ensure-etag! resp))
                     (inm (cdr (assoc "if-none-match"
                                      (lumen.core.http:req-headers req)
                                      :test #'string-equal))))
                (if (and etag-val inm
                         (or (some (lambda (x) (string= x "*"))
                                   (%parse-if-none-match inm))
                             (some (lambda (x) (%weak-match-p x etag-val))
                                   (%parse-if-none-match inm))))
                    ;; 304 Not Modified : pas de corps
                    (make-instance 'lumen.core.http:response
                                   :status 304
                                   :headers (lumen.utils:ensure-header
                                             (copy-list (lumen.core.http:resp-headers resp))
                                             "etag" etag-val)
                                   :body "")
                    resp))
              ;; Pas concerné → renvoyer tel quel
              resp))))))

;;; ---------------- Helpers Accept-Encoding ----------------
(defun %parse-accept-encoding (h)
  "Retourne une alist '( (\"gzip\" . q) (\"deflate\" . q) ... ) triée par q desc."
  (labels ((parse-q (s)
             (handler-case
                 (let* ((v (read-from-string s nil nil)))
                   (if (numberp v) (coerce v 'float) 1.0))
               (error () 1.0))))
    (when (and h (plusp (length h)))
      (let* ((tokens (uiop:split-string h :separator ","))
             (pairs
               (mapcar (lambda (tok)
                         (let* ((t1 (string-trim '(#\Space #\Tab) tok))
                                (pos (position #\; t1))
                                (enc (string-downcase
                                      (string-trim '(#\Space #\Tab)
                                                   (subseq t1 0 (or pos (length t1))))))
                                (q   (if pos
                                         (let* ((p2 (position #\= t1 :start (1+ pos))))
                                           (if p2
                                               (parse-q (string-trim '(#\Space #\Tab)
                                                                     (subseq t1 (1+ p2))))
                                               1.0))
                                         1.0)))
                           (cons enc q)))
                       tokens)))
	;; filtre q=0, trie desc
        (sort (remove-if (lambda (c) (<= (cdr c) 0.0)) pairs)
              #'> :key #'cdr)))))

(defun %client-wants-gzip-p (headers)
  (let ((ae (cdr (assoc "accept-encoding" headers :test #'string-equal))))
    (member "gzip" (mapcar #'car (%parse-accept-encoding ae))
            :test #'string=)))

(defun %choose-encoding (headers &key (supported '("gzip" "deflate")))
  "Renvoie \"gzip\" ou \"deflate\" selon Accept-Encoding et q-values, ou NIL si rien."
  (let* ((ae (cdr (assoc "accept-encoding" headers :test #'string-equal)))
         (prefs (%parse-accept-encoding ae)))
    (some (lambda (p)
            (let ((enc (car p)))
              (when (member enc supported :test #'string=)
                enc)))
          prefs)))

(defun %content-type (headers)
  (cdr (assoc "content-type" headers :test #'string=)))

(defun %type-prefixed-p (ct prefixes)
  "Vrai si le Content-Type CT commence par un des PREFIXES (strings)."
  (and ct prefixes
       (let ((ct* (string-downcase ct)))
         (some (lambda (pfx) (lumen.utils:str-prefix-p (string-downcase pfx) ct*))
               prefixes))))

;;; ---------------- Compressors ----------------
(defun %bytes (body)
  (etypecase body
    (string (trivial-utf-8:string-to-utf-8-bytes body))
    ((vector (unsigned-byte 8)) body)
    (null (trivial-utf-8:string-to-utf-8-bytes ""))))

(defun gzip-bytes (u8vec)
  "Retourne un nouveau vecteur d'octets gzip du contenu U8VEC."
  (let ((out (flexi-streams:make-in-memory-output-stream
              :element-type '(unsigned-byte 8))))
    (salza2:with-compressor (c 'salza2:gzip-compressor :stream out)
      (dotimes (i (length u8vec))
        (salza2:compress-octet c (aref u8vec i))))
    (flexi-streams:get-output-stream-sequence out)))

(defun deflate-bytes (u8vec)
  "Retourne un nouveau vecteur d'octets *deflate* (zlib wrapper, RFC1950) du contenu U8VEC."
  (let ((out (flexi-streams:make-in-memory-output-stream
              :element-type '(unsigned-byte 8))))
    ;; NB: 'deflate' côté HTTP signifie généralement zlib-wrapped (pas raw)
    (salza2:with-compressor (c 'salza2:zlib-compressor :stream out)
      (dotimes (i (length u8vec))
        (salza2:compress-octet c (aref u8vec i))))
    (flexi-streams:get-output-stream-sequence out)))

;;; ---------------- Middleware ----------------
;; ---------- Détection contenus déjà compressés / peu compressibles ----------
(defun %starts-with (vec &rest bytes)
  (when (and (vectorp vec)
             (subtypep (array-element-type vec) '(unsigned-byte 8))
             (<= (length bytes) (length vec)))
    (loop for i from 0 below (length bytes)
          always (= (aref vec i) (nth i bytes)))))

(defun %looks-gzip-p   (bytes) (%starts-with bytes #x1F #x8B))                ; GZIP
(defun %looks-zlib-p   (bytes) (or (%starts-with bytes #x78 #x01)             ; ZLIB/deflate
                                   (%starts-with bytes #x78 #x9C)
                                   (%starts-with bytes #x78 #xDA)))
(defun %looks-png-p    (bytes) (%starts-with bytes #x89 #x50 #x4E #x47 #x0D #x0A #x1A #x0A))
(defun %looks-jpeg-p   (bytes) (%starts-with bytes #xFF #xD8 #xFF))
(defun %looks-gif-p    (bytes)
  (and (>= (length bytes) 6)
       (let ((s (map 'string #'code-char (subseq bytes 0 6))))
         (or (string= s "GIF87a") (string= s "GIF89a")))))
(defun %looks-webp-p   (bytes)
  (and (>= (length bytes) 12)
       (string= (map 'string #'code-char (subseq bytes 0 4))  "RIFF")
       (string= (map 'string #'code-char (subseq bytes 8 12)) "WEBP")))
(defun %looks-zip-p    (bytes) (%starts-with bytes #x50 #x4B #x03 #x04))
(defun %looks-pdf-p    (bytes)
  (and (>= (length bytes) 4)
       (string= (map 'string #'code-char (subseq bytes 0 4)) "%PDF")))

(defun %incompressible-magic-p (bytes)
  "Renvoie T si BYTES semble déjà compressé (gzip/zlib) ou de type peu compressible (images, zip, pdf)."
  (or (%looks-gzip-p bytes)
      (%looks-zlib-p bytes)
      (%looks-png-p  bytes)
      (%looks-jpeg-p bytes)
      (%looks-gif-p  bytes)
      (%looks-webp-p bytes)
      (%looks-zip-p  bytes)
      (%looks-pdf-p  bytes)))

(defun compression
    (&key (threshold 1024)
       (add-vary t)
       (skip-types '("image/" "video/" "audio/"
                     "application/zip" "application/pdf"
                     "application/x-gzip" "application/x-7z-compressed"
                     "application/x-rar-compressed" "application/x-tar"))
       (only-types nil)
       (min-ratio 0.95)
       (skip-if nil))		     ; (lambda (req resp) ...) -> bool
  "Compresse la réponse (gzip/deflate) si pertinent.
Options:
  - THRESHOLD  : octets minimum du corps original
  - ADD-VARY   : ajoute Vary: Accept-Encoding
  - SKIP-TYPES : liste de préfixes MIME à ignorer
  - ONLY-TYPES : liste de préfixes MIME autorisés (si non-NIL, restreint la compression)
  - MIN-RATIO  : ne garde pas la compression si (compressed/original) > MIN-RATIO
  - SKIP-IF    : prédicat (req resp) → T pour désactiver la compression"
  (labels
      ((decorate-vary (hdrs)
         (if add-vary
             (let* ((existing (cdr (assoc "vary" hdrs :test #'string=)))
                    (new (if existing
                             (if (search "accept-encoding" (string-downcase existing))
                                 existing
                                 (format nil "~A, Accept-Encoding" existing))
                             "Accept-Encoding")))
               (lumen.utils:ensure-header hdrs "vary" new))
             hdrs)))
    (lambda (next)
      (lambda (req)
        (let* ((resp    (funcall next req))
               (status  (lumen.core.http:resp-status resp))
               (req-h   (lumen.core.http:req-headers req))
               (resp-h  (lumen.core.http:resp-headers resp))
               (ct      (%content-type resp-h)))

          (cond
            ;; Jamais compresser ces statuts / si déjà encodé
            ((or (member status '(204 304))
                 (assoc "content-encoding" resp-h :test #'string=))
             (setf (lumen.core.http:resp-headers resp)
                   (decorate-vary (lumen.core.http:resp-headers resp)))
             resp)

            ;; Respecter un éventuel prédicat d’exclusion
            ((and skip-if (funcall skip-if req resp))
             (setf (lumen.core.http:resp-headers resp)
                   (decorate-vary (lumen.core.http:resp-headers resp)))
             resp)

            ;; Respecter ONLY-TYPES (si fourni)
            ((and only-types (not (%type-prefixed-p ct only-types)))
             (setf (lumen.core.http:resp-headers resp)
                   (decorate-vary (lumen.core.http:resp-headers resp)))
             resp)

            ;; Ne pas compresser si CT est dans SKIP-TYPES
            ((%type-prefixed-p ct skip-types)
             (setf (lumen.core.http:resp-headers resp)
                   (decorate-vary (lumen.core.http:resp-headers resp)))
             resp)

            ;; Négociation de contenu
            (t
             (let ((chosen (%choose-encoding req-h :supported '("gzip" "deflate"))))
               (if (null chosen)
                   (progn
                     (setf (lumen.core.http:resp-headers resp)
                           (decorate-vary (lumen.core.http:resp-headers resp)))
                     resp)
                   (let* ((orig (lumen.core.http:resp-body resp))
                          (bytes (etypecase orig
                                   (string (trivial-utf-8:string-to-utf-8-bytes orig))
                                   ((vector (unsigned-byte 8)) orig)
                                   (null (trivial-utf-8:string-to-utf-8-bytes ""))))
                          (len (length bytes)))

                     (cond
                       ((< len threshold)
                        (setf (lumen.core.http:resp-headers resp)
                              (decorate-vary (lumen.core.http:resp-headers resp)))
                        resp)

                       ((%incompressible-magic-p bytes)
                        (setf (lumen.core.http:resp-headers resp)
                              (decorate-vary (lumen.core.http:resp-headers resp)))
                        resp)

                       (t
                        (let* ((encoded (ecase (intern (string-upcase chosen) :keyword)
                                          (:GZIP    (gzip-bytes bytes))
                                          (:DEFLATE (deflate-bytes bytes))))
                               (ratio (if (plusp len)
                                          (/ (float (length encoded)) (float len))
                                          1.0)))
                          (if (> ratio min-ratio)
                              ;; Gain insuffisant
                              (progn
                                (setf (lumen.core.http:resp-headers resp)
                                      (decorate-vary (lumen.core.http:resp-headers resp)))
                                resp)
                              ;; Conserver compression
                              (progn
                                (setf (lumen.core.http:resp-headers resp)
                                      (-> (lumen.core.http:resp-headers resp)
                                          (lumen.utils:ensure-header "content-encoding" chosen)
                                          (decorate-vary)))
                                (setf (lumen.core.http:resp-body resp) encoded)
                                resp)))))))))))))))

;;; If-Modified-Since → 304 ----------------------------------------
(defun last-modified-conditional (&key (only-status '(200)))
  "Si la réponse porte Last-Modified, compare If-Modified-Since et renvoie 304 si applicable.
   Respecte la règle de priorité : si If-None-Match est présent, on laisse l'autre middleware (ETag) décider."
  (lambda (next)
    (lambda (req)
      (let* ((resp   (funcall next req))
             (status (lumen.core.http:resp-status resp)))
        (labels ((->304 (resp lm)
                   (make-instance 'lumen.core.http:response
                                  :status 304
                                  :headers (lumen.utils:ensure-header
                                            (copy-list (lumen.core.http:resp-headers resp))
                                            "last-modified" lm)
                                  :body "")))
          (if (and (member status only-status)
                   (method-get-or-head-p req))
              (let* ((req-h  (lumen.core.http:req-headers req))
                     (resp-h (lumen.core.http:resp-headers resp))
                     (ims    (cdr (assoc "if-modified-since" req-h :test #'string-equal)))
                     (inm    (cdr (assoc "if-none-match" req-h :test #'string-equal))) ; priorité ETag
                     (lm     (cdr (assoc "last-modified" resp-h :test #'string=))))
                (if (and lm ims (null inm))
                    (let* ((ims-ut (lumen.utils:parse-http-date ims))
                           (lm-ut  (lumen.utils:parse-http-date lm)))
                      (if (and ims-ut lm-ut (>= ims-ut lm-ut))
                          (->304 resp lm)
                          resp))
                    resp))
              resp))))))

;;; Session
(defun session (&key secret (ttl lumen.core.session:*session-ttl*)
                     (cookie-name lumen.core.session:*session-cookie*)
                     (http-only t) (secure lumen.core.session:*secure-cookie*)
                     (path "/"))
  "Sessions mémoire via cookie signé. SECRET requis."
  (assert (and secret (plusp (length secret))) () "session: :secret requis.")
  (lambda (next)
    (lambda (req)
      ;; lecture cookie
      (let* ((raw (or (cookie req cookie-name) ""))
             (sid (and (> (length raw) 0)
                       (lumen.core.session:verify-signed-sid raw secret)))
             (data (and sid (lumen.core.session:store-get sid))))
        ;; si pas de session → créer
        (unless sid
          (setf sid (lumen.core.session:make-session-id))
          (setf data '()))
        ;; mettre en ctx
        (lumen.core.http:ctx-set! req :session-id sid)
        (lumen.core.http:ctx-set! req :session    data)
        ;; suite
        (let* ((resp (funcall next req))
               (sid* (lumen.core.session:session-id req))
               (dat* (lumen.core.session:session-data req)))
          ;; persister si non-vide (ou toujours, à toi de voir)
          (lumen.core.session:store-put! sid* dat* ttl)
          ;; cookie signé
          (let* ((signed (lumen.core.session:sign-sid sid* secret)))
            (set-cookie! resp cookie-name signed
                         :path path :http-only http-only :secure secure
                         :max-age ttl))
          resp)))))

;;; CSRF
#|
- On génère un token aléatoire, stocké en session (:csrf) et envoyé aussi au client (cookie csrf_token).
- Pour les méthodes mutables (POST, PUT, PATCH, DELETE), on vérifie que la requête fournit le même token via header X-CSRF-Token ou champ de formulaire (ctx :form → key "csrf_token").
- Si absent/incorrect → 403.
|#
(defun %random-token ()
  (let* ((b (lumen.core.session:rand-bytes 32)))
    (cl-base64:usb8-array-to-base64-string b :uri t))) ; base64url

(defun csrf (&key (cookie-name "csrf_token")
                  (header-name "x-csrf-token")
                  (methods '("POST" "PUT" "PATCH" "DELETE"))
                  (path "/")
                  (except-prefixes '())   ; ex: '("/login" "/api/webhook")
                  (except-exact '())      ; ex: '("/login" "/signup")
                  (skip-if nil))          ; (lambda (req) -> boolean)
  (labels ((path-exempt-p (req)
             (let ((p (lumen.core.http:req-path req)))
               (or
                ;; skip par prédicat custom
                (and skip-if (funcall skip-if req))
                ;; skip par match exact
                (member p except-exact :test #'string=)
                ;; skip par préfixe
                (some (lambda (pre)
                        (and (<= (length pre) (length p))
                             (string= pre p :end2 (length pre))))
                      except-prefixes)))))
    (lambda (next)
      (lambda (req)
        ;; (1) session requise (middleware :session doit être avant)
        (let* ((method (lumen.core.http:req-method req))
               (mut?   (member method methods :test #'string=))
               ;; token en session (créé si absent)
               (tok (or (lumen.core.session:session-get req :csrf)
                        (let ((tkn (%random-token)))
                          (lumen.core.session:session-set! req :csrf tkn) tkn))))
          (labels ((emit-cookie (resp)
                     (set-cookie! resp cookie-name tok :path path :http-only nil)))
            (cond
              ;; Cas exempté → pas de vérif, mais on émet/rafraîchit le cookie
              ((or (not mut?) (path-exempt-p req))
               (let ((resp (funcall next req)))
                 (emit-cookie resp)
                 resp))

              ;; Méthode mutable + pas exempté → vérification stricte
              (t
               (let* ((hdr (lumen.core.http:req-headers req))
                      (hdr-raw (cdr (assoc header-name hdr :test #'string-equal)))
                      ;; tolérance: si on reçoit littéralement "csrf_token", lire la valeur dans les cookies
                      (hdr-tok (if (and hdr-raw (string= hdr-raw cookie-name))
                                   (cdr (assoc cookie-name (lumen.core.http:req-cookies req) :test #'string=))
                                   hdr-raw))
                      (form (lumen.core.http:ctx-get req :form))
                      (field-tok (cdr (assoc "csrf_token" form :test #'string=)))
                      (ok (or (and hdr-tok (string= hdr-tok tok))
                              (and field-tok (string= field-tok tok)))))
                 (if ok
                     (let ((resp (funcall next req)))
                       (emit-cookie resp)
                       resp)
                     (let ((resp (lumen.core.http:respond-json
                                  '((:error . ((:type . "csrf")
                                               (:message . "invalid or missing CSRF token"))))
                                  :status 403)))
                       (emit-cookie resp) ; pour que le client récupère le bon token
                       resp)))))))))))

(defun %bearer-token-from (headers)
  "Extrait proprement le Bearer token (case-insensitive, espaces tolérés)."
  (let* ((raw (cdr (assoc "authorization" headers :test #'string-equal))))
    (when raw
      (let* ((s (string-trim " " raw)))
        (labels ((starts-with-ci (needle hay)
                   (and (>= (length hay) (length needle))
                        (string-equal needle hay :end2 (length needle)))))
          (cond
            ((starts-with-ci "Bearer " s) (subseq s 7))
            ((starts-with-ci "bearer " s) (subseq s 7))
            (t nil)))))))

(defun %alist-get-ci (alist key)
  "getf/assoc tolérant clé string/keyword (case-insensitive)."
  (or (cdr (assoc key alist :test #'equal))
      (and (stringp key)
           (or (cdr (assoc (intern (string-upcase key) :keyword) alist :test #'eq))
               (cdr (assoc (string-downcase key) alist :test #'string=))
               (cdr (assoc (string-upcase key) alist :test #'string=))))
      (and (keywordp key)
           (or (cdr (assoc (string-downcase (symbol-name key)) alist :test #'string=))
               (cdr (assoc (string-upcase   (symbol-name key)) alist :test #'string=))))))

(defun %cookie (req name)
  (cdr (assoc name (lumen.core.http:req-cookies req) :test #'string=)))

(defun %query-token-from (req &key (keys '("access" "token")))
  (let ((qp (lumen.core.http:req-query req)))
    (loop for k in keys
          for v = (or (%alist-get-ci qp k)
                      (%alist-get-ci qp (intern (string-upcase k) :keyword)))
          when (and v (stringp v) (> (length v) 0))
          do (return v))))

(defun auth-jwt (&key (secret nil secret-supplied-p)
                   (required-p nil)
                   (roles-allow nil)
                   (scopes-allow nil)
                   (scopes-mode :any)
                   (leeway-sec 60)
                   (allow-query-token-p t)
                   (qs-keys '("access" "token"))
                   ;; ---- Nouveaux switches ----
                   (admin-roles '("admin"))
                   (admin-bypass-roles-p t)
                   (admin-bypass-scopes-p t))
  (print "******* IN AUTH JWT")
  (let ((secret (if secret-supplied-p secret lumen.core.jwt:*jwt-secret*)))
    (lambda (next)
      (lambda (req)
        (labels
            ((fail (status msg)
               (lumen.core.http:respond-json
                `((:error . ((:type . "auth") (:message . ,msg))))
                :status status))
             (normalize-scopes (v)
               (handler-case
                   (let* ((lst (cond
                                 ((null v) '())
                                 ((stringp v)
                                  (let* ((s (string-trim " " v)))
                                    (cond
                                      ((and (> (length s) 1)
                                            (char= (char s 0) #\[)
                                            (char= (char s (1- (length s))) #\]))
                                       (let* ((inner (subseq s 1 (1- (length s))))
                                              (parts (cl-ppcre:split "\\s*,\\s*" inner)))
                                         (mapcar (lambda (x) (string-trim " \"" x)) parts)))
                                      ((search "," s)
                                       (cl-ppcre:split "\\s*,\\s*" s))
                                      (t (list s)))))
                                 ((listp v) v)
                                 ((vectorp v)
                                  (if (every #'characterp v)
                                      (list (coerce v 'string))
                                      (loop for x across v append (if (stringp x) (list x) (list (princ-to-string x))))))
                                 (t (list (princ-to-string v))))))
                     (remove-duplicates
                      (remove-if (lambda (s) (or (null s) (string= s "")))
                                 (mapcar (lambda (s) (substitute #\: #\. (string s))) lst))
                      :test #'string=))
                 (error () '())))
             (has-scopes? (needed have)
               (cond
                 ((null needed) t)
                 ((eq scopes-mode :all)
                  (every (lambda (x) (member x have :test #'string=)) needed))
                 (t
                  (some  (lambda (x) (member x have :test #'string=)) needed)))))
          (let* ((hdrs (lumen.core.http:req-headers req))
                 (tok  (or (%bearer-token-from hdrs)
                           (and allow-query-token-p (%query-token-from req :keys qs-keys))
                           (%cookie req "access_token"))))
            ;; Décodage du JWT
            (when tok
              (multiple-value-bind (payload ok)
                  (ignore-errors
                   (lumen.core.jwt:jwt-decode tok :secret secret :verify t
						  :leeway leeway-sec :debug t))
		;;(format t "~&TOKEN: ~A~%" tok)
		;;(format t "~&PAYLOAD: ~A~%" payload)
		;;(format t "~&OK: ~A~%" ok)
                (when ok
                  (lumen.core.http:ctx-set! req :jwt payload)
                  (let* ((tenant (or (%alist-get-ci payload :tenant)
                                     (%alist-get-ci payload "tenant")))
                         (tenant-id (or (%alist-get-ci payload :tenant-id)
                                        (%alist-get-ci payload "tenant_id")
                                        tenant)))
                    (when tenant-id (lumen.core.http:ctx-set! req :tenant-id tenant-id))
                    (when tenant    (lumen.core.http:ctx-set! req :tenant tenant))))))
            ;; Vérifs d’accès
            (let* ((jwt   (lumen.core.http:ctx-get req :jwt))
                   (role  (and jwt (or (%alist-get-ci jwt :role)
                                       (%alist-get-ci jwt "role"))))
                   (sc    (normalize-scopes (and jwt (%alist-get-ci jwt :scopes))))
                   (is-admin (and role (member role admin-roles :test #'string=)))
                   (roles-ok
                     (or (null roles-allow)
                         (member role roles-allow :test #'string=)
                         (and is-admin admin-bypass-roles-p)))
                   (scopes-ok
                     (or (null scopes-allow)
                         (has-scopes? scopes-allow sc)
                         (and is-admin admin-bypass-scopes-p))))
	      ;;(format t "~&JWT: ~A~%" jwt)
	      ;;(format t "~&IS ADMIN: ~A~%" is-admin)
	      ;;(format t "~&IS REQUIRED ? ~A~%" required-p)
              (cond
                ((and required-p (null jwt))
                 (fail 401 "missing or invalid token"))
                ((not roles-ok)
                 (fail 403 "forbidden"))
                ((not scopes-ok)
                 (fail 403 "insufficient_scope"))
                (t
                 ;; Audit : note qu’un bypass admin a été utilisé (si c’est le cas)
                 (when (and is-admin (or (and roles-allow admin-bypass-roles-p)
                                         (and scopes-allow admin-bypass-scopes-p)))
                   (lumen.core.http:ctx-set! req :auth-bypass "admin"))
                 (funcall next req))))))))))

(defun auth-required (&key (admin-bypass-roles-p t)
                           (admin-bypass-scopes-p t)
                           (allow-query-token-p t)
                           (qs-keys '("access" "token")))
  "JWT requis, sans contrainte de rôle/scope. Bypass admin actif par défaut."
  (auth-jwt :required-p t
            :admin-bypass-roles-p admin-bypass-roles-p
            :admin-bypass-scopes-p admin-bypass-scopes-p
            :allow-query-token-p   allow-query-token-p
            :qs-keys               qs-keys))

(defun roles-allowed (roles &key (admin-bypass-roles-p t)
                                 (admin-bypass-scopes-p t)
                                 (allow-query-token-p t)
                                 (qs-keys '("access" "token")))
  "JWT requis + appartenance à l’un des ROLES (admin bypass par défaut)."
  (auth-jwt :required-p t
            :roles-allow roles
            :admin-bypass-roles-p admin-bypass-roles-p
            :admin-bypass-scopes-p admin-bypass-scopes-p
            :allow-query-token-p   allow-query-token-p
            :qs-keys               qs-keys))

;;; HTTPS
(defun %bool (x) (and x t))

(defun %request-secure-p (req)
  "Vrai si la requête est déjà HTTPS (socket TLS ou proxy qui l’indique)."
  (or (lumen.core.http:ctx-get req :secure)
      (let* ((h (lumen.core.http:req-headers req))
             (xfp (cdr (assoc "x-forwarded-proto" h :test #'string-equal)))
             (xfs (cdr (assoc "x-forwarded-ssl"   h :test #'string-equal))))
        (or (and xfp (string-equal xfp "https"))
            (and xfs (string-equal xfs "on"))))))

(defun %path-has-prefix-p (path prefix)
  (and (<= (length prefix) (length path))
       (string= prefix path :end2 (length prefix))))

(defun %build-query (req)
  "Reconstruit la query string si possible."
  (let ((q (lumen.core.http:req-query req)))
    (cond
      ((null q) "")
      ((stringp q) (if (plusp (length q)) (concatenate 'string "?" q) ""))
      ((listp q)
       (let ((parts
               (mapcar (lambda (cell)
                         (let ((k (car cell)) (v (cdr cell)))
                           ;; k/v peuvent être keywords ou strings
                           (format nil "~A=~A"
                                   (etypecase k
                                     (string k)
                                     (symbol (string-downcase (symbol-name k))))
                                   (etypecase v
                                     (string v)
                                     (symbol (string-downcase (symbol-name v)))
                                     (t (princ-to-string v))))))
                       q)))
         (if parts
             (concatenate 'string "?" (map 'string #'identity
                                           (with-output-to-string (s)
                                             (format s "~{~A~^&~}" parts))))
             "")))
      (t ""))))

(defun host-without-port (host)
  (let ((pos (position #\: host)))
    (if pos (subseq host 0 pos) host)))

(defun %https-location (req &key ssl-port)
  "Construit l’URL de redirection https (conserve host, path, query).
   Ajoute :ssl-port si non standard (≠443)."
  (let* ((h    (lumen.core.http:req-headers req))
         (host (or (cdr (assoc "x-forwarded-host" h :test #'string-equal))
                   (cdr (assoc "host"             h :test #'string-equal))
                   "localhost"))
	 (host0 (host-without-port host))
         (path (lumen.core.http:req-path req))
         (qs   (%build-query req))
         (port-suffix (if (and ssl-port (not (= ssl-port 443)))
                          (format nil ":~D" ssl-port) ""))
         (scheme "https://"))
    (format nil "~A~A~A~A~A" scheme host0 port-suffix path qs)))

(defun https-redirect (&key
                         (prefixes '("/"))            ; préfixes à forcer en HTTPS
                         (except-prefixes nil)         ; préfixes EXCLUS (pas de redirect/HSTS)
                         (ssl-port 443)                ; port public HTTPS
                         (hsts nil)                    ; t ⇒ ajoute Strict-Transport-Security (sur HTTPS)
                         (hsts-max-age 15552000)       ; 180 jours
                         (hsts-include-subdomains t)
                         (hsts-preload nil))
  "Redirige HTTP → HTTPS (301) pour les chemins dont le préfixe matche.
Options:
- PREFIXES         : liste de préfixes (strings) à forcer en https (\"/\" pour tout).
- EXCEPT-PREFIXES  : liste de préfixes à EXCLURE (pas de redirect, pas de HSTS).
- SSL-PORT         : port https externe (443 par défaut).
- HSTS             : si T, ajoute Strict-Transport-Security *sur les réponses HTTPS*
                     (sauf si le chemin est exclu par EXCEPT-PREFIXES)."
  (labels ((matches-any-p (path lst)
             (and lst (some (lambda (pfx) (%path-has-prefix-p path pfx)) lst))))
    (lambda (next)
      (lambda (req)
        (let* ((path     (lumen.core.http:req-path req))
               (in-scope (matches-any-p path prefixes))
               (excluded (matches-any-p path except-prefixes))
               (secure?  (%request-secure-p req)))
          (cond
            ;; Déjà en HTTPS : on passe au suivant, et on pose HSTS si demandé (et non exclu)
            (secure?
             (let ((resp (funcall next req)))
               (when (and hsts (not excluded))
                 (let* ((hdrs (lumen.core.http:resp-headers resp))
                        (val  (with-output-to-string (s)
                                (format s "max-age=~D" hsts-max-age)
                                (when hsts-include-subdomains (format s "; includeSubDomains"))
                                (when hsts-preload           (format s "; preload")))))
                   (setf (lumen.core.http:resp-headers resp)
                         (lumen.utils:ensure-header hdrs "strict-transport-security" val))))
               resp))

            ;; HTTP + ciblé + non exclu → 301 vers HTTPS
            ((and in-scope (not excluded) (not secure?))
             (let* ((loc (%https-location req :ssl-port ssl-port))
                    (headers (list (cons "location" loc)
                                   (cons "cache-control" "no-store"))))
               (make-instance 'lumen.core.http:response
                              :status 301 :headers headers :body "")))

            ;; HTTP hors-scope ou exclu → passer au suivant
            (t
             (funcall next req))))))))

;;Le rate limiting contrôle combien de fois un client peut appeler une route
;; dans une période donnée. C’est une protection anti-abus / DDoS / brute-force.
(defun rate-limit (&key (capacity 10) (refill-per-sec 1) (route-key "default"))
  (lambda (next)
    (lambda (req)
      (if (lumen.core.ratelimit:allow? req :capacity capacity
					   :refill-per-sec refill-per-sec
					   :route-key route-key)
          (funcall next req)
          (lumen.core.http:respond-json
           '((:error . ((:type . "rate_limit") (:message . "Too Many Requests"))))
           :status 429)))))

;;; Request Timeout (504 si le handler dépasse N ms)
(defun request-timeout (&key (ms 5000))
  (lambda (next)
    (lambda (req)
      (let* ((lock (bordeaux-threads:make-lock "rt-lock"))
             (cv   (bordeaux-threads:make-condition-variable))
             (resp nil))
	(bordeaux-threads:make-thread
	 (lambda ()
	   (let ((r (funcall next req)))
             (bordeaux-threads:with-lock-held (lock)
               (setf resp r)
               (bordeaux-threads:condition-notify cv)))))
	(bordeaux-threads:with-lock-held (lock)
	  (unless (bordeaux-threads:condition-wait cv lock :timeout (/ ms 1000.0))
            ;; timeout → 504
            (return-from request-timeout
              (lumen.core.http:respond-json
               '((:error . ((:type . "timeout") (:message . "gateway timeout"))))
               :status 504))))
	resp))))

;;; max-body-size (413 si Content-Length trop grand)
(defun max-body-size (&key (bytes (* 2 1024 1024))) ; 2 MiB
  (lambda (next)
    (lambda (req)
      (let* ((clen (cdr (assoc "content-length" (lumen.core.http:req-headers req)
                               :test #'string-equal)))
             (n (and clen (parse-integer clen :junk-allowed t))))
	(when (and n (> n bytes))
	  (return-from max-body-size
            (lumen.core.http:respond-json
             `((:error . ((:type . "payload_too_large")
			  (:message . ,(format nil "max ~D bytes" bytes)))))
             :status 413))))
      ;; pour les cas sans Content-Length, on laisse les parseurs respecter une limite via ctx si tu veux
      (lumen.core.http:ctx-set! req :max-body-size bytes)
      (funcall next req))))

;;; Access Log JSON
(defun %open-log-file (path)
  (open path :direction :output :if-exists :append :if-does-not-exist :create
             :external-format :utf-8))

(defun %rfc3339-now ()
  (local-time:format-timestring
   nil (local-time:now)
   :format '(:year "-" (:month 2) "-" (:day 2) "T" (:hour 2) ":" (:min 2) ":" (:sec 2) "Z")))

(defun %plist->alist (plist)
  (loop for (k v) on plist by #'cddr collect (cons k v)))

(defun %normalize-fields (fields req resp ms bytes)
  "Retourne une alist à partir de FIELDS (NIL | alist | plist | fn)."
  (cond
    ((null fields) nil)
    ((and (listp fields)
          (every #'consp fields)) fields)                  ; déjà alist
    ((and (listp fields) (evenp (length fields))) (%plist->alist fields)) ; plist
    ((functionp fields) (let ((res (funcall fields req resp ms bytes)))
                          (cond
                            ((null res) nil)
                            ((and (listp res) (every #'consp res)) res)
                            ((and (listp res) (evenp (length res))) (%plist->alist res))
                            (t nil))))
    (t nil)))

(defun %merge-alist (base extras &key (test #'equal))
  "Merge en donnant priorité à EXTRAS (override)."
  (let ((res (copy-list base)))
    (dolist (kv extras)
      (let* ((k (car kv))
             (cell (assoc k res :test test)))
        (if cell
            (setf (cdr cell) (cdr kv))
            (push kv res))))
    (nreverse res)))

(defun access-log-json (&key stream to-file fields)
  "Middleware: écrit une ligne JSON par requête.
:fields accepte NIL, alist, plist, ou (lambda (req resp ms bytes) -> alist/plist)."
  (let* ((out (or (and to-file (%open-log-file to-file))
                  (or stream *standard-output*)))
         (lock (bordeaux-threads:make-lock "access-log-json")))
    (lambda (next)
      (lambda (req)
        (labels ((h (name)
                   (cdr (assoc name (lumen.core.http:req-headers req) :test #'string-equal))))
          (let* ((t0  (get-internal-real-time))
                 (rid (or (lumen.core.http:ctx-get req :request-id) ""))
                 (ip  (or (let* ((xff (h "x-forwarded-for")))
                            (and xff (car (uiop:split-string xff :separator ","))))
                          (h "x-real-ip") ""))
                 (mth (or (lumen.core.http:req-method req) "GET"))
                 (host (or (h "host") ""))
                 (pth (or (lumen.core.http:req-path req) "/"))
                 (resp (funcall next req))
                 (ms  (/ (- (get-internal-real-time) t0)
                         (/ internal-time-units-per-second 1000.0)))
                 (body (lumen.core.http:resp-body resp))
                 (bytes (cond
                          ((stringp body) (length (trivial-utf-8:string-to-utf-8-bytes body)))
                          ((typep body '(simple-array (unsigned-byte 8) (*))) (length body))
                          (t nil))) ; streaming inconnu → omis
                 (base `((:ts . ,(%rfc3339-now))
                         (:request_id . ,rid)
                         (:ip . ,ip)
                         (:method . ,mth)
                         (:host . ,host)
                         (:path . ,pth)
                         (:status . ,(lumen.core.http:resp-status resp))
                         (:ms . ,(round ms))
                         ,@(when bytes `((:bytes . ,bytes)))))
                 (extra (%normalize-fields fields req resp ms bytes))
                 (obj   (%merge-alist base extra :test
                                      (lambda (a b)
                                        (string-equal (if (symbolp a) (symbol-name a) (format nil "~A" a))
                                                      (if (symbolp b) (symbol-name b) (format nil "~A" b)))))))
            (bordeaux-threads:with-lock-held (lock)
              (handler-case
                  (progn
                    (write-string (cl-json:encode-json-to-string obj) out)
                    (terpri out)
                    (finish-output out))
                (error (_)
                  (declare (ignore _))
                  (format out "~S~%" obj)
                  (finish-output out))))
            resp))))))

;;;--------------------------------------------------------------------------
;;; Le middleware request-context (léger, idempotent) qui remplit ctx avec :ip, :ua, :tenant-id (déjà posé par ton tenant-from-host) et garantit :request-id (+ renvoi dans le header X-Request-ID)
;;;--------------------------------------------------------------------------
;; ---- Helpers ---------------------------------------------------------------
(defun %header (req name)
  (cdr (assoc name (lumen.core.http:req-headers req) :test #'string-equal)))

(defun %peer-ip (req)
  "IP client en respectant les proxys (X-Forwarded-For / X-Real-IP),
   ou ctx[:remote-addr] posée par le serveur, sinon \"-\"."
  (or (let* ((xff (%header req "x-forwarded-for")))
        (when xff
          (string-trim " " (car (uiop:split-string xff :separator ",")))))
      (%header req "x-real-ip")
      (lumen.core.http:ctx-get req :remote-addr)
      "-"))

(defun %user-agent (req)
  (or (%header req "user-agent") ""))

(defun %aget-ci (alist &rest keys)
  (loop for k in keys
        for v = (or (cdr (assoc k alist :test #'string=))
                    (and (symbolp k) (cdr (assoc (intern (string-upcase (symbol-name k)) :keyword) alist))))
        when v do (return v)))

(defun %normalize-scopes-loose (v)
  "Accepte liste/vecteur/csv/[...] string → liste de scopes normalisés (courriers.read → courriers:read)."
  (handler-case
      (let* ((lst (cond
                    ((null v) '())
                    ((listp v) v)
                    ((vectorp v) (loop for x across v collect x))
                    ((stringp v)
                     (let ((s (string-trim " " v)))
                       (cond
                         ((and (> (length s) 1)
                               (char= (char s 0) #\[)
                               (char= (char s (1- (length s))) #\]))
                          (let* ((inner (subseq s 1 (1- (length s))))
                                 (parts (cl-ppcre:split "\\s*,\\s*" inner)))
                            (mapcar (lambda (x) (string-trim " \"" x)) parts)))
                         ((search "," s)
                          (cl-ppcre:split "\\s*,\\s*" s))
                         (t (list s)))))
                    (t (list (princ-to-string v))))))
        (remove-duplicates
         (remove-if (lambda (s) (or (null s) (string= s "")))
                    (mapcar (lambda (s) (substitute #\: #\. (string s))) lst))
         :test #'string=))
    (error () '())))

;; ---- Middleware request-context -------------------------------------------
(defun request-context (&key (generate-request-id-p t) (audit-config-key :audit/enabled))
  "Middleware: enrichit req.ctx avec actor-id, role, scopes, tenant-id/tenant-code (si présents),
   request-id, ip, ua et audit?."
  (lambda (next)
    (lambda (req)
      ;; request-id (si pas déjà posé par un autre mw)
      (print "IN REQUEST-CONTEXT")
      (when generate-request-id-p
        (unless (lumen.core.http:ctx-get req :request-id)
          (lumen.core.http:ctx-set! req :request-id (lumen.utils:gen-uuid-string))))
      ;; ip / ua
      (let* ((hdrs (lumen.core.http:req-headers req))
             (ip  (%peer-ip req))
         (ua  (%user-agent req)))
        (when ua (lumen.core.http:ctx-set! req :ua ua))
        (when ip (lumen.core.http:ctx-set! req :ip ip)))
      ;; JWT déjà décodé par auth-jwt → current-user-id/role/scopes
      (let* ((actor (lumen.core.http:current-user-id req))
             (role  (lumen.core.http:current-role req))
             (jwt   (lumen.core.http:ctx-get req :jwt))
             (sc    (%normalize-scopes-loose (%aget-ci jwt :scopes "scopes")))
             (tenant-id (or (lumen.core.http:ctx-get req :tenant-id)
                            (%aget-ci jwt :tenant-id "tenant_id" :tenant "tenant"))))
        (when actor (lumen.core.http:ctx-set! req :actor-id actor))
        (when role  (lumen.core.http:ctx-set! req :role role))
        (when sc    (lumen.core.http:ctx-set! req :scopes sc))
        (when tenant-id (lumen.core.http:ctx-set! req :tenant-id tenant-id)))
      ;; audit?
      (let ((audit? (lumen.core.config:cfg-get audit-config-key :default t)))
        (lumen.core.http:ctx-set! req :audit? audit?))
      (funcall next req))))
