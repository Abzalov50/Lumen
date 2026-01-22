(in-package :cl)
(defpackage :lumen.core.router
  (:use :cl :alexandria :cl-ppcre)
  (:import-from :lumen.core.http
   :request :response :req-method :req-path :req-headers :req-query :req-cookies
		:req-params :req-body-stream :req-ctx :resp-status :resp-headers
   :resp-body :respond-text :respond-json :respond-404 :respond-500
   :ctx-get :ctx-set!)
  (:import-from :lumen.core.http
   :request :response :req-method :req-path :req-headers :req-query :req-cookies
		:req-params :req-body-stream :req-ctx :resp-status :resp-headers
   :resp-body :respond-text :respond-json :respond-404 :respond-500)
  (:export :defroute :router-dispatch :param :with-params :clear-routes
	   :defprotected :defroles :with-guards :defguarded :def-api-route))

(in-package :lumen.core.router)

;;; ===========================================================================
;;; Modèle de route (ajout d'un filtre d'hôte optionnel)
;;; ===========================================================================

(defstruct route
  method                ; "GET" / "POST" ...
  pattern               ; scanner cl-ppcre pour le PATH
  param-names           ; noms des params extraits du PATH (déjà existant)
  handler               ; (req &rest extras) -> response
  host-scanner          ; scanner cl-ppcre pour l'HÔTE (NIL => match tout)
  host-param-names)     ; réservé si un jour on veut capturer des variables d'hôte

(defparameter *routes* (make-array 0 :adjustable t :fill-pointer 0))

(defun clear-routes ()
  "Réinitialise la table des routes."
  (setf *routes* (make-array 0 :adjustable t :fill-pointer 0)))

(defun add-route (r)
  (vector-push-extend r *routes*))

;;; ===========================================================================
;;; Normalisation & compilation des hôtes
;;; ===========================================================================

(defun normalize-host (req)
  "Récupère l'hôte 'effectif' d'une requête (sans port), en tenant compte de trust-proxy si présent."
  (let* ((h (or (ctx-get req :host)
                (cdr (assoc "host" (req-headers req) :test #'string-equal)))))
    (when h
      (let ((p (position #\: h)))
	(if p (subseq h 0 p) h))))) ; strip :port

(defun %wildcard-host->regex (s)
  "Transforme 'api.example.com' en regex exact ; '*.example.com' en sous-domaine quelconque."
  ;; on découpe à '.', '*' devient [^.]+, le reste est 'quoté'
  (let* ((parts (uiop:split-string s :separator "."))
         (rx (map 'string #'identity
                  (with-output-to-string (out)
                    (loop for i from 0 below (length parts) do
                      (when (> i 0) (write-string "\\." out))
                      (let ((p (elt parts i)))
                        (if (string= p "*")
                            (write-string "[^.]+"
                                          out)
                            (write-string (quote-meta-chars p) out))))))))
    (format nil "^(?:~A)$" rx)))

(defun %re-quote (s)
  "Échappe les méta-caractères regex dans S."
  (let ((buf (make-string-output-stream)))
    (loop for ch across s do
      (case ch
        ((#\^ #\$ #\. #\[ #\] #\{ #\} #\( #\) #\? #\* #\+ #\\ #\|)
         (write-char #\\ buf) (write-char ch buf))
        (t (write-char ch buf))))
    (get-output-stream-string buf)))

(defun %host->regex-body (host)
  "Retourne un motif SANS ancres pour un host ou wildcard.
   Exemples:
   - \"admin.localhost\"         => admin\.localhost
   - \"*.example.com\"           => (?:[^.]+\\.)example\\.com
   - \"**.example.com\" (option) => (?:[^.]+\\.)+example\\.com"
  (cond
    ;; wildcard multi-niveaux optionnel: **.example.com
    ((cl-ppcre:scan "^\\*\\*\\.(.+)$" host)
     (let ((rest (cl-ppcre:regex-replace "^\\*\\*\\." host "")))
       (format nil "(?:[^.]+\\.)+~A" (%re-quote rest))))
    ;; wildcard simple: *.example.com
    ((cl-ppcre:scan "^\\*\\.(.+)$" host)
     (let ((rest (cl-ppcre:regex-replace "^\\*\\." host "")))
       (format nil "(?:[^.]+\\.)~A" (%re-quote rest))))
    ;; pas de wildcard
    (t (%re-quote host))))

(defun compile-host-spec (spec)
  "SPEC :
   - string \"api.example.com\" (exact) ou \"*.example.com\"
   - liste '(\"a.example.com\" \"b.example.com\") → OR
   - NIL → aucun filtre (route valable pour tous les hôtes).
   Retourne un SCANNER cl-ppcre (ou NIL)."
  (cond
    ((null spec) nil)
    ((stringp spec)
     (let* ((body (%host->regex-body spec))
            (rx   (format nil "^(?:~A)$" body)))
       (cl-ppcre:create-scanner rx :case-insensitive-mode t)))
    ((and (listp spec) (every #'stringp spec))
     (let* ((bodies (mapcar #'%host->regex-body spec))
            (rx (format nil "^(?:~{~A~^|~})$" bodies)))
       (cl-ppcre:create-scanner rx :case-insensitive-mode t)))
    (t
     (error "Host spec invalide: ~S. Attendu string, liste de strings ou NIL." spec))))

;;; ===========================================================================
;;; Compilation du PATH
;;; ===========================================================================
#|
(defun compile-path (path)
  "Ex: \"/api/users/:id\" -> scanner + '(\"id\")"
  (let ((parts (remove-if #'(lambda (s) (zerop (length s)))
                          (cl-ppcre:split "/" path)))
        (rx "^")
        (params '()))
    (print path)
    (print parts)
    (dolist (p parts)
      (setf rx (concatenate 'string rx "/"))
      (print rx)
      (if (and (> (length p) 1) (char= (char p 0) #\:))
          (let* ((name (subseq p 1))
                 (name* (string-downcase name)))
            (push name* params)
            (setf rx (concatenate 'string rx "([^/]+)")))
          (setf rx (concatenate 'string rx (cl-ppcre:quote-meta-chars p)))))
    (print (concatenate 'string rx "$"))
    (values (concatenate 'string rx "$") (nreverse params))))
|#

(defun compile-path (path)
  "Ex: \"/api/users/:id\" -> (values \"^/api/users/([^/]+)$\" '(\"id\"))
   Gère correctement la racine \"/\"."
  (let* ((path (or path "/"))
         (parts (remove-if (lambda (s) (zerop (length s)))
                           (cl-ppcre:split "/" path)))
         (rx "^")
         (params '()))
    (cond
      ;; Cas racine strict: exactement "/"
      ((null parts)
       (setf rx "^/$"))
      (t
       (dolist (p parts)
         (setf rx (concatenate 'string rx "/"))
         (if (and (> (length p) 1) (char= (char p 0) #\:))
             (let* ((name (subseq p 1))
                    (name* (string-downcase name)))
               (push name* params)
               (setf rx (concatenate 'string rx "([^/]+)")))
             (setf rx (concatenate 'string rx (cl-ppcre:quote-meta-chars p)))))
       (setf rx (concatenate 'string rx "$"))))
    (values rx (nreverse params))))

;;; ===========================================================================
;;; defroute (ajout :host / :hosts via PATH-ARG plist)
;;; ===========================================================================

(defun %parse-route-args (path-arg)
  "Retourne (values host-spec real-path)."
  (cond
    ((stringp path-arg)
     (values nil path-arg))
    ((and (listp path-arg)
          (getf path-arg :path))
     (let ((host  (or (getf path-arg :host) nil))
           (hosts (getf path-arg :hosts))
           (p     (getf path-arg :path)))
       (values (or hosts host) p)))
    (t
     (error "defroute: PATH doit etre une string ou un plist (:host/:hosts :path). Recu: ~S" path-arg))))

(defmacro defroute (method path-arg arglist &body body)
  "Ajoute une route. Nouveautés :
   - PATH-ARG peut etre une string \"/users/:id\" (comportement inchangé),
     ou un plist  (:host \"api.example.com\" :path \"/users/:id\")
     ou (:hosts (\"a.example.com\" \"b.example.com\") :path \"/...\").
   - ARGLIST : comme avant, () ou (REQ [&rest extras])."
  (let* ((m (string-upcase (string method)))
         (rx (gensym "RX"))
         (params (gensym "PARAMS"))
         (host-spec (gensym "HOSTSPEC"))
         (host-sc (gensym "HOSTSC"))
         (handler (gensym "HANDLER"))
         (default-req (or (find-symbol "REQ" *package*)
                          (intern "REQ" *package*)))
         (req-sym (or (first arglist) default-req))
         (extra-args (rest arglist)))
    `(multiple-value-bind (,host-spec %path) (%parse-route-args ,path-arg)
       (let* ((,rx     (nth-value 0 (compile-path %path)))
              (,params (nth-value 1 (compile-path %path)))
              (,host-sc (compile-host-spec ,host-spec)))
         (flet ((,handler (,req-sym ,@extra-args)
                  (declare (ignorable ,req-sym ,@extra-args))
                  ,@body))
           (add-route
            (make-route :method ,m
                        :pattern (cl-ppcre:create-scanner ,rx)
                        :param-names ,params
                        :handler #',handler
                        :host-scanner ,host-sc)))))))

;;; ===========================================================================
;;; Dispatch (prise en compte de l'hôte)
;;; ===========================================================================

(defun %match-route-host-p (route host)
  "Vrai si ROUTE n'a pas de filtre d'hôte, ou si HOST matche host-scanner."
  (let ((sc (route-host-scanner route)))
    (or (null sc)
        (and host (cl-ppcre:scan sc host)))))

(defun %match-route-path (route path)
  "Retourne (values ok? captures) ; ok? vrai si path matche la route."
  (multiple-value-bind (okp caps)
      (scan-to-strings (route-pattern route) path)
    (values okp caps)))

(defun %bind-path-params (req route captures)
  "Place les params extraits du PATH dans req-ctx :params, en suivant l'ordre de param-names."
  (let* ((names (route-param-names route))
         (alist '()))
    (dotimes (i (length names))
      (let ((name (nth i names))
            (val  (aref captures (1+ i))))   ; 0 = total match ; groupes à partir de 1
        (push (cons name val) alist)))
    ;; on fusionne (prepend) avec params existants s'il y en a
    (let* ((ctx (req-ctx req))
           (old (cdr (assoc :params ctx))))
      (lumen.core.http:ctx-set! req :params (nconc (nreverse alist) old)))))

#|
(defun router-dispatch (next)
  "Middleware de dispatch des routes, en tenant compte de l'hôte."
  (lambda (req)
    (let* ((method (slot-value req 'lumen.core.http::method))
           (path   (slot-value req 'lumen.core.http::path))
           ;; HEAD tombe sur GET pour le matching (write-response gère l’absence de corps)
           (match-method (if (string= method "HEAD") "GET" method))
           (host   (normalize-host req))
           (matched nil))
      (cond
        ((string= method "OPTIONS")
         (respond-options path))

        (t
         ;; on cherche une route de même pattern, bonne méthode ET (si défini) bon hôte
         (loop for r across *routes* do
               (when (and (string= match-method (route-method r))
                          (%match-route-host-p r host)
                          (cl-ppcre:scan (route-pattern r) path))
                 (setf matched r)
                 (return)))
         (if matched
             (progn
               (setf (slot-value req 'lumen.core.http::params)
                     (extract-path-params (route-pattern matched)
                                          (route-param-names matched)
                                          path))
               (funcall (route-handler matched) req))
             ;; si aucun handler, mais pattern existant (pour CET hôte) -> 405 ; sinon -> next
             (if (allowed-methods-for path)
                 (respond-405 path)
                 (funcall next req))))))))
|#

(defun router-dispatch (next)
  "Middleware de dispatch des routes, en tenant compte de l'hôte.
   Capture les interruptions (halt)."
  (lambda (req)
    ;; AJOUT : On enveloppe tout le traitement dans un handler-case
    (handler-case
        (let* ((method (slot-value req 'lumen.core.http::method))
               (path   (slot-value req 'lumen.core.http::path))
               ;; HEAD tombe sur GET pour le matching (write-response gère l’absence de corps)
               (match-method (if (string= method "HEAD") "GET" method))
               (host   (normalize-host req))
               (matched nil))
          
          (cond
            ((string= method "OPTIONS")
             (respond-options path))

            (t
             ;; on cherche une route de même pattern, bonne méthode ET (si défini) bon hôte
             (loop for r across *routes* do
                   (when (and (string= match-method (route-method r))
                              (%match-route-host-p r host)
                              (cl-ppcre:scan (route-pattern r) path))
                     (setf matched r)
                     (return)))
             
             (if matched
                 (progn
                   (setf (slot-value req 'lumen.core.http::params)
                         (extract-path-params (route-pattern matched)
                                              (route-param-names matched)
                                              path))
                   ;; C'est ici que le code utilisateur est exécuté
                   ;; Si (halt ...) est appelé dedans, le handler-case ci-dessous l'attrapera
                   (funcall (route-handler matched) req))
                 
                 ;; si aucun handler, mais pattern existant (pour CET hôte) -> 405 ; sinon -> next
                 (if (allowed-methods-for path)
                     (respond-405 path)
                     (funcall next req))))))

      ;; --- INTERCEPTION DU HALT ---
      (lumen.core.http:http-halt (c)
        ;; On récupère l'objet réponse stocké dans la condition et on le renvoie
        (lumen.core.http:halt-response c)))))

(defun extract-path-params (scanner names path)
  (multiple-value-bind (match regs) (cl-ppcre:scan-to-strings scanner path)
    (when match
      (loop for nm in names
            for i from 0
            for v = (aref regs i)
            when v collect (cons (string-downcase (string nm)) v)))))

(defun param (req name)
  (cdr (assoc (string-downcase name) (req-params req) :test #'string=)))

#|
(defmacro with-params ((&rest names) &body body)
  "Lie chaque NAME à (param req \"name\") dans le *contexte où `req` est lexicalement visible*."
  `(let ,(mapcar (lambda (n)
                   `(,n (param req ,(string-downcase (string n)))))
                 names)
     ,@body))
|#

(defmacro with-params ((req-sym &rest names) &body body)
  `(let ,(mapcar (lambda (n)
                   `(,n (param ,req-sym ,(string-downcase (string n)))))
                 names)
     ,@body))

(defun match-route (method path)
  (loop for r across *routes* do
    (when (and (string= method (route-method r))
	       (ppcre:scan (route-pattern r) path))
      (return r))))

;; Helper: méthodes autorisées pour un PATH (indépendamment de la méthode reçue)
(defun allowed-methods-for (path)
  (let ((methods '()))
    (loop for r across *routes* do
      (when (cl-ppcre:scan (route-pattern r) path)
        (pushnew (route-method r) methods :test #'string=)))
    ;; HEAD est toujours autorisé si GET l’est (RFC)
    (when (member "GET" methods :test #'string=)
      (pushnew "HEAD" methods :test #'string=))
    ;; n’ajouter OPTIONS que s’il existe AU MOINS une méthode trouvée
    (when methods
      (pushnew "OPTIONS" methods :test #'string=))
    (sort (copy-list methods) #'string<)))

(defun make-allow-header (methods)
  (cons "allow" (format nil "~{~A~^, ~}" methods)))

(defun respond-405 (path)
  (let* ((methods (allowed-methods-for path)))
    (lumen.core.http:respond-json
     `((:error . ((:type . "method_not_allowed")
                  (:allowed . ,methods))))
     :status 405
     :headers (list (make-allow-header methods)))))

(defun respond-options (path)
  (let* ((methods (allowed-methods-for path)))
    (make-instance 'lumen.core.http:response
                   :status 204
                   :headers (list (make-allow-header methods))
                   :body "")))


#|
(defmacro defroute (method path arglist &body body)
  "Déclare une route. ARGLIST peut être () ou (REQ [&rest autres]).
   - Si vide, on lie le paramètre de handler au symbole interne LUMEN.CORE.ROUTER::REQ,
     ce qui garde la compatibilité avec `with-params` qui référence ce symbole."
  (let* ((m (string-upcase (string method)))
         (rx (gensym "RX"))
         (params (gensym "PARAMS"))
         (handler (gensym "HANDLER"))
         ;; Si l'utilisateur n'a pas fourni de nom, on utilise le symbole interne
         ;; que `with-params` attend (même package que cette macro).
         (default-req (or (find-symbol "REQ" *package*)
                          (intern "REQ" *package*)))
         (req-sym (or (first arglist) default-req))
         (extra-args (rest arglist)))
    `(let* ((,rx     (nth-value 0 (compile-path ,path)))
            (,params (nth-value 1 (compile-path ,path))))
       (flet ((,handler (,req-sym ,@extra-args)
                (declare (ignorable ,req-sym ,@extra-args))
                ,@body))
         (add-route
          (make-route :method ,m
                      :pattern (cl-ppcre:create-scanner ,rx)
                      :param-names ,params
:handler #',handler))))))

(defun router-dispatch (next)
  (lambda (req)
    (let* ((method (slot-value req 'lumen.core.http::method))
           (path   (slot-value req 'lumen.core.http::path))
           ;; HEAD tombe sur GET pour le matching (write-response gère l’absence de corps)
           (match-method (if (string= method "HEAD") "GET" method))
           (matched nil))
      (cond
        ((string= method "OPTIONS")
         (respond-options path))

        (t
         ;; on cherche une route de même pattern et bonne méthode
         (loop for r across *routes* do
           (when (and (string= match-method (route-method r))
                      (cl-ppcre:scan (route-pattern r) path))
             (setf matched r)
             (return)))
         (if matched
             (progn
               (setf (slot-value req 'lumen.core.http::params)
                     (extract-path-params (route-pattern matched)
                                          (route-param-names matched)
                                          path))
               (funcall (route-handler matched) req))
             ;; si aucun handler, mais pattern existant -> 405 ; sinon -> next (404 plus loin)
             (if (allowed-methods-for path)
                 (respond-405 path)
                 (funcall next req))))))))

|#
