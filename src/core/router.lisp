(in-package :cl)
(defpackage :lumen.core.router
  (:use :cl :alexandria :cl-ppcre)
  (:import-from :lumen.core.http
   :request :response :req-method :req-path :req-headers :req-query :req-cookies
   :req-params :req-body-stream :req-ctx :resp-status :resp-headers
   :resp-body :respond-text :respond-json :respond-404 :respond-500 :respond-405
   :ctx-get :ctx-set! :halt-response :http-halt)
  (:export 
   ;; API Utilisateur
   :defroute :param :with-params :clear-routes
   :defprotected :defroles :with-guards :defguarded :def-api-route
   ;; API Introspection
   :%all-routes-registry-list :respond-415 :respond-options
   ;; API Container (Lumen 2.0)
   :create-router :register-module-routes :merge-module-routes :match-and-execute
   :*global-router* :construct-route))

(in-package :lumen.core.router)

;;; ===========================================================================
;;; 1. STRUCTURES DE DONNÉES (Route & Router)
;;; ===========================================================================

(defstruct route
  method                ; "GET" / "POST" ...
  pattern               ; scanner cl-ppcre pour le PATH
  param-names           ; noms des params extraits du PATH
  handler               ; (req &rest extras) -> response
  host-scanner          ; scanner cl-ppcre pour l'HÔTE (NIL => match tout)
  host-param-names      ; réservé
  source-path)          ; "/users/:id" (pour introspection)

;; NOUVEAU : On encapsule la liste des routes dans une structure "Router"
;; Cela permet d'avoir plusieurs routeurs (un par App)
(defclass router ()
  ((routes :initform (make-array 0 :adjustable t :fill-pointer 0) 
           :accessor router-routes)))

(defun create-router ()
  "Crée une nouvelle instance de routeur vide."
  (make-instance 'router))

(defun add-route-to-router (router r)
  (vector-push-extend r (router-routes router)))

;;; ===========================================================================
;;; 2. REGISTRE DES MODULES (Le "Catalogue")
;;; ===========================================================================

;; Stocke les définitions de routes par module (avant instanciation de l'app)
;; Map : Keyword (Nom Module) -> List of Route Objects
(defvar *module-registry* (make-hash-table :test 'equal))

(defun register-module-routes (module-name routes-list)
  "Enregistre une liste de routes pour un module donné."
  (let ((clean-routes (remove nil routes-list)))
    (setf (gethash module-name *module-registry*) clean-routes)
    (format t "~&[Router] Module ~A : ~D routes enregistrées.~%" 
            module-name (length clean-routes))
    clean-routes))

(defun merge-module-routes (router module-name)
  "Injecte les routes d'un module dans un routeur actif."
  (let ((routes (gethash module-name *module-registry*)))
    (unless routes
      (warn "Module ~A not found in registry (or empty)." module-name))
    (dolist (r routes)
      (add-route-to-router router r))))

;;; ===========================================================================
;;; 3. COMPATIBILITÉ GLOBALE (Legacy)
;;; ===========================================================================

;; Le routeur par défaut pour les scripts simples (sans defapp)
(defparameter *global-router* (make-instance 'router))

(defun clear-routes ()
  "Réinitialise le routeur global."
  (setf (router-routes *global-router*) (make-array 0 :adjustable t :fill-pointer 0)))

(defun add-route (r)
  "Ajoute une route au routeur global (Legacy)."
  (add-route-to-router *global-router* r))

;;; ===========================================================================
;;; 4. LOGIQUE DE MATCHING (Hôte, Path, Regex)
;;; ===========================================================================
;; (Fonctions utilitaires inchangées, je les garde compactes)

(defun normalize-host (req)
  (let* ((h (or (ctx-get req :host)
                (cdr (assoc "host" (req-headers req) :test #'string-equal)))))
    (when h (let ((p (position #\: h))) (if p (subseq h 0 p) h)))))

(defun %wildcard-host->regex (s)
  (let* ((parts (uiop:split-string s :separator "."))
         (rx (map 'string #'identity
                  (with-output-to-string (out)
                    (loop for i from 0 below (length parts) do
                      (when (> i 0) (write-string "\\." out))
                      (let ((p (elt parts i)))
                        (if (string= p "*") (write-string "[^.]+" out) (write-string (cl-ppcre:quote-meta-chars p) out))))))))
    (format nil "^(?:~A)$" rx)))

(defun %re-quote (s) (cl-ppcre:quote-meta-chars s))

(defun %host->regex-body (host)
  (cond ((cl-ppcre:scan "^\\*\\*\\.(.+)$" host)
         (format nil "(?:[^.]+\\.)+~A" (%re-quote (subseq host 3))))
        ((cl-ppcre:scan "^\\*\\.(.+)$" host)
         (format nil "(?:[^.]+\\.)~A" (%re-quote (subseq host 2))))
        (t (%re-quote host))))

(defun compile-host-spec (spec)
  (cond ((null spec) nil)
        ((stringp spec)
         (cl-ppcre:create-scanner (format nil "^(?:~A)$" (%host->regex-body spec)) :case-insensitive-mode t))
        ((listp spec)
         (cl-ppcre:create-scanner (format nil "^(?:~{~A~^|~})$" (mapcar #'%host->regex-body spec)) :case-insensitive-mode t))
        (t (error "Invalid Host spec: ~S" spec))))

(defun compile-path (path)
  (let* ((path (or path "/"))
         (parts (remove-if (lambda (s) (zerop (length s))) (cl-ppcre:split "/" path)))
         (rx "^") (params '()))
    (if (null parts) (setf rx "^/$")
        (progn (dolist (p parts)
                 (setf rx (concatenate 'string rx "/"))
                 (if (and (> (length p) 1) (char= (char p 0) #\:))
                     (progn (push (string-downcase (subseq p 1)) params)
                            (setf rx (concatenate 'string rx "([^/]+)")))
                     (setf rx (concatenate 'string rx (cl-ppcre:quote-meta-chars p)))))
               (setf rx (concatenate 'string rx "$"))))
    (values rx (nreverse params))))

;;; ===========================================================================
;;; 5. DEFROUTE & PARAMS
;;; ===========================================================================
(defun %parse-route-args (path-arg)
  (cond ((stringp path-arg) (values nil path-arg))
        ((listp path-arg) (values (or (getf path-arg :hosts) (getf path-arg :host)) (getf path-arg :path)))
        (t (error "Invalid defroute path-arg: ~S" path-arg))))

;; 1. Fabrique l'objet route (Regex, Handler) mais ne l'enregistre pas.
(defmacro construct-route (method path-arg arglist &body body)
  "Compile une route et retourne l'instance #S(ROUTE ...) sans effet de bord."
  (let* ((m (string-upcase (string method)))
         (doc (when (stringp (first body)) (first body)))
         ;;(actual-body (if doc (rest body) body))
         (rx (gensym "RX")) 
         (params (gensym "PARAMS")) 
         (host-sc (gensym "HOSTSC")) 
         (handler (gensym "HANDLER"))
         
         ;; --- FIX DÉFENSIF ULTIME ---
         (raw (if (and arglist (listp arglist)) (first arglist) arglist))
         (req-sym (if (listp raw) (first raw) raw))
         (req-sym (or req-sym (intern "REQ" *package*)))
         
         (extra-args (if (listp arglist) (rest arglist) nil)))
    
    `(multiple-value-bind (host-spec %path) (%parse-route-args ,path-arg)
       (let* ((,rx     (nth-value 0 (compile-path %path)))
              (,params (nth-value 1 (compile-path %path)))
              (,host-sc (compile-host-spec host-spec)))
         (flet ((,handler (,req-sym ,@extra-args)
                  (declare (ignorable ,req-sym ,@extra-args))
                  ,@body))
           ;; ON RETOURNE L'OBJET
           (make-route :method ,m 
                       :pattern (cl-ppcre:create-scanner ,rx)
                       :param-names ,params 
                       :handler #',handler
                       :host-scanner ,host-sc 
                       :source-path ,path-arg))))))

;; Legacy Wrapper (pour compatibilité si nécessaire ailleurs)
(defmacro defguarded (method path-arg args options &body body)
  `(add-route (construct-guarded-route ,method ,path-arg ,args ,options ,@body)))

;; 2. DEFROUTE (Legacy / Scripting)
;; On garde defroute pour la compatibilité, mais il utilise maintenant construct-route
(defmacro defroute (method path-arg arglist &body body)
  "Ajoute une route au routeur global (Comportement legacy)."
  `(add-route (construct-route ,method ,path-arg ,arglist ,@body)))

(defun extract-path-params (scanner names path)
  (multiple-value-bind (match regs) (cl-ppcre:scan-to-strings scanner path)
    (when match
      (loop for nm in names for i from 0
            for v = (aref regs i) when v collect (cons (string-downcase (string nm)) v)))))

(defun param (req name)
  (cdr (assoc (string-downcase name) (req-params req) :test #'string=)))

(defmacro with-params ((req-sym &rest names) &body body)
  `(let ,(mapcar (lambda (n) `(,n (param ,req-sym ,(string-downcase (string n))))) names) ,@body))

;;; ===========================================================================
;;; 6. DISPATCH & EXECUTION (Refactorisé pour l'instance)
;;; ===========================================================================

(defun %match-route-host-p (route host)
  (let ((sc (route-host-scanner route)))
    (or (null sc) (and host (cl-ppcre:scan sc host)))))

(defun allowed-methods-for (router path)
  (let ((methods '()))
    (loop for r across (router-routes router) do
      (when (cl-ppcre:scan (route-pattern r) path)
        (pushnew (route-method r) methods :test #'string=)))
    (when (member "GET" methods :test #'string=) (pushnew "HEAD" methods :test #'string=))
    (when methods (pushnew "OPTIONS" methods :test #'string=))
    (sort (copy-list methods) #'string<)))

(defun respond-options (router path)
  (let ((methods (allowed-methods-for router path)))
    (make-instance 'lumen.core.http:response :status 204
                   :headers (list (cons "allow" (format nil "~{~A~^, ~}" methods)))
                   :body "")))

(defun respond-405 (router path)
  (let ((methods (allowed-methods-for router path)))
    (lumen.core.http:respond-json 
     `((:error . ((:type . "method_not_allowed") (:allowed . ,methods))))
     :status 405 :headers (list (cons "allow" (format nil "~{~A~^, ~}" methods))))))

(defun match-and-execute (router req)
  "Cœur du routage : Prend un routeur spécifique et une requête."
  (handler-case
      (let* ((method (req-method req))
             (path   (req-path req))
             (match-method (if (string= method "HEAD") "GET" method))
             (host   (normalize-host req))
             (matched nil))

	(print method)
	(print path)
	(print match-method)
	(print host)
        
        (cond
          ;; OPTIONS Auto-magic
          ((string= method "OPTIONS")
           (if (allowed-methods-for router path)
               (respond-options router path)
               (respond-404 "Not Found")))
          
          (t
           ;; Recherche linéaire dans le routeur fourni
           (loop for r across (router-routes router) do
             (when (and (string= match-method (route-method r))
                        (%match-route-host-p r host)
                        (cl-ppcre:scan (route-pattern r) path))
               (setf matched r)
               (return)))
	   (print matched)
           
           (if matched
               ;; MATCH
               (progn
                 (lumen.core.http:ctx-set! req :params 
                   (extract-path-params (route-pattern matched) (route-param-names matched) path))
                 (lumen.core.http:ctx-set! req :route-pattern (route-pattern matched))
                 (funcall (route-handler matched) req))
               
               ;; NO MATCH
               (if (allowed-methods-for router path)
                   (respond-405 router path)
                   (respond-404 "Not Found"))))))
    
    (http-halt (c) (halt-response c))))

(defun dispatch (req)
  "Legacy Dispatcher (utilise le routeur global)."
  (match-and-execute *global-router* req))

;;; ===========================================================================
;;; 7. INTROSPECTION
;;; ===========================================================================

(defun %all-routes-registry-list ()
  (loop for r across (router-routes *global-router*)
        collect (list :method (route-method r)
                      :path (route-source-path r)
                      :params (route-param-names r))))
