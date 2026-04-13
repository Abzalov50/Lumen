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
   :dispatch :dispatch-async
   :*global-router* :construct-route))

(in-package :lumen.core.router)

;;; ===========================================================================
;;; 1. STRUCTURES DE DONNÉES
;;; ===========================================================================

(defstruct route
  method                ; "GET" / "POST" ...
  pattern               ; scanner cl-ppcre pour le PATH
  param-names           ; noms des params extraits du PATH
  handler               ; (req &rest extras) -> response
  host-scanner          ; scanner cl-ppcre pour l'HÔTE (NIL => match tout)
  host-param-names      ; réservé
  source-path)          ; "/users/:id" (pour introspection)

(defclass router ()
  ((routes :initform (make-array 0 :adjustable t :fill-pointer 0) 
           :accessor router-routes)))

(defun create-router ()
  "Crée une nouvelle instance de routeur vide."
  (make-instance 'router))

(defun add-route-to-router (router r)
  (vector-push-extend r (router-routes router)))

;;; ===========================================================================
;;; 2. REGISTRE DES MODULES
;;; ===========================================================================

(defvar *module-registry* (make-hash-table :test 'equal))

(defun register-module-routes (module-name routes-list)
  "Enregistre une liste de routes pour un module donné."
  (let ((clean-routes (remove-if #'null routes-list)))
    (setf (gethash module-name *module-registry*) clean-routes)
    (format t "~&[Router] Module ~A : ~D routes enregistrées.~%" 
            module-name (length clean-routes))
    clean-routes))

#|
(defun merge-module-routes (router module-name)
  "Injecte les routes d'un module dans un routeur actif."
  (let ((routes (gethash module-name *module-registry*)))
    (unless routes
      (warn "Module ~A not found in registry (or empty)." module-name))
    (dolist (r routes)
      (add-route-to-router router r))))
|#

(defun merge-module-routes (router module-name &key app-prefix)
  "Injecte les routes d'un module dans un routeur actif.
   Si app-prefix est fourni, recompile la volée les routes pour s'y conformer."
  (let ((routes (gethash module-name *module-registry*)))
    (unless routes
      (warn "Module ~A not found in registry (or empty)." module-name))
    
    (dolist (r routes)
      (if app-prefix
          ;; CAS A : L'APPLICATION A UN PRÉFIXE
          (let* ((old-path (route-source-path r))
                 ;; Concaténation propre : si old-path est "/", on le supprime pour éviter "/holiperf/"
                 (new-path (format nil "~A~A" 
                                   (string-right-trim "/" app-prefix)
                                   (if (string= old-path "/") "" old-path))))
            
            ;; 1. On recompile la Regex (pattern) avec le nouveau chemin absolu
            (multiple-value-bind (rx params) (compile-path new-path)
              
              ;; 2. On clone la route originale (généré automatiquement par defstruct route)
              (let ((cloned-route (copy-route r)))
                ;; 3. On écrase les attributs du clone avec les nouvelles données
                (setf (route-pattern cloned-route) (cl-ppcre:create-scanner rx))
                (setf (route-param-names cloned-route) params)
                (setf (route-source-path cloned-route) new-path)
                
                ;; 4. On ajoute le clone au routeur
                (add-route-to-router router cloned-route))))
          
          ;; CAS B : COMPORTEMENT NORMAL (PAS DE PRÉFIXE)
          (add-route-to-router router r)))))
;;; ===========================================================================
;;; 3. COMPATIBILITÉ GLOBALE
;;; ===========================================================================

(defparameter *global-router* (make-instance 'router))

(defun clear-routes ()
  (setf (router-routes *global-router*) (make-array 0 :adjustable t :fill-pointer 0)))

(defun add-route (r)
  (add-route-to-router *global-router* r))

;;; ===========================================================================
;;; 4. LOGIQUE DE MATCHING & REGEX
;;; ===========================================================================

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

;; --- MISE A JOUR PROXY : Support du Wildcard "*" ---
(defun compile-path (path)
  (let* ((path (or path "/"))
         (parts (remove-if (lambda (s) (zerop (length s))) (cl-ppcre:split "/" path)))
         (rx "^") (params '()))
    (if (null parts) (setf rx "^/$")
        (progn (dolist (p parts)
                 (setf rx (concatenate 'string rx "/"))
                 (cond
                   ;; 1. Paramètre nommé :id
                   ((and (> (length p) 1) (char= (char p 0) #\:))
                    (push (string-downcase (subseq p 1)) params)
                    (setf rx (concatenate 'string rx "([^/]+)")))
                   
                   ;; 2. Wildcard Proxy * (Devient ".*" pour tout capturer)
                   ((string= p "*")
                    (setf rx (concatenate 'string rx ".*")))
                   
                   ;; 3. Segment statique (échappé)
                   (t
                    (setf rx (concatenate 'string rx (cl-ppcre:quote-meta-chars p))))))
               (setf rx (concatenate 'string rx "$"))))
    (values rx (nreverse params))))

;;; ===========================================================================
;;; 5. DEFROUTE & CONSTRUCT-ROUTE
;;; ===========================================================================

(defun %parse-route-args (path-arg)
  (cond ((stringp path-arg) (values nil path-arg))
        ((listp path-arg) (values (or (getf path-arg :hosts) (getf path-arg :host)) (getf path-arg :path)))
        (t (error "Invalid defroute path-arg: ~S" path-arg))))

;; --- MISE A JOUR VHOST : Ajout de &key host ---
(defmacro construct-route ((method path-arg arglist &key host) &body body)
  "Compile une route. Supporte maintenant :host explicitement."
  (let* ((m (string-upcase (string method)))
         (rx (gensym "RX")) 
         (params (gensym "PARAMS")) 
         (host-sc (gensym "HOSTSC")) 
         (handler (gensym "HANDLER"))
         
         (raw (if (and arglist (listp arglist)) (first arglist) arglist))
         (req-sym (if (listp raw) (first raw) raw))
         (req-sym (or req-sym (intern "REQ" *package*)))
         (extra-args (if (listp arglist) (rest arglist) nil)))
    
    `(multiple-value-bind (parsed-host %path) (%parse-route-args ,path-arg)
       ;; Priorité : Argument explicite > Argument dans le path (Legacy)
       (let* ((final-host (or ,host parsed-host))
              (,rx      (nth-value 0 (compile-path %path)))
              (,params  (nth-value 1 (compile-path %path)))
              (,host-sc (compile-host-spec final-host)))
         (flet ((,handler (,req-sym ,@extra-args)
                  (declare (ignorable ,req-sym ,@extra-args))
                  ,@body))
           (make-route :method ,m 
                       :pattern (cl-ppcre:create-scanner ,rx)
                       :param-names ,params 
                       :handler #',handler
                       :host-scanner ,host-sc 
                       :source-path ,path-arg))))))

(defmacro defguarded (method path-arg args options &body body)
  `(add-route (construct-guarded-route ,method ,path-arg ,args ,options ,@body)))

(defmacro defroute (method path-arg arglist &body body)
  `(add-route (construct-route (,method ,path-arg ,arglist) ,@body)))

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
;;; 6. DISPATCH
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
    (when (member "PATCH" methods :test #'string=) (pushnew "PUT" methods :test #'string=))
    (when (member "PUT" methods :test #'string=) (pushnew "PATCH" methods :test #'string=))
    (when methods (pushnew "OPTIONS" methods :test #'string=))
    (print "IN ALLOWAD METHODS")
    (print path)
    (print methods)
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
             (match-method
	       (cond ((string= method "HEAD") "GET")
		     ((string= method "PUT") '("PATCH" "PUT"))
		     ((string= method "PATCH") '("PATCH" "PUT"))
		     (t	 method)))
             (host   (normalize-host req))
             (matched nil))
        (format t "~&[MATCH-AND-EXECUTE] METHODE: ~A | HOST: ~A | PATH: ~A | MATCH-METHOD: ~A~%" method host path match-method)
        
        (cond
          ((string= method "OPTIONS")
           (if (allowed-methods-for router path)
               (respond-options router path)
               (respond-404 "Not Found")))
          
          (t
           ;; 1. Recherche de la route
           (loop for r across (router-routes router) do
             (when (and (if (stringp match-method)
			    (string= match-method (route-method r))
			    (member (route-method r) match-method :test #'equal))
                        (%match-route-host-p r host)
                        (cl-ppcre:scan (route-pattern r) path))
               (setf matched r)
               (return)))
	   (format t "~&[MATCH-AND-EXECUTE] ROUTE MATCHED? ~A~%" (if matched t nil))
           
           (if matched
               (progn
                 ;; 2. Extraction des valeurs des paramètres (ex: #("123"))
                 ;; On utilise scan-to-strings pour récupérer les groupes de capture
                 (let ((param-values 
                        (multiple-value-bind (full-match regs)
                            (cl-ppcre:scan-to-strings (route-pattern matched) path)
                          (declare (ignore full-match))
                          (if regs (coerce regs 'list) nil)))) ;; Convertit le vecteur en liste
                   
                   ;; 3. Mise à jour du contexte
                   (lumen.core.http:ctx-set!
		    req :params 
                    (extract-path-params (route-pattern matched)
					 (route-param-names matched) path))
                   (lumen.core.http:ctx-set! req :route-pattern (route-pattern matched))
		   ;;(print (route-handler matched))
                   ;; 4. EXECUTION AVEC APPLY (Le Correctif)
                   ;; Si param-values est ("123"), cela appelle (handler req "123")
                   ;; Si param-values est NIL, cela appelle (handler req)
                   (let ((response (apply (route-handler matched) req param-values)))
		     ;;(print response)
		     response)))
               
               ;; Pas de route trouvée
               (if (allowed-methods-for router path)
                   (respond-405 router path)
                   (respond-404 "Not Found"))))))
    
    (http-halt (c) (halt-response c))))

(defun match-and-execute-async (router req)
  "Cœur du routage asynchrone : Retourne toujours (lambda (responder) ...)"
  (lambda (responder)
    (handler-case
        (let* ((method (req-method req))
               (path   (req-path req))
               (match-method
                (cond ((string= method "HEAD") "GET")
                      ((string= method "PUT") '("PATCH" "PUT"))
                      ((string= method "PATCH") '("PATCH" "PUT"))
                      (t method)))
               (host   (normalize-host req))
               (matched nil))
          
          (format t "~&[MATCH-ASYNC] METH: ~A | HOST: ~A | PATH: ~A~%" method host path)
          
          (cond
            ;; --- REQUÊTES OPTIONS (CORS) ---
            ((string= method "OPTIONS")
             (funcall responder
                      (if (allowed-methods-for router path)
                          (respond-options router path)
                          (respond-404 "Not Found"))))
            
            ;; --- ROUTAGE CLASSIQUE ---
            (t
             ;; 1. Recherche de la route
             (loop for r across (router-routes router) do
               (when (and (if (stringp match-method)
                              (string= match-method (route-method r))
                              (member (route-method r) match-method :test #'equal))
                          (%match-route-host-p r host)
                          (cl-ppcre:scan (route-pattern r) path))
                 (setf matched r)
                 (return)))
             
             (if matched
                 (progn
                   ;; 2. Extraction des valeurs des paramètres
                   (let ((param-values 
                          (multiple-value-bind (full-match regs)
                              (cl-ppcre:scan-to-strings (route-pattern matched) path)
                            (declare (ignore full-match))
                            (if regs (coerce regs 'list) nil))))
                     
                     ;; 3. Mise à jour du contexte
                     (lumen.core.http:ctx-set! req :params 
                                               (extract-path-params (route-pattern matched)
                                                                    (route-param-names matched) path))
                     (lumen.core.http:ctx-set! req :route-pattern (route-pattern matched))
                     
                     ;; 4. EXÉCUTION HYBRIDE (La Magie Asynchrone)
                     (let ((result (apply (route-handler matched) req param-values)))
                       ;; On vérifie si l'application est asynchrone (Proxy) ou synchrone (API classique)
                       (if (functionp result)
                           ;; Le handler est asynchrone : on lui passe le relais (le callback)
                           (funcall result responder)
                           ;; Le handler est synchrone : on exécute le callback immédiatement
                           (funcall responder result)))))
                 
                 ;; 5. Pas de route trouvée
                 (funcall responder
                          (if (allowed-methods-for router path)
                              (respond-405 router path)
                              (respond-404 "Not Found")))))))
      
      ;; 6. Gestion du HTTP Halt
      (http-halt (c)
        (funcall responder (halt-response c)))
      
      ;; 7. Filet de sécurité
      (error (e)
        (format t "~&[ROUTER ASYNC ERROR] ~A~%" e)
        (funcall responder (respond-500 "Internal Router Error"))))))

(defun dispatch (req)
  (match-and-execute *global-router* req))

(defun dispatch-async (req)
  "Examine la requête, trouve la route correspondante et retourne une tâche asynchrone (CPS)."
  (lambda (responder)
    ;; 1. On cherche la route dans la table de routage globale
    ;; (En supposant une fonction match-route qui renvoie le handler et les paramètres d'URL)
    (multiple-value-bind (handler params)
        (match-route (lumen.core.http:req-method req)
                     (lumen.core.http:req-path req))
      
      (if handler
          ;; ==========================================
          ;; CAS 1 : ROUTE TROUVÉE
          ;; ==========================================
          (progn
            ;; On injecte les variables d'URL dans la requête (ex: /users/:id -> id)
            (setf (lumen.core.http:req-params req) params)
            
            ;; On exécute la route métier en la protégeant contre les crashs
            (handler-case
                (let ((result (funcall handler req)))
                  
                  ;; Support Hybride (Sync/Async) :
                  (if (functionp result)
                      ;; A. Le handler a renvoyé une tâche asynchrone, on lui passe notre responder
                      (funcall result responder)
                      
                      ;; B. Le handler a renvoyé un objet Réponse (Lisp standard), on répond immédiatement
                      (funcall responder result)))
              
              ;; Interception des crashs dans la route métier
              (error (e)
                (format t "~&[ROUTER CRASH] ~A~%" e)
                (funcall responder (lumen.core.http:respond-500)))))
          
          ;; ==========================================
          ;; CAS 2 : AUCUNE ROUTE (404 PASS-THROUGH)
          ;; ==========================================
          ;; C'est ce bloc précis qui sera intercepté par la condition 
          ;; (= status 404) de votre middleware pour appeler 'next'.
          (funcall responder (lumen.core.http:respond-404))))))

;;; ===========================================================================
;;; 7. INTROSPECTION
;;; ===========================================================================

(defun %all-routes-registry-list ()
  (loop for r across (router-routes *global-router*)
        collect (list :method (route-method r)
                      :path (route-source-path r)
                      :params (route-param-names r))))
