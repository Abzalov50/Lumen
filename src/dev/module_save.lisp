(in-package :cl)

(defpackage :lumen.dev.module
  (:use :cl)
  (:import-from :lumen.core.router :defroute :defguarded :def-api-route :construct-route)
  (:import-from :lumen.data.dao :defentity :register-entity-meta-only!)
  (:import-from :lumen.http.crud :make-entity-crud-guard :mount-crud!)
  (:import-from :lumen.core.pipeline :execute-middleware-chain)
  (:export :defmodule :find-module :get-modules
           :module-meta-path-prefix :module-meta-name :module-meta-doc
           :module-meta-entities :module-meta-resources :module-meta-routes))

(in-package :lumen.dev.module)

;;; --- REGISTRY ---
(defvar *app-modules* (make-hash-table :test 'eq))

(defstruct module-meta name doc path-prefix entities resources routes)

(defun register-module! (name meta)
  (setf (gethash name *app-modules*) meta))

(defun get-modules ()
  (loop for k being the hash-keys of *app-modules* using (hash-value v) collect v))

(defun find-module (name)
  (gethash name *app-modules*))

(defmacro construct-guarded-route (method path-arg args options &body body)
  ;; --- FIX DÉFENSIF ULTIME ---
  (let* ((raw (if (and args (listp args)) (first args) args))
         (req-sym (if (listp raw) (first raw) raw))
         (req-sym (or req-sym (intern "REQ" *package*)))
         
         (rk (gensym "ROUTEKEY"))
         (required (getf options :required))
         (roles    (getf options :roles))
         (scopes   (getf options :scopes))
         (sc-mode  (getf options :scopes-mode))
         (rate-cap (getf options :rate-cap))
         (rate-refill (getf options :rate-refill))
         (route-key   (getf options :route-key))
         (admin-bypass? (getf options :admin-bypass?))
         (admin-roles   (getf options :admin-roles))
         (allow-query?  (getf options :allow-query-token?))
         (qs-keys       (getf options :qs-keys))
         (secret        (getf options :secret))
         (leeway        (getf options :leeway-sec))
         
         (required-effective (or required (not (null roles)) (not (null scopes)))))

    `(construct-route ,method ,path-arg ,args
       (let* ((,rk (or ,route-key
                       (multiple-value-bind (host-spec real-path)
                           (lumen.core.router::%parse-route-args ,path-arg)
                         (declare (ignore host-spec))
                         (format nil "~A ~A" ,(string-upcase (string method)) real-path)))))
         
         (declare (ignorable ,rk))
         
         (lumen.core.pipeline:execute-middleware-chain
          (list
           ;; 1. AUTH
           (make-instance 'lumen.core.middleware:auth-middleware
                          :required-p ,(if required-effective t nil)
                          ,@(when roles        `(:roles-allow ,roles))
                          ,@(when scopes       `(:scopes-allow ,scopes))
                          ,@(when sc-mode      `(:scopes-mode ,sc-mode))
                          ,@(when admin-roles  `(:admin-roles ,admin-roles))
                          ,@(when admin-bypass? `(:bypass-admin ,admin-bypass?))
                          ,@(when (not (null allow-query?)) `(:allow-query ,allow-query?))
                          ,@(when qs-keys      `(:qs-keys ,qs-keys))
                          ,@(when secret       `(:secret ,secret))
                          ,@(when leeway       `(:leeway ,leeway)))
           
           ;; 2. RATE LIMIT
           ,@(when rate-cap
               `((make-instance 'lumen.core.middleware:rate-limit-middleware
                                :capacity ,rate-cap
                                :refill ,(or rate-refill 1)
                                :route-key ,rk))))
          
          ;; Handler : On utilise req-sym garanti symbole
          (lambda (,req-sym) 
            ,@body)
          
          ;; Initial Req : On passe le symbole variable
          ,req-sym)))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  
  (defun %split-body-opts (body-form)
    (let ((opts '()) (code body-form))
      (loop while (and code (keywordp (car code)))
            do (push (pop code) opts) (push (pop code) opts))
      (values (nreverse opts) code)))

  (defun %generate-crud-meta (resource-def module-prefix)
    (destructuring-bind (entity-sym
			 &key (name (string-downcase (symbol-name entity-sym)))
                           (actions '(:index :show :create :patch :delete))
			 &allow-other-keys)
	resource-def
      (let* ((prefix (string-right-trim "/" (or module-prefix "")))
             (base-path (format nil "~A/~A" prefix name)) 
             (routes '()))
        (when (member :index actions) (push `(:method "GET" :path ,base-path :summary ,(format nil "List ~A" name)) routes))
        (when (member :create actions) (push `(:method "POST" :path ,base-path :summary ,(format nil "Create ~A" name)) routes))
        (when (member :show actions) (push `(:method "GET" :path ,(format nil "~A/:id" base-path) :summary ,(format nil "Get ~A" name)) routes))
        (when (member :patch actions) (push `(:method "PATCH" :path ,(format nil "~A/:id" base-path) :summary ,(format nil "Update ~A" name)) routes))
        (when (member :delete actions) (push `(:method "DELETE" :path ,(format nil "~A/:id" base-path) :summary ,(format nil "Delete ~A" name)) routes))
        (nreverse routes))))

  #|
  (defun %extract-custom-route-meta (route-def module-prefix)
    (if (keywordp (first route-def)) ;; C'est une route standard (:GET ...)
	(destructuring-bind (method subpath args &body body) route-def
          (declare (ignore args))
          (multiple-value-bind (opts code) (%split-body-opts body)
            (declare (ignore code))
            `(:method ,(string-upcase (symbol-name method))
               :path ,(format nil "~A~A" (string-right-trim "/" module-prefix) subpath)
               :summary ,(getf opts :summary "Custom Route"))))
	;; C'est un générateur (mount-...), on ne peut pas deviner la doc statiquement
  nil))
  |#

  (defun %extract-custom-route-meta (route-def module-prefix)
  (if (and (consp route-def) (keywordp (first route-def)))
      (destructuring-bind (method subpath args &body body) route-def
        (declare (ignore args))
        (multiple-value-bind (opts code) (%split-body-opts body)
          (declare (ignore code))
          `(:method ,(string-upcase (symbol-name method))
            :path ,(format nil "~A~A" (string-right-trim "/" module-prefix) subpath)
            :summary ,(getf opts :summary "Custom Route"))))
      nil))

  (defun %extract-declarations (body)
    "Sépare les (declare ...) du reste du corps."
    (let ((decls '()) (rest body))
      (loop while (and (consp (first rest)) 
                       (eq (car (first rest)) 'declare))
            do (push (pop rest) decls))
      (values (nreverse decls) rest)))

  ;; --- EXPANSION ROUTES CUSTOM ---
(defun %expand-route (method subpath args body-and-opts prefix module-mws-sym)
  (multiple-value-bind (opts raw-code) (%split-body-opts body-and-opts)
    (multiple-value-bind (decls code) (%extract-declarations raw-code)
      (let* ((full-path (format nil "~A~A" prefix subpath))
             (method-str (string-upcase (symbol-name method)))
             (method-kw  (intern method-str :keyword))
             
             ;; --- FIX DÉFENSIF ULTIME ---
             ;; On s'assure d'avoir le symbole pur 'req', même si args est ((req)) ou (req)
             (req-sym (let ((raw (if (and args (listp args)) (first args) args)))
                        (if (listp raw) (first raw) raw)))
             ;; Si req-sym est nul (route sans param), on crée un symbole
             (req-sym (or req-sym (intern "REQ" *package*)))

             (is-api-route (or (getf opts :roles) (getf opts :scopes) (getf opts :tag)))
             (traced-code 
              `((lumen.core.trace:with-tracing ("Route Handler" :path ,full-path :method ,method-str)
                  ,@code)))
             
             (final-body 
              (if module-mws-sym
                  `((lumen.core.pipeline:execute-middleware-chain 
                     ,module-mws-sym 
                     (lambda ,args 
                       ,@decls
                       ,@traced-code) 
                     ;; ICI : On passe req-sym garanti symbole
                     ,req-sym)) 
                  `(,@decls
                    ,@traced-code))))
        
        (if is-api-route
            `(construct-guarded-route ,method-kw ,full-path ,args
               (:roles ,(getf opts :roles) 
                :scopes ,(getf opts :scopes)
                :admin-bypass? ,(getf opts :admin-bypass? t)
                :allow-query-token? t)
               ,@final-body)
            
            `(lumen.core.router:construct-route ,method-kw ,full-path ,args
               ,@final-body))))))

  ;; --- EXPANSION HOOKS ---
  (defun %expand-hook (entity-sym hook-def)
    (let* ((hook-key  (first hook-def))
           (rest-def  (cdr hook-def))
           (qualifier (when (member (first rest-def) '(:around :before :after)) (pop rest-def)))
           (category  (case hook-key ((:index :show :create :patch :delete) :core) (t :hook)))
           (generic-name (case hook-key
                           (:index      'lumen.data.repo.core:repo-index)
                           (:show       'lumen.data.repo.core:repo-show)
                           (:create     'lumen.data.repo.core:repo-create)
                           (:patch      'lumen.data.repo.core:repo-patch)
                           (:delete     'lumen.data.repo.core:repo-delete)
                           (:normalize 'lumen.data.repo.core:repo-normalize)
                           (:validate  'lumen.data.repo.core:repo-validate)
                           (:before     'lumen.data.repo.core:repo-before)
                           (:after      'lumen.data.repo.core:repo-after)
                           (:persist    'lumen.data.repo.core:repo-persist)
                           (:authorize 'lumen.data.repo.core:repo-authorize)
                           (t (error "Hook inconnu: ~A" hook-key)))))

      (let ((op-val (if (eq category :hook) (pop rest-def) nil))
            (args   (pop rest-def))
            (body   rest-def))

        (let ((adjusted-args 
                (cond
                  ((member hook-key '(:before :persist :authorize))
                   (let ((ctx-var (first args)) (keys (rest args)))
                     `(,ctx-var &key ,@keys &allow-other-keys)))
                  ((eq hook-key :after)
                   (append args '(&key &allow-other-keys)))
                  ((member hook-key '(:normalize :validate))
                   args)
                  (t (if (member '&key args) args (append args '(&key &allow-other-keys)))))))

          (let ((method-args
                  (if (eq category :core)
                      `((entity (eql ',entity-sym)) ,@adjusted-args)
                      `((op (eql ,op-val)) (entity (eql ',entity-sym)) ,@adjusted-args))))

            `(defmethod ,generic-name 
               ,@(when qualifier (list qualifier))
               ,method-args
               (declare (ignorable op entity ,(first args)))
               ,@body)))))))

;;; --- LA MACRO DEFMODULE ---
(defmacro defmodule (name &key doc path-prefix middlewares entities resources routes hooks)
  (let ((sanitized-prefix (string-right-trim "/" (or path-prefix "")))
        (mws-var (intern (format nil "*~A-MIDDLEWARES*" name)))) 
    
    ;; ---------------------------------------------------------
    ;; PHASE 1 : PRÉ-ENREGISTREMENT (META-DATA)
    ;; ---------------------------------------------------------
    ;; On peuple le registre DAO immédiatement pour que la suite de la macro
    ;; (mount-crud!) puisse lire les infos (table, fields...).
    (dolist (ent-def entities)
      (destructuring-bind (sym &rest args) ent-def
        (lumen.data.dao:register-entity-meta-only! sym args)))

    ;; ---------------------------------------------------------
    ;; PHASE 2 : ANALYSE DES ROUTES
    ;; ---------------------------------------------------------
    ;; Maintenant, mount-crud! va fonctionner car le registre est plein.
    
    (let ((all-routes-meta 
            (append 
             (loop for res in resources appending (%generate-crud-meta res sanitized-prefix))
             (remove nil (loop for r in routes collect (%extract-custom-route-meta r sanitized-prefix))))))
      
      `(progn
         ;; 0. Middlewares
         (defparameter ,mws-var (list ,@middlewares))

         ;; 1. Register Module Meta
         (register-module! ,name 
           (make-module-meta :name ,name :doc ,doc
                             :path-prefix ,sanitized-prefix
                             :entities ',(mapcar #'car entities)
                             :resources ',(mapcar #'car resources)
                             :routes ',all-routes-meta))
         
         ;; 2. Code Runtime pour les Entités (defentity officiel)
         ;; On le garde car il contient peut-être plus de logique runtime (hooks, types précis...)
         ,@(loop for e in entities collect `(defentity ,@e))
         
         ;; 3. Routes (CRUD + Custom)
         (lumen.core.router:register-module-routes ,name
           (list 
            ;; A. CRUD
            ,@(loop for res in resources appending
                    (destructuring-bind (entity-sym
                                         &key (name (string-downcase (symbol-name entity-sym)))
                                              (guard nil) (required-p t) 
                                              (host nil) (order-whitelist nil)
                                              (actions '(:index :show :create :patch :delete)) 
                                         &allow-other-keys) res
                      
                      ;; APPEL DIRECT À LA FONCTION GÉNÉRATRICE
                      (lumen.http.crud:mount-crud! 
                         entity-sym 
                         :base sanitized-prefix 
                         :name name
                         :host host
                         :order-whitelist order-whitelist ;; Attention au quote ici
                         :actions actions ;; Attention au quote ici
                         :auth-guard (if guard 
                                         guard 
                                         `(lumen.http.crud:make-entity-crud-guard ',entity-sym :required-p ,required-p)))))
            
            ;; B. Custom
            ,@(loop for r in routes collect
                    (destructuring-bind (method subpath args &body body) r
                      (%expand-route method subpath args body sanitized-prefix mws-var)))))
         
         ;; 4. Hooks
         ,@(loop for (entity-sym . entity-hooks) in hooks appending
                 (loop for hook-def in entity-hooks collect
                       (%expand-hook entity-sym hook-def)))))))
