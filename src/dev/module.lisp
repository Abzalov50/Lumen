(in-package :cl)

(defpackage :lumen.dev.module
  (:use :cl)
  (:import-from :lumen.core.router :defroute :with-guards :def-api-route)
  (:import-from :lumen.data.dao :defentity)
  (:import-from :lumen.http.crud :make-entity-crud-guard :mount-crud!)
  (:import-from :lumen.core.pipeline :execute-middleware-chain)
  (:export :defmodule :find-module :get-modules
   :module-meta-path-prefix :module-meta-name :module-meta-doc
	   :module-meta-entities :module-meta-resources :module-meta-routes
	   ))

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

;;; --- HELPERS D'ANALYSE (Compile-Time) ---

(eval-when (:compile-toplevel :load-toplevel :execute)
  
  (defun %split-body-opts (body-form)
    (let ((opts '()) (code body-form))
      (loop while (and code (keywordp (car code)))
            do (push (pop code) opts) (push (pop code) opts))
      (values (nreverse opts) code)))

  (defun %generate-crud-meta (resource-def module-prefix)
    (destructuring-bind (entity-sym &key (name (string-downcase (symbol-name entity-sym)))
                                         (actions '(:index :show :create :patch :delete)) &allow-other-keys) resource-def
      (let* ((prefix (string-right-trim "/" (or module-prefix "")))
             (base-path (format nil "~A/~A" prefix name)) 
             (routes '()))
        (when (member :index actions) (push `(:method "GET" :path ,base-path :summary ,(format nil "List ~A" name)) routes))
        (when (member :create actions) (push `(:method "POST" :path ,base-path :summary ,(format nil "Create ~A" name)) routes))
        (when (member :show actions) (push `(:method "GET" :path ,(format nil "~A/:id" base-path) :summary ,(format nil "Get ~A" name)) routes))
        (when (member :patch actions) (push `(:method "PATCH" :path ,(format nil "~A/:id" base-path) :summary ,(format nil "Update ~A" name)) routes))
        (when (member :delete actions) (push `(:method "DELETE" :path ,(format nil "~A/:id" base-path) :summary ,(format nil "Delete ~A" name)) routes))
        (nreverse routes))))

  (defun %extract-custom-route-meta (route-def module-prefix)
    (destructuring-bind (method subpath args &body body) route-def
      (declare (ignore args))
      (multiple-value-bind (opts code) (%split-body-opts body)
        (declare (ignore code))
        `(:method ,(string-upcase (symbol-name method))
          :path ,(format nil "~A~A" (string-right-trim "/" module-prefix) subpath)
           :summary ,(getf opts :summary "Custom Route")))))

  (defun %extract-declarations (body)
    "Sépare les (declare ...) du reste du corps."
    (let ((decls '())
          (rest body))
      (loop while (and (consp (first rest)) 
                       (eq (car (first rest)) 'declare))
            do (push (pop rest) decls))
      (values (nreverse decls) rest)))

  (defun %expand-route (method subpath args body-and-opts prefix module-mws-sym)
  "Génère la définition de route.
   - module-mws-sym : Le symbole de la variable contenant la liste des instances de middlewares du module."
  (multiple-value-bind (opts raw-code) (%split-body-opts body-and-opts)
    
    ;; 1. EXTRACTION DES DECLARATIONS
    (multiple-value-bind (decls code) (%extract-declarations raw-code)
      
      (let* ((full-path (format nil "~A~A" prefix subpath))
             (method-str (string-upcase (symbol-name method)))
             (is-api-route (or (getf opts :summary) (getf opts :scopes) 
                               (getf opts :roles) (getf opts :tag) (getf opts :params)))
             
             ;; 2. INSTRUMENTATION (Sans les déclarations)
             (traced-code 
              `((lumen.core.trace:with-tracing ("Route Handler" 
                                                :path ,full-path 
                                                :method ,method-str)
                  ,@code))) ;; On ne met que le code exécutable ici

             ;; 3. CHAÎNAGE & REINSERTION DES DÉCLARATIONS
             (final-body 
              (if module-mws-sym
                  ;; Cas Middleware : On injecte les decls dans la lambda anonyme
                  `((lumen.core.pipeline:execute-middleware-chain 
                     ,module-mws-sym 
                     (lambda ,args 
                       ,@decls  ;; <-- Les déclarations remontent ICI (début de lambda)
                       ,@traced-code) 
                     ,(first args)))
                  
                  ;; Cas Simple : On injecte les decls au début du body de defroute
                  `(,@decls  ;; <-- ICI
                    ,@traced-code))))

        (if is-api-route
            `(def-api-route ,method ,full-path ,args 
               (:summary ,(getf opts :summary) :tag ,(getf opts :tag)
                :scopes ',(getf opts :scopes) :roles ',(getf opts :roles) :params ,(getf opts :params)) 
               ,@final-body)
            `(lumen.core.router:defroute ,method ,full-path ,args
               ,@final-body))))))

  ;; --- 4. EXPANSION DES HOOKS INTELLIGENTE & CONGRUENTE ---  
  (defun %expand-hook (entity-sym hook-def)
  "Génère la méthode CLOS en garantissant la congruence des arguments."
  (let* ((hook-key  (first hook-def))
         (rest-def  (cdr hook-def))
         (qualifier (when (member (first rest-def) '(:around :before :after)) (pop rest-def)))
         (category  (case hook-key ((:index :show :create :patch :delete) :core) (t :hook)))
         (generic-name (case hook-key
                         (:index     'lumen.data.repo.core:repo-index)
                         (:show      'lumen.data.repo.core:repo-show)
                         (:create    'lumen.data.repo.core:repo-create)
                         (:patch     'lumen.data.repo.core:repo-patch)
                         (:delete    'lumen.data.repo.core:repo-delete)
                         (:normalize 'lumen.data.repo.core:repo-normalize)
                         (:validate  'lumen.data.repo.core:repo-validate)
                         (:before    'lumen.data.repo.core:repo-before)
                         (:after     'lumen.data.repo.core:repo-after)
                         (:persist   'lumen.data.repo.core:repo-persist)
                         (:authorize 'lumen.data.repo.core:repo-authorize)
                         (t (error "Hook inconnu: ~A" hook-key)))))

    (let ((op-val (if (eq category :hook) (pop rest-def) nil)) ;; La valeur (ex: :create)
          (args   (pop rest-def))
          (body   rest-def))

      (let ((adjusted-args 
              (cond
                ;; CAS A : :before, :persist, :authorize 
                ;; Signature Generic : (op entity ctx &key ...)
                ;; User Input        : (ctx id payload)
                ;; Transformation    : (ctx &key id payload &allow-other-keys)
                ((member hook-key '(:before :persist :authorize))
                 (let ((ctx-var (first args))
                       (keys (rest args)))
                   `(,ctx-var &key ,@keys &allow-other-keys)))

                ;; CAS B : :after
                ;; Signature Generic : (op entity ctx result &key ...)
                ;; User Input        : (ctx result)
                ;; Transformation    : (ctx result &key &allow-other-keys)
                ((eq hook-key :after)
                 ;; On suppose que l'utilisateur met toujours (ctx result ...)
                 (append args '(&key &allow-other-keys)))

                ;; CAS C : :normalize, :validate
                ;; Signature Generic : (op entity ctx payload) -> Tout est positionnel
                ;; User Input        : (ctx payload)
                ;; Transformation    : (ctx payload) -> Pas de changement
                ((member hook-key '(:normalize :validate))
                 args)

                ;; CAS D : Core Methods (create, patch...)
                ;; On ajoute juste &allow-other-keys par sécurité si absent
                (t (if (member '&key args)
                       args
                       (append args '(&key &allow-other-keys)))))))

        ;; 5. Construction Signature finale
        (let ((method-args
                (if (eq category :core)
                    `((entity (eql ',entity-sym)) ,@adjusted-args)
                    `((op (eql ,op-val)) (entity (eql ',entity-sym)) ,@adjusted-args))))

          `(defmethod ,generic-name 
             ,@(when qualifier (list qualifier))
             ,method-args
             (declare (ignorable op entity ,(first args)))
             ,@body))))))
  
  )

;;; --- LA MACRO DEFMODULE ---
(defmacro defmodule (name &key doc path-prefix middlewares entities resources routes hooks)
  (let ((sanitized-prefix (string-right-trim "/" (or path-prefix "")))
        (mws-var (intern (format nil "*~A-MIDDLEWARES*" name)))) ;; Variable pour stocker les instances
    
    (let ((all-routes-meta 
            (append 
             (loop for res in resources appending (%generate-crud-meta res sanitized-prefix))
             (loop for r in routes collect (%extract-custom-route-meta r sanitized-prefix)))))
      
      `(progn
         ;; 0. Instantiation des Middlewares du Module
         (defparameter ,mws-var (list ,@middlewares))

         ;; 1. Register Meta
         (register-module! ,name 
			   (make-module-meta :name ,name :doc ,doc
					     :path-prefix ,sanitized-prefix
					     :entities ',(mapcar #'car entities)
					     :resources ',(mapcar #'car resources)
					     :routes ',all-routes-meta))
         
         ;; 2. DAO Entities
         ,@(loop for e in entities collect `(defentity ,@e))
         
         ;; 3. CRUD Resources (À adapter pour injecter mws-var si mount-crud le supporte)
         ,@(loop for res in resources collect
				      (destructuring-bind (entity-sym
							   &key (name (string-downcase (symbol-name entity-sym)))
                                                      (guard nil) (required-p t) (actions '(:index :show :create :patch :delete)) &allow-other-keys) res
                   ;; TODO: Passer ,mws-var à mount-crud! pour qu'il l'applique aussi
                   `(let ((g ,(if guard guard `(lumen.http.crud:make-entity-crud-guard ',entity-sym :required-p ,required-p))))
                      (mount-crud! ',entity-sym :base ,sanitized-prefix :name ,name :auth-guard g :actions ',actions))))
         
         ;; 4. Custom Routes (Injecte mws-var)
         ,@(loop for r in routes collect
                 (destructuring-bind (method subpath args &body body) r
                   (%expand-route method subpath args body sanitized-prefix mws-var)))
         
         ;; 5. HOOKS EXPANSION
         ,@(loop for (entity-sym . entity-hooks) in hooks appending
                 (loop for hook-def in entity-hooks collect
                       (%expand-hook entity-sym hook-def)))))))
