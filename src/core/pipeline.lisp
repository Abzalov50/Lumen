(in-package :cl)

(defpackage :lumen.core.pipeline
  (:use :cl)
  (:export :middleware
   :handle
           :pipeline
   :defmiddleware
           :make-pipeline
   :pipeline-add
           :pipeline-compile
   :pipeline-middlewares
           :execute-pipeline
   :describe-pipeline
	   :execute-middleware-chain
   :pipeline-to-list))

(in-package :lumen.core.pipeline)

;;; ---------------------------------------------------------------------------
;;; 1. L'INTERFACE MIDDLEWARE (CLOS)
;;; ---------------------------------------------------------------------------

(defclass middleware ()
  ((name :initarg :name 
         :initform :unnamed 
         :accessor mw-name
         :documentation "Nom pour l'introspection/debug")
   (enabled :initarg :enabled 
            :initform t 
            :accessor mw-enabled-p))
  (:documentation "Classe de base pour tous les middlewares Lumen 2.0"))

(defgeneric handle (middleware request next)
  (:documentation "Méthode principale. 
   - middleware : l'instance de l'objet (pour accéder à sa config).
   - request    : l'objet requête actuel.
   - next       : une FONCTION (thunk) à appeler pour passer au suivant."))

;; Implémentation par défaut : passe-plat transparent
(defmethod handle ((mw middleware) request next)
  (funcall next request))

;;; ---------------------------------------------------------------------------
;;; LA MACRO MAGIQUE (Defclass + Defmethod wrapper)
;;; ---------------------------------------------------------------------------
(eval-when (:compile-toplevel :load-toplevel :execute)
  (defmacro defmiddleware (name slots-and-args (req-var next-var) &body body)
    "Définit un middleware CLOS.
   
   Exemple:
   (defmiddleware cors-middleware 
       ((origin :initarg :origin :initform \"*\")) ;; Slots CLOS classiques
       (req next)
     (let ((res (funcall next req))) ;; Corps de la méthode handle
       (acons \"Access-Control-Allow-Origin\" (slot-value mw 'origin) res)))"
  
    (let ((class-name name)
          (mw-var (intern "MW"))) ;; Nom de variable pour l'instance (this/self)
    
      `(progn
	 ;; 1. Définition de la classe (Configuration)
	 (defclass ,class-name (middleware)
           ,slots-and-args
           (:documentation ,(format nil "Middleware ~A" class-name)))

	 ;; 2. Définition de la méthode (Logique)
	 (defmethod handle ((,mw-var ,class-name) ,req-var ,next-var)
           ;; On rend les slots accessibles comme des variables locales via with-slots ?
           ;; Simplification: l'utilisateur utilisera (slot-value mw '...) ou des accessors
           ;; Pour plus de confort, on peut wrapper dans un symbol-macrolet plus tard.
         
           (declare (ignorable ,mw-var)) ;; Au cas où le mw n'a pas de config
           (block lumen.core.pipeline:handle
             ,@body)))))
  )

;;; ---------------------------------------------------------------------------
;;; 2. L'OBJET PIPELINE (Le Conteneur)
;;; ---------------------------------------------------------------------------

(defclass pipeline ()
  ((middlewares :initform (make-array 0 :adjustable t :fill-pointer 0)
                :accessor pipeline-middlewares
                :documentation "Vecteur ordonné des instances de middleware.")
   (compiled-chain :accessor pipeline-chain
                   :documentation "La closure compilée prête à exécuter (Cache).")
   (final-handler :initarg :final-handler
                  :initform (lambda (req) (declare (ignore req)) '(404 (:content-type "text/plain") ("Not Found")))
                  :accessor pipeline-final-handler)))

(defun make-pipeline (&key final-handler)
  (let ((p (make-instance 'pipeline)))
    (when final-handler
      (setf (pipeline-final-handler p) final-handler))
    p))

(defun pipeline-add (pipeline middleware-instance)
  "Ajoute une INSTANCE de middleware à la fin du pipeline."
  (vector-push-extend middleware-instance (pipeline-middlewares pipeline))
  ;; On invalide le cache
  (slot-makunbound pipeline 'compiled-chain)
  pipeline)

;;; ---------------------------------------------------------------------------
;;; 3. LA COMPILATION (Chain of Responsibility)
;;; ---------------------------------------------------------------------------

(defun pipeline-compile (pipeline)
  "Transforme la liste d'objets en une seule fonction executable (Closure optimisée)."
  (let* ((mws (pipeline-middlewares pipeline))
         (app (pipeline-final-handler pipeline)))
    
    ;; On parcourt la liste à l'envers : le dernier MW appelle 'app', 
    ;; l'avant-dernier appelle le dernier, etc.
    (loop for i downfrom (1- (length mws)) to 0
          do (let ((current-mw (aref mws i))
                   (next-step app)) ;; Capture de la closure 'next'
               (setf app (lambda (req)
                           (if (mw-enabled-p current-mw)
                               (handle current-mw req next-step)
                               ;; Si désactivé, on saute directement au suivant
                               (funcall next-step req))))))
    
    (setf (pipeline-chain pipeline) app)))

(defun execute-pipeline (pipeline request)
  "Point d'entrée pour le serveur HTTP."
  (unless (slot-boundp pipeline 'compiled-chain)
    (pipeline-compile pipeline))
  (funcall (pipeline-chain pipeline) request))

(defun execute-middleware-chain (middlewares final-handler request)
  "Exécute une liste de middlewares sur une requête."
  (let ((next final-handler))
    (loop for i downfrom (1- (length middlewares)) to 0
          do (let ((mw (nth i middlewares))
                   (n next))
               (setf next (lambda (r) 
                            (if (mw-enabled-p mw)
                                (handle mw r n)
                                (funcall n r))))))
    (funcall next request)))

;;; ---------------------------------------------------------------------------
;;; 4. INTROSPECTION (Lumen 2.0 Feature)
;;; ---------------------------------------------------------------------------

(defmethod describe-object ((p pipeline) stream)
  (format stream "#<PIPELINE (~D layers)>~%" (length (pipeline-middlewares p)))
  (loop for mw across (pipeline-middlewares p)
        for i from 1
        do (format stream "  ~2D. ~A [~A]~%" 
                   i 
                   (type-of mw) 
                   (if (mw-enabled-p mw) "ON" "OFF"))))

(defun describe-pipeline (pipeline)
  "Retourne une liste de descriptions pour l'API d'inspection."
  (loop for mw across (pipeline-middlewares pipeline)
        collect (list :type (string (type-of mw))
                      :name (mw-name mw)
                      :enabled (mw-enabled-p mw)
                      ;; On pourrait ajouter ici :config (introspect-slots mw)
                      )))

(defun safe-slot-value (obj slot-name)
  (if (slot-boundp obj slot-name)
      (slot-value obj slot-name)
      :unbound))

(defun pipeline-to-list (pipeline)
  "Convertit le pipeline en structure de données pour JSON."
  (loop for mw across (pipeline-middlewares pipeline)
        for i from 1
        collect 
        `((:index . ,i)
          (:name . ,(mw-name mw))
          (:type . ,(string (type-of mw)))
          (:enabled . ,(mw-enabled-p mw))
          (:package . ,(package-name (symbol-package (type-of mw))))
          ;; Introspection basique des slots configurables (via initargs)
          ;; Nécessite SBCL MOP ou closer-mop. Version simplifiée :
          (:description . ,(format nil "~A" mw)))))
