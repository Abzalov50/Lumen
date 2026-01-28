(in-package :cl)

(defpackage :lumen.core.app
  (:use :cl)
  (:export :defapp :start-app :stop-app :*current-app* :app-config :app-routes
	   :app-middleware :app-name :app-port :app-listeners :app-modules
   :get-app-conf))

(in-package :lumen.core.app)

;; La variable magique accessible partout (Contrôleurs, Middlewares)
(defvar *current-app* nil)

(defclass lumen-app ()
  ((name       :initarg :name       :accessor app-name)
   (port       :initarg :port       :initform 8080 :accessor app-port)
   (config     :initarg :config     :initform nil :accessor app-config)
   (routes     :initform (make-hash-table :test 'equal) :accessor app-routes)
   (middleware :initarg :middleware :initform nil :accessor app-middleware)
   (modules    :initarg :modules    :initform nil :accessor app-modules)
   ;; On stocke ici la liste des 'listeners' retournés par lumen.core.server:start
   (listeners  :initform nil :accessor app-listeners)))

;;; --- MACRO ---
(defmacro defapp (name &key port modules config middleware)
  `(defparameter ,name
     (make-instance 'lumen-app
                    :name ',name
                    :port ,port
                    :modules ',modules
                    :config ',config
                    :middleware ,middleware)))

;;; --- COMPILATION DES ROUTES ---
(defun %compile-routes (app)
  (let ((router (lumen.core.router:create-router))) 
    (dolist (mod-key (app-modules app))
      (lumen.core.router:merge-module-routes router mod-key))
    (setf (app-routes app) router)))

;;; --- RECUPERATION DES CONFIGS DE L'APP ACTIVE ---
(defun get-app-conf (key &optional default)
  "Récupère une valeur de config de l'application courante."
  (let ((val (getf (app-config *current-app*) key)))
    (or val default)))

;;; --- DEMARRAGE ---
(defmethod start-app ((app lumen-app))
  (when (app-listeners app)
    (format t "~&[APP] ~A already running.~%" (app-name app))
    (return-from start-app))

  ;; 1. Préparation du routeur
  (%compile-routes app)
  
  ;; 2. PRÉPARATION DES COMPOSANTS (Extraction pour performance)
  (let ((mws (app-middleware app))
        (router (app-routes app)))
    
    ;; 3. CRÉATION DU HANDLER
    (let ((handler-fn 
           (lambda (req)
             ;; A. Injection du Contexte Global (pour introspect, trace, etc.)
             (let ((*current-app* app)) 
               
               ;; B. Appel du Pipeline (Fonction Pure)
               ;; On passe explicitement la liste des mws et le "Final Handler" (le Router)
               (lumen.core.pipeline:execute-middleware-chain 
                mws
                ;; Le "Next" final, c'est le Routeur
                (lambda (final-req)
                  (lumen.core.router:match-and-execute router final-req))
                ;; La requête initiale
                req)))))
      
      ;; 4. Démarrage du serveur
      (setf (app-listeners app)
            (lumen.core.server:start 
             :port (app-port app)
             :handler handler-fn))))
  
  (format t "~&[APP] ~A started on port ~A.~%" (app-name app) (app-port app)))

(defmethod stop-app ((app lumen-app))
  (when (app-listeners app)
    (lumen.core.server:stop (app-listeners app))
    (setf (app-listeners app) nil)
    (format t "~&[APP] ~A stopped.~%" (app-name app))))
