(in-package :cl)

(defpackage :lumen.app.app
  (:use :cl)
  (:export :defapp :start-app :stop-app :app-config :app-routes
	   :app-middleware :app-name :app-port :app-listeners :app-modules
   :get-app-conf :reload!))

(in-package :lumen.app.app)

;; La variable magique accessible partout (Contrôleurs, Middlewares)
;;(defvar *current-app* nil)

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
#|
(defmacro defapp (name &key port modules config middleware)
  `(defparameter ,name
     (make-instance 'lumen-app
                    :name ',name
                    :port ,port
                    :modules ',modules
                    :config ',config
                    :middleware ,middleware)))
|#

(defmacro defapp (name &key port modules config middleware)
  `(progn
     ;; 1. On s'assure que la variable globale existe (sans l'écraser si elle existe déjà)
     (defvar ,name nil)
     
     ;; 2. Logique de Création ou Mise à jour (Idempotence)
     (if (and ,name (typep ,name 'lumen-app))
         
         ;; CAS A : L'instance existe déjà -> On la met à jour (HOT RELOAD)
         (progn
           (format t "~&[DEFAPP] Mise à jour de l'instance existante ~A...~%" ',name)
           (reinitialize-instance ,name
                                  :port ,port
                                  :modules ,modules
                                  :config ',config
                                  :middleware ,middleware)
           ;; On force la recompilation des routes immédiatement
           (%compile-routes ,name))
         
         ;; CAS B : Nouvelle instance (Premier chargement)
         (setf ,name
               (make-instance 'lumen-app
                              :name ',name
                              :port ,port
                              :modules ,modules
                              :config ',config
                              :middleware ,middleware)))
     
     ;; 3. On retourne l'objet
     ,name))

;;; --- COMPILATION DES ROUTES ---
(defun %compile-routes (app)
  (let ((router (lumen.core.router:create-router))) 
    (dolist (mod-key (app-modules app))
      (lumen.core.router:merge-module-routes router mod-key))
    (setf (app-routes app) router)))

;;; --- RECUPERATION DES CONFIGS DE L'APP ACTIVE ---
(defun get-app-conf (key &optional default)
  "Récupère une valeur de config de l'application courante."
  (let ((val (getf (app-config lumen.core.context:*current-app*) key)))
    (or val default)))

;;; --- DEMARRAGE ---
(defmethod start-app ((app lumen-app))
  (when (app-listeners app)
    (format t "~&[APP] ~A already running.~%" (app-name app))
    (return-from start-app))

  ;; 1. Préparation du routeur
  (%compile-routes app)

  ;; ---------------------------------------------------------
  ;; 2. DÉMARRAGE DU SCHEDULER & JOBS SYSTÈME
  ;; ---------------------------------------------------------
  (format t "~&[APP] Starting Scheduler...~%")
  (lumen.core.scheduler:start-scheduler)
  
  ;; On planifie le GC de session toutes les heures (3600s)
  (lumen.core.scheduler:schedule-cron 'lumen.http.session:session-gc 3600)
  
  ;; On lance un premier nettoyage immédiat (asynchrone) pour être propre dès le boot
  (lumen.core.scheduler:enqueue 'lumen.http.session:session-gc nil)
    
  ;; 3. CRÉATION DU HANDLER
  (let ((handler-fn 
          (lambda (req)
            ;; A. Injection du Contexte Global (pour introspect, trace, etc.)
            (let ((lumen.core.context:*current-app* app))

	      ;; A. On récupère mws et router A CHAQUE REQUÊTE depuis l'objet app.
              ;; Cela permet le Hot-Reloading.
              (let ((current-mws (app-middleware app))
                    (current-router (app-routes app)))
               
		;; C. Appel du Pipeline (Fonction Pure)
		;; On passe explicitement la liste des mws et le "Final Handler" (le Router)
		(lumen.core.pipeline:execute-middleware-chain 
                 current-mws
                 ;; Le "Next" final, c'est le Routeur
                 (lambda (final-req)
                   ;; On utilise le routeur fraîchement récupéré
                   (if current-router
                       (lumen.core.router:match-and-execute current-router final-req)
                       ;; Fallback sécurité si routeur nil
                       '(500 (:content-type "text/plain") ("No router initialized"))))
                 ;; La requête initiale
                 req))))))
      
    ;; 4. Démarrage du serveur
    (setf (app-listeners app)
          (lumen.core.server:start 
           :port (app-port app)
           :handler handler-fn)))
  
  (format t "~&[APP] ~A started on port ~A.~%" (app-name app) (app-port app)))

(defun reload! (app)
  "Recharge la configuration et les routes de l'application SANS couper le serveur."
  (format t "~&[APP] Reloading ~A...~%" (app-name app))
  
  ;; 1. Re-compiler le routeur (puisque defmodule a changé les tables globales)
  (%compile-routes app)
  
  ;; 2. Si vous avez une logique pour recharger les config ou middlewares, mettez-la ici.
  ;; Note: defapp recrée déjà la liste des middlewares quand on recompile app.lisp,
  ;; mais attention : defapp crée une NOUVELLE instance.
  
  (format t "~&[APP] Routes updated. Ready.~%"))

(defmethod stop-app ((app lumen-app))

  ;; 1. Arrêt du serveur HTTP
  (when (app-listeners app)
    (lumen.core.server:stop (app-listeners app))
    (setf (app-listeners app) nil))

  ;; 2. Arrêt du Scheduler
  (lumen.core.scheduler:stop-scheduler)
  (format t "~&[APP] ~A stopped.~%" (app-name app)))
