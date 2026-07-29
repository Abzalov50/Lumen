(in-package :cl)

(defpackage :lumen.app.app
  (:use :cl :lumen.core.config)
  (:export :defapp :start-app :stop-app :app-config :app-routes
	   :app-middleware :app-name :app-port :app-listeners :app-modules
   :get-app-conf :reload! :app-path))

(in-package :lumen.app.app)

(defclass lumen-app ()
  ((name       :initarg :name       :accessor app-name)
   (port       :initarg :port       :initform 8080 :accessor app-port)
   (config     :initarg :config     :initform nil :accessor app-config)
   (prefix     :initarg :app-prefix :initform nil :accessor app-prefix)
   (routes     :initform (make-hash-table :test 'equal) :accessor app-routes)
   (middleware :initarg :middleware :initform nil :accessor app-middleware)
   (modules    :initarg :modules    :initform nil :accessor app-modules)
   ;; On stocke ici la liste des 'listeners' retournés par lumen.core.server:start
   (listeners  :initform nil :accessor app-listeners)))

;;; --- MACRO ---
(defmacro defapp (name &key port modules config middleware app-prefix)
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
                                  :middleware ,middleware
				  :app-prefix ,app-prefix)
           ;; On force la recompilation des routes immédiatement
           (%compile-routes ,name))
         
         ;; CAS B : Nouvelle instance (Premier chargement)
         (setf ,name
               (make-instance 'lumen-app
                              :name ',name
                              :port ,port
                              :modules ,modules
                              :config ',config
                              :middleware ,middleware
			      :app-prefix ,app-prefix)))
     
     ;; 3. On retourne l'objet
     ,name))

;; On définit un JOB système invisible pour l'utilisateur
(lumen.core.scheduler:defjob :sys-gc-temp-files (ignore)
  (declare (ignore ignore))
  (lumen.extras.jobs::%system-cleanup-spool))

;; Helper pour le reverse
(defun app-path (base-path)
  "Préfixe dynamiquement un chemin avec le préfixe de l'application active."
  (let* ((app lumen.core.context:*current-app*)
         (prefix (when app (app-prefix app))))
    (if prefix
        ;; Concaténation sécurisée (évite les doubles slashes)
        (format nil "~A~A" 
                (string-right-trim "/" prefix)
                (if (string= base-path "/") "" base-path))
        base-path)))

;;; --- COMPILATION DES ROUTES ---
(defun %compile-routes (app)
  (let ((router (lumen.core.router:create-router))
	(prefix (app-prefix app))) 
    (dolist (mod-key (app-modules app))
      (lumen.core.router:merge-module-routes router mod-key :app-prefix prefix))
    (setf (app-routes app) router)))

;;; --- RECUPERATION DES CONFIGS DE L'APP ACTIVE ---
(defun get-app-conf (key &optional default)
  "Récupère une valeur de config de l'application courante."
  (let ((val (getf (app-config lumen.core.context:*current-app*) key)))
    (or val default)))

(defun ensure-runtime-directories ()
  "S'assure que les dossiers critiques du framework existent et sont accessibles."
  (format t "~&[Lumen] Vérification du dossier temporaire : ~A~%" lumen.core.config:*tmp-dir*)
  
  ;; 1. Tentative de création
  (handler-case 
      (ensure-directories-exist *tmp-dir*)
    (file-error (e)
      (error "FATAL: Impossible de créer le dossier temporaire Lumen à '~A'. Vérifiez les permissions. Erreur: ~A" 
             *tmp-dir* e)))

  ;; 2. Vérification d'écriture (Test ultime)
  ;; On essaie d'écrire un petit fichier pour être sûr qu'on a le droit.
  (let ((test-file (merge-pathnames "write_test.tmp" *tmp-dir*)))
    (handler-case
        (progn
          (with-open-file (out test-file :direction :output :if-exists :supersede)
            (write-line "test" out))
          (delete-file test-file))
      (error (e)
        (error "FATAL: Le dossier temporaire '~A' n'est pas accessible en écriture. Les uploads échoueront. Erreur: ~A" 
               *tmp-dir* e))))
  
  (format t "~&[Lumen] Dossier temporaire OK.~%"))

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

  ;; Lumen cherche le nom du pool DB dans la config de l'app.
  ;; Si l'application ne spécifie rien, Lumen utilise :DEFAULT par convention.
  (let ((db-pool-name
          (getf (app-config app) :db-pool :default)))

    (format t "~&[APP] Planification du nettoyage des sessions.~%")

    ;; Nettoyage périodique uniquement.
    ;; Aucun lancement immédiat au démarrage.
    (lumen.core.scheduler:schedule-cron
     'lumen.http.session:session-gc
     3600
     db-pool-name))
    
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

    ;; On prépare l'environnement avant de lancer le serveur HTTP
    (ensure-runtime-directories)

    ;; 2. Nettoyage immédiat au boot (fichiers présents avant le start)
    ;; On peut le faire de manière synchrone ou threadée ici, sans risque d'âge
    ;; car au boot, aucun upload n'est en cours.
    (bt:make-thread (lambda () (lumen.extras.jobs::%system-cleanup-spool)) 
                    :name "lumen-boot-gc")

    ;; 3. Planification du GC Récurrent (Toutes les heures)
    ;; Le développeur n'a rien à faire, c'est "built-in".
    (lumen.core.scheduler:schedule-cron :sys-gc-temp-files (* 24 3600) nil)
      
    ;; 4. Démarrage du serveur
    (let* ((ssl-requested-p
             (getf
              (app-config app)
              :ssl
              nil))

	   (ssl-port
             (getf
              (app-config app)
              :ssl-port
              8443))

	   (cert-file
             (getf
              (app-config app)
              :cert-file
              nil))

	   (key-file
             (getf
              (app-config app)
              :key-file
              nil))

	   ;; HTTPS n'est activé que si les deux fichiers sont fournis.
	   (ssl-enabled
             (and ssl-requested-p
		  cert-file
		  key-file)))

      (when (and ssl-requested-p
		 (not ssl-enabled))

	(format *error-output*
		"~&[SSL] HTTPS désactivé : certificat ou clé privée manquant.~%"))

      (setf
       (app-listeners app)

       (lumen.core.server:start
	:port (app-port app)
	:handler handler-fn
	:ssl ssl-enabled
	:ssl-port ssl-port
	:cert-file cert-file
	:key-file key-file))

      (format t
              "~&[APP] ~A started on port ~A (SSL: ~A).~%"
              (app-name app)
              (app-port app)
              (if ssl-enabled
		  "ON"
		  "OFF"))))
  )

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
