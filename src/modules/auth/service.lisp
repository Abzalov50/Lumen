(defpackage :lumen.modules.auth.service
  (:use :cl :lumen.utils :lumen.core.jwt :lumen.core.auth :lumen.core.http :lumen.http.session)
  (:export :authenticate-user :hash-password :verify-password   
   :login-user :logout-user :get-scopes-for-role
	   :respond-success :respond-error
	   :issue-token-for :current-user-id :is-authenticated-p
	   :is-impersonating-p :impersonate-user :stop-impersonation))

(in-package :lumen.modules.auth.service)

(defparameter *role-definitions*
  '(("admin"   . ("*")) ;; Super-admin
    ("manager" . ("read:*" 
                  "write:nc" "delete:nc" "validate:nc"
                  "write:action" "delete:action"
                  "write:kpi"))
    ("user"    . ("read:*" 
                  "write:nc" 
                  "write:action" 
                  "write:kpi-entry"))) ;; Peut saisir une valeur, mais pas configurer le KPI
  "Matrice de correspondance entre les Rôles utilisateurs et les Scopes techniques.")

;; Clé utilisée dans la session data pour stocker l'ID user
(defparameter *session-user-key* "uid")
(defparameter *session-tenant-key* :tid) ;; Si on veut stocker le tenant aussi
(defparameter *session-impersonator-key* "impersonator_id")

(defun login-user (req user-id &optional tenant-id)
  "Enregistre l'utilisateur dans la session courante."
  ;; Le middleware se chargera de persister tout ça et d'envoyer le cookie
  (session-set! req *session-user-key* user-id)
  (when tenant-id
    (session-set! req *session-tenant-key* tenant-id))
  t)

(defun logout-user (req)
  "Déconnecte l'utilisateur (nettoie les clés Auth de la session)."
  (session-del! req *session-user-key*)
  (session-del! req *session-tenant-key*)
  t)

(defun current-user-id (req)
  "Retourne l'ID de l'utilisateur connecté (ou NIL)."
  (session-get req *session-user-key*))

(defun is-authenticated-p (req)
  (not (null (current-user-id req))))

(defun get-scopes-for-role (role)
  "Retourne la liste des scopes associés à un rôle (pour la génération de Token)."
  (cdr (assoc role *role-definitions* :test #'string=)))

(defun %verify-password (raw-password user)
  "Vérifie si le mot de passe correspond au hash stocké (PBKDF2)."
  (let ((salt  (col-get user :pw_salt))
        (iters (col-get user :pw_iters))
        (hash  (col-get user :pw_hash)))
    (if (and salt iters hash)
        (lumen.core.auth:verify-password raw-password salt iters hash)
        nil)))

(defun authenticate-user (email password tenant-id)
  "Vérifie les crédentiels, calcule les scopes, et retourne le JWT token + User."
  (lumen.data.db:ensure-connection
    (let* (;; 1. Résolution du Tenant
           (tenant (pomo:query "SELECT * FROM tenants WHERE id = $1" tenant-id :alist)))
      (format t "~&[AUTH USER] TENANT ID: ~A~%" tenant-id)
      (unless tenant (return-from authenticate-user (values nil "Organisation inconnue")))

      ;; 2. Recherche Utilisateur
      (let ((user (pomo:query 
                          "SELECT * FROM users WHERE email = $1 AND tenant_id = $2 AND is_active = 'true'" 
                          email tenant-id :alist)))
        (format t "~&[AUTH USER] USER: ~A~%" user)
        ;; 3. Vérification
        (if (and user (%verify-password password user))
            (let* ((uid  (alist-get user :id))
                   (role (alist-get user :role))
                   
                   ;; --- VOTRE LOGIQUE DE SCOPES ---
                   ;; A. Scopes du Rôle
                   (role-scopes (get-scopes-for-role role))
                   ;; B. Scopes Utilisateur (JSONB -> List)
                   (user-scopes (coerce (alist-get user :scopes) 'list))
                   ;; C. Fusion
                   (final-scopes (remove-duplicates 
                                  (append role-scopes user-scopes) 
                                  :test #'string=)))
              (format t "~&[AUTH USER] USER ROLE: ~A~%USER SCOPES: ~A~%ROLE SCOPES: ~A~%" role user-scopes role-scopes)
              ;; 4. Génération du JWT (Access Token)
              ;; Note: Pour une App Web (HTMX), on utilise souvent juste un Access Token 
              ;; avec une durée de vie moyenne (ex: 2h) stocké en Cookie Secure.
              (let ((token (lumen.core.jwt:issue-access
                            uid 
                            :role role 
                            :scopes final-scopes
                            :tenant tenant-id
                            :claims `((:firstname . ,(alist-get user :firstname))
                                      (:lastname  . ,(alist-get user :lastname)))
                            :ttl (* 3600 2)))) ;; 2 heures
                (format t "~&[AUTH USER] TOKEN: ~A~%" token)
                (values user token)))
            
            ;; Echec
            (values nil "Email ou mot de passe incorrect"))))))

(defun respond-success (req user target-url &key (msg "Opération réussie"))
  "Enregistre l'utilisateur en session et redirige via HTMX."
  (print "OOOOOO")
  ;; 1. Session (Gérée par le middleware)
  (login-user req (lumen.utils:alist-get user :id))

  (print "OOOOOO")
  ;; 2. Réponse HTML vide
  (let ((resp (respond-html "")))
    
    ;; 3. Toast
    (setf (resp-headers resp)
          (lumen.utils:ensure-header (resp-headers resp) "HX-Trigger" 
           (cl-json:encode-json-to-string 
            `((:show-message . ((:type . "success") (:message . ,msg)))))))

    ;; 4. Redirection
    (setf (resp-headers resp)
          (lumen.utils:ensure-header
	   (resp-headers resp) 
	   "HX-Redirect"
	   target-url))
    (print "OOOOOO")
    (print (resp-headers resp))
    resp))

(defun respond-error (form-values error-msg &key (render-fn #'lumen.modules.auth.view:render-signup-form))
  "Génère une réponse HTMX d'erreur :
   1. Renvoie le HTML du formulaire avec les valeurs saisies et le message d'erreur.
   2. Status 200 (pour que HTMX swap) ou 422 (le fix JS)."
  
  (let ((html-content (funcall render-fn 
                               :values form-values 
                               :error error-msg)))
    
    (respond-html html-content 
                  ;; On force 200 OK pour simplifier le swap HTMX sans script JS additionnel,
                  ;; ou 422 si vous avez mis en place le listener 'htmx:beforeSwap'.
                  :status 200)))

(defun issue-token-for (user)
  "Génère un Access Token JWT pour un utilisateur donné (alist)."
  (let* ((uid       (lumen.utils:alist-get user :id))
         (role      (lumen.utils:alist-get user :role))
         ;; Gestion souple de la clé tenant_id (snake_case ou kebab-case)
         (tid       (or (lumen.utils:alist-get user :tenant_id)
                        (lumen.utils:alist-get user :tenant-id)))
         (firstname (lumen.utils:alist-get user :firstname))
         (lastname  (lumen.utils:alist-get user :lastname))
         
         ;; Calcul des scopes
         ;; Pour un nouvel utilisateur (signup), on se base sur son rôle.
         ;; (Si l'user avait des scopes perso en DB, on les fusionnerait ici)
         (role-scopes (get-scopes-for-role role))
         
         ;; On s'assure que si c'est un admin, il a le scope admin
         (final-scopes (if (equal role "admin")
                           (adjoin "admin" role-scopes :test #'string=)
                           role-scopes)))

    ;; Appel au générateur Core
    (lumen.core.jwt:issue-access
     uid
     :role role
     :tenant tid
     :scopes final-scopes
     :claims `((:firstname . ,firstname)
               (:lastname  . ,lastname))
     ;; Durée de vie par défaut (ex: 2h ou défini dans config)
     :ttl (* 3600 2))))

(defun impersonate-user (req target-user-id)
  "Sauvegarde l'admin actuel et connecte le target."
  (format t "~&[IMPERSONATE USER] TARGET UID: ~A~%" target-user-id)
  (let ((admin-id (current-user-id req)))
    (format t "~&[IMPERSONATE USER] ADMIN ID: ~A~%" admin-id)
    ;; On stocke l'admin ID dans une clé spéciale
    (session-set! req *session-impersonator-key* admin-id)
    ;; On connecte le target comme si c'était lui
    (login-user req target-user-id)))

(defun stop-impersonation (req)
  "Restaure la session admin."
  (let ((admin-id (session-get req *session-impersonator-key*)))
    (when admin-id
      (login-user req admin-id)
      (session-del! req *session-impersonator-key*)
      t)))

(defun is-impersonating-p (req)
  (not (null (session-get req *session-impersonator-key*))))
