(defpackage :lumen.modules.auth.module
  (:use :cl :lumen.core.router :lumen.utils :lumen.core.http
   :lumen.core.middleware :lumen.data.db :lumen.data.dao
   :lumen.data.repo.core :lumen.data.repo.query :lumen.modules.auth.service)
  (:import-from :lumen.http.crud :make-entity-crud-guard)
  (:import-from :lumen.modules.socle :tenant :tenant-domain :user)
  (:import-from :lumen.dev.module #:defmodule)
  (:import-from :lumen.modules.auth.view :render-login-page :render-signup-page
		:render-signup-form :render-signup-page)
  (:export ))

(in-package :lumen.modules.auth.module)

(defmodule :auth-test
  :path-prefix "/auth"
  :doc "Module d'authentification Core (JWT + Cookie + HTMX)"
  
  :routes
  (;; 1. PAGE DE LOGIN (GET)
   (GET "/login" (req)
	:summary "Affiche le formulaire de connexion"
	(lumen.core.http:respond-html
	 (lumen.modules.auth.view:render-login-page :title "Connexion - Lumen")))

   ;; 2. ACTION DE LOGIN (POST HTMX)
   (POST "/login" (req)
	 :summary "Traite l'authentification"
	 (let* ((form (lumen.core.http:ctx-get req :form))
		(email (alist-get form "email"))
		(pass  (alist-get form "password"))
		;; Récupération Tenant Contextuel (via Middleware Resolve-Tenant)
		(current-tid (lumen.core.http:ctx-get req :tenant-id)))
	   (format t "~&[LOGIN USER] CURRENT TENANT ID: ~A~%" current-tid)
	   ;; Authentification (Service)
	   (multiple-value-bind (user error-msg)
               (lumen.modules.auth.service:authenticate-user email pass current-tid)
         
             (if user
		 ;; SUCCÈS : On passe 'req' pour écrire en session
		 (lumen.modules.auth.service:respond-success req user "/" :msg "Connexion réussie.")
             
		 ;; ÉCHEC
		 (lumen.modules.auth.service:respond-error form error-msg)))))

   ;; 3. LOGOUT
   (POST "/logout" (req)
	 (lumen.modules.auth.service:logout-user req)
	 (let ((resp (lumen.core.http:respond-html "")))
	   ;; Redirection HTMX vers Login
	   (setf (lumen.core.http:resp-headers resp)
		 (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) 
                                            "HX-Location" "/auth/login"))
	   resp))

   ;; --- 2. SIGNUP (Contextualisé) ---
   ;; Cas A : "Je veux créer mon compte employé sur 'acme.lumen.app'"
   ;; Cas B : "Je veux créer ma nouvelle société sur 'www.lumen.app'"
   
   (GET "/signup" (req)
	(lumen.core.http:respond-html (lumen.modules.auth.view:render-signup-page)))

   (POST "/signup" (req)
	 (let* ((form (lumen.core.http:ctx-get req :form))
		;; On récupère le tenant résolu par le middleware (ex: si on est sur acme.lvh.me)
		(current-tid (lumen.core.http:ctx-get req :tenant-id)))
    
	   (if current-tid
               ;; --- CAS A : Inscription MEMBRE (Tenant connu) ---
               ;; On est déjà sur le bon sous-domaine.
               ;; register-member retourne (user token error) -> 3 valeurs
               (multiple-value-bind (user token err)
		   (register-member current-tid form)
          
		 (if user 
		     ;; Succès : On reste sur le même domaine, redirection locale vers Dashboard
		     (lumen.modules.auth.service:respond-success req user "/" 
								 :msg "Inscription réussie ! Bienvenue dans l'équipe.")
              
		     ;; Erreur
		     (lumen.modules.auth.service:respond-error form err)))
        
               ;; --- CAS B : Inscription NOUVEAU TENANT (SaaS) ---
               ;; On est sur le domaine racine (www ou root).
               ;; register-tenant-and-admin retourne (user token err redirect-url) -> 4 valeurs
               (multiple-value-bind (user token err redirect-url)
		   (register-tenant-and-admin form)
		 
		 (if user
		     ;; Succès : On redirige vers l'URL absolue (http://nouveau-tenant.lvh.me:8080/)
		     ;; Note : C'est ici que la magie du "redirect-url" opère
		     (lumen.modules.auth.service:respond-success req user redirect-url 
								 :msg "Votre espace a été créé avec succès. Redirection...")
              
		     ;; Erreur
		     (lumen.modules.auth.service:respond-error form err))))))

   (POST "/stop-impersonation" (req)
	 ;; 1. On appelle la logique de session pour rétablir l'identité
	 (lumen.modules.auth.service:stop-impersonation req)

	 ;; 2. Réponse
	 ;; Comme votre bouton a l'attribut :hx-on--after-request "window.location.reload()",
	 ;; une réponse 200 OK simple suffit. Le navigateur rechargera la page,
	 ;; et le backend générera la navbar "Admin" au lieu de "Impersonation".
	 (lumen.core.http:respond-json '((:success . t) (:msg . "Session restaurée"))))))

;; --- LOGIQUE D'ENREGISTREMENT (Service) ---

(defun register-member (tid form)
  "Crée un utilisateur simple dans le tenant courant."
  (let ((email (alist-get form "email"))
        (pass  (alist-get form "password")))
    (lumen.data.repo.core:repo-create 
     'user 
     (list :tenant-id tid) ;; Contexte Tenant forcé
     `((:email . ,email) 
       (:password . ,pass)
       (:firstname . ,(alist-get form "firstname"))
       (:lastname . ,(alist-get form "lastname"))
       (:role . "user"))))) ;; Rôle par défaut

(defun register-tenant-and-admin (form)
  "Crée toute la structure pour un nouveau client SaaS."
  (lumen.data.db:run-in-transaction
   (lambda ()
     (let* ((company (alist-get form "company"))
	    (code (slugify company))
	    (root-domain (lumen.core.config:cfg-get "ROOT_DOMAIN" :default "lvh.me"))
	    (port (lumen.app.app:app-port lumen.core.context:*current-app*))
	    (subdom (format nil "~A.~A" code root-domain))
	    (full-host (format nil "~A:~A" subdom port))
	    (redirect-url (format nil "http://~A/" full-host))
            ;; 1. Tenant
            ;;(tenant (lumen.data.repo.core:repo-create 
            ;;         'tenant nil `((:name . ,company) (:code . ,(slugify company)))))
	    (tid (pomo:query "INSERT INTO tenants(name, code) VALUES($1, $2) RETURNING id" company code :single))
	    (ctx (list :tenant-id tid)))
       (format t "~&[REGISTER TENANT] ID: ~A | CODE: ~A~% | FULL HOST: ~A~% | REDIRECT TO: ~A~%" tid code full-host redirect-url)
       ;;(error "OK")
       ;; 2. User Admin
       (let ((user (lumen.data.repo.core:repo-create
                    'user
		    ctx
                    `((:email . ,(alist-get form "email"))
                      (:password . ,(alist-get form "password"))
                      (:firstname . ,(alist-get form "firstname"))
                      (:lastname . ,(alist-get form "lastname"))
                      (:role . "admin")
		      (:scopes . ,(lumen.modules.auth.service:get-scopes-for-role
				   "admin"))))))
         
         ;; 3. Domaine (Optionnel : sous-domaine auto)
         (lumen.data.repo.core:repo-create
          'tenant-domain nil
          `((:tenant_id . ,tid)
            (:host . ,subdom))) ;; Ex: acme.localhost

         ;; Retourne user et token
         (values user
		 (lumen.modules.auth.service:issue-token-for user)
		 nil
		 redirect-url))))))
