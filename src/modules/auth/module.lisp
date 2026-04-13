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
	 (let* ((form (ctx-get req :form))
		(email (alist-get form "email"))
		(pass  (alist-get form "password"))
		(current-tid (ctx-get req :tenant-id)))

	   (multiple-value-bind (user error-msg)
               (lumen.modules.auth.service:verify-credentials email pass current-tid)
         
             (if user
		 ;; Succès : On déclenche HTMX !
		 (lumen.modules.auth.service:respond-success req user 
                                                             (lumen.app.app:app-path "/") 
                                                             :msg "Connexion réussie.")
		 ;; Erreur
		 (lumen.modules.auth.service:respond-error
		  form error-msg
		  :render-fn #'lumen.modules.auth.view:render-login-form)))))
   
   (POST "/api/v1/login" (req)
	 ;; Appel la version qui génère le token
	 (let* ((form (ctx-get req :form))
		(email (alist-get form "email"))
		(pass  (alist-get form "password"))
		(current-tid (ctx-get req :tenant-id)))
	   (multiple-value-bind (user token)
               (authenticate-for-api email pass current-tid)
             (respond-json `((:token . ,token))))))

   ;; 3. LOGOUT
   (POST "/logout" (req)
	 (lumen.modules.auth.service:logout-user req)
	 (let ((resp (lumen.core.http:respond-html "")))
	   ;; --- REFACTORISÉ : HX-Location dynamique ---
	   (setf (lumen.core.http:resp-headers resp)
		 (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) 
					    "HX-Location" 
					    (lumen.app.app:app-path "/auth/login")))
	   resp))

   ;; --- 2. SIGNUP (Contextualisé) ---
   (GET "/signup" (req)
	(lumen.core.http:respond-html (lumen.modules.auth.view:render-signup-page)))

   (POST "/signup" (req)
	 (let* ((form (lumen.core.http:ctx-get req :form))
		(current-tid (lumen.core.http:ctx-get req :tenant-id)))
    
	   (if current-tid
               ;; --- CAS A : Inscription MEMBRE (Tenant connu) ---
               (multiple-value-bind (user token err)
		   (register-member current-tid form)
          
		 (if user 
		     ;; --- REFACTORISÉ : Redirection locale ---
		     (lumen.modules.auth.service:respond-success req user 
								 (lumen.app.app:app-path "/") 
								 :msg "Inscription réussie ! Bienvenue dans l'équipe.")
              
		     (lumen.modules.auth.service:respond-error form err)))
        
               ;; --- CAS B : Inscription NOUVEAU TENANT (SaaS) ---
               (multiple-value-bind (user token err redirect-url)
		   (register-tenant-and-admin form)
         
		 (if user
		     ;; --- PAS DE CHANGEMENT ICI ---
		     ;; redirect-url est déjà une URL HTTP absolue (http://subdom.root/), 
		     ;; donc on ne doit PAS utiliser app-path dessus !
		     (lumen.modules.auth.service:respond-success req user redirect-url 
								 :msg "Votre espace a été créé avec succès. Redirection...")
              
		     (lumen.modules.auth.service:respond-error form err))))))

   (POST "/stop-impersonation" (req)
	 (lumen.modules.auth.service:stop-impersonation req)
	 (lumen.core.http:respond-json '((:success . t) (:msg . "Session restaurée"))))))

;; --- LOGIQUE D'ENREGISTREMENT (Service) ---

(defun register-member (tid form)
  "Crée un utilisateur simple dans le tenant courant."
  (let* ((email (alist-get form "email"))
         (pass  (alist-get form "password"))
	 (user (lumen.data.repo.core:repo-create 
		'user 
		(list :tenant-id tid) ;; Contexte Tenant forcé
		`((:email . ,email) 
		  (:password . ,pass)
		  (:firstname . ,(alist-get form "firstname"))
		  (:lastname . ,(alist-get form "lastname"))
		  (:role . "user")))))
    (values user
	    (lumen.modules.auth.service:issue-token-for user)
	    nil)))

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
