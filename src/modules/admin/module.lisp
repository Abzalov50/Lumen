(defpackage :lumen.admin.module
  (:use :cl :lumen.utils :lumen.core.http
   :lumen.core.middleware :lumen.data.db :lumen.data.dao
   :lumen.data.repo.core :lumen.data.repo.query)
  (:import-from :lumen.dev.module #:defmodule)
  (:export :table-to-entity))

(in-package :lumen.admin.module)

(defmodule :admin
  :path-prefix "/admin"
  :doc "Couteau suisse d'administration."
  
  :routes
  (;; Dashboard
   (GET "/" (req)
	:roles '("admin")
	(let ((stats (lumen.admin.dashboard:get-kpis)))
	  (lumen.core.http:respond-html 
           (lumen.admin.view:render-dashboard req stats))))
   
   ;; Route générique : /admin/list/:entity
   (GET "/list/:entity" (req entity-str)
	(let* ((entity-sym (lumen.admin.utils:resolve-entity-symbol entity-str))
               ;; Parsing des Query Params
               (params     (lumen.core.http:req-query req))
               (page       (or (parse-integer (or (cdr (assoc "page" params :test #'string=)) "1") :junk-allowed t) 1))
               (search     (cdr (assoc "search" params :test #'string=)))
               (sort-str   (cdr (assoc "sort" params :test #'string=)))
               (sort-col   (when (and sort-str (> (length sort-str) 0)) 
                             (intern (string-upcase sort-str) :keyword)))
               (dir-str    (cdr (assoc "dir" params :test #'string=)))
               (sort-dir   (if (equal dir-str "desc") :desc :asc)))
       
	  ;; Fetch Data
	  (multiple-value-bind (items total)
              (lumen.admin.grid:fetch-grid-data
	       req entity-sym :page page :sort-col sort-col
			  :sort-dir sort-dir :search search)
         
            (let ((grid-html (lumen.admin.view:render-data-grid 
                              entity-sym items total page 20 sort-col sort-dir search)))
           
              (if (lumen.view.html:htmx-request-p req)
		  ;; CAS HTMX : On renvoie juste le tableau (swap)
		  (lumen.core.http:respond-html grid-html)
               
		  ;; CAS NORMAL : Page complète
		  (lumen.core.http:respond-html
                   (lumen.admin.view:render-admin-layout 
                    req 
                    :title (format nil "Gestion : ~A" (string-capitalize entity-str))
                    :content (format nil "<div id='admin-grid'>~A</div>" grid-html))))))))

   ;; --- CREATE (GET) ---
   (GET "/create/:entity" (req entity-str)
	(let ((entity-sym (lumen.admin.utils:resolve-entity-symbol entity-str))) ;; <--- CORRECTION
	  (lumen.core.http:respond-html
           (lumen.admin.view:render-admin-layout
	    req 
	    :title (format nil "Nouveau ~A" entity-str)
	    :content (lumen.admin.view:render-entity-form entity-sym nil nil)))))

   ;; --- CREATE (POST) ---
   (POST "/create/:entity" (req entity-str)
	 (let* ((entity-sym (lumen.admin.utils:resolve-entity-symbol entity-str))
		(ctx (ctx-from-req req))
		(form-data (getf ctx :fields))
		(clean-data (lumen.admin.utils:normalize-post-params entity-sym form-data)))
       
	   (handler-case
               (progn
		 (lumen.data.repo.core:repo-create entity-sym ctx clean-data)
		 (respond-ok (format nil "/admin/list/~A" entity-str)))
             (error (e)
               (lumen.core.http:respond-html
		(lumen.admin.view:render-admin-layout
		 req 
		 :title "Erreur Création"
		 :content (lumen.admin.view:render-entity-form entity-sym clean-data (princ-to-string e))))))))

   ;; --- EDIT (GET) ---
   (GET "/edit/:entity/:id" (req entity-str id)
	(let* ((entity-sym (lumen.admin.utils:resolve-entity-symbol entity-str))
	       (ctx (ctx-from-req req))
            ;; On récupère l'enregistrement
            (record (lumen.data.repo.core:repo-show entity-sym ctx id)))
       (if record
           (lumen.core.http:respond-html
            (lumen.admin.view:render-admin-layout req 
              :title (format nil "Édition ~A" entity-str)
              :content (lumen.admin.view:render-entity-form entity-sym record nil)))
           (lumen.core.http:respond-404))))

   ;; --- EDIT (POST) ---
   (POST "/edit/:entity/:id" (req entity-str id)
	 (let* ((entity-sym (lumen.admin.utils:resolve-entity-symbol entity-str))
		(ctx (ctx-from-req req))
		(form-data (getf ctx :fields))
		(clean-data (lumen.admin.utils:normalize-post-params entity-sym form-data))
		)
	   (format t "~&[ADMIN ENT. EDIT] ENTITY: ~A | FORM DATA: ~A~%" entity-sym form-data)
	   (format t "~&[ADMIN ENT. EDIT] CLEAN DATA: ~A~%" form-data)
       
	   (handler-case
               (progn
		 ;; Si c'est un User et que password est vide, on le retire pour ne pas l'écraser
		 (when (string= entity-str "user")
                   (unless (and (assoc "password" clean-data :test #'string=)
				(plusp (length (cdr (assoc "password" clean-data :test #'string=)))))
                     (setf clean-data (remove "password" clean-data :key #'car :test #'string=))))

		 (lumen.data.repo.core:repo-patch entity-sym ctx id clean-data)

		 (print "NNNNNNN")
		 (respond-ok (format nil "/admin/list/~A" entity-str))
		 )
             (error (e)
	       (print "LLLLLLL")
               (lumen.core.http:respond-html
		(lumen.admin.view:render-admin-layout
		 req 
		 :title "Erreur Édition"
		 ;; On fusionne les données postées avec l'ID pour réafficher
		 :content (lumen.admin.view:render-entity-form 
			   entity-sym (acons :id id clean-data)
			   (princ-to-string e))))))))

   ;; --- DELETE ---
   (DELETE "/delete/:entity/:id" (req entity-str id)
	   (let* ((entity-sym (lumen.admin.utils:resolve-entity-symbol entity-str))
		  (ctx (ctx-from-req req)))
             (lumen.data.repo.core:repo-delete entity-sym ctx id)
             (respond-ok (format nil "/admin/list/~A" entity-str) :msg "Élément supprimé.")))

   ;; --- BATCH ACTIONS ---
   (POST "/action/:entity" (req entity-str)
	 (let* ((entity-sym (lumen.admin.utils:resolve-entity-symbol entity-str))
		(ctx (ctx-from-req req))
		(x (print ctx))
		(form (getf ctx :form))
		(action (intern (string-upcase (alist-get form "action")) :keyword))
		(x (print form))
		(x (print action))
		;; Les checkboxes multiples de même nom ("ids") arrivent souvent en liste
		;; Mais selon le parser, il faut peut-être collecter.
		;; Supposons que votre parser body gère les clés multiples en liste :
		(ids (lumen.utils:alist-get-all form "ids"))) 
       
	   (lumen.admin.actions:handle-batch-action req entity-sym action ids)))

   ;; --- DASHBOARD REFRESH (HTMX Polling optionnel) ---
   (GET "/kpis" (req)
     ;; Si vous voulez que les widgets se mettent à jour tout seuls
     (let ((stats (lumen.admin.dashboard:get-kpis)))
       ;; Renvoie juste le fragment HTML des cartes
       (lumen.core.http:respond-html 
         (lumen.admin.dashboard:render-kpi-cards stats))))

   ;; --- AUDIT LOG LISTE ---
   (GET "/audit" (req)
     (lumen.data.db:ensure-connection
       ;; Récupère les 50 derniers logs
       (let ((logs (lumen.data.db:query-a "SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT 50")))
         (lumen.core.http:respond-html
          (lumen.admin.audit:render-audit-view req logs)))))

   ;; --- AUDIT LOG DETAIL (HTMX Fragment) ---
   (GET "/audit/:id" (req id)
     (lumen.data.db:ensure-connection
       (let* ((log (first (lumen.data.db:query-a "SELECT diff FROM audit_logs WHERE id = $1" id)))
              (diff (alist-get log :diff)))
         (lumen.core.http:respond-html 
          (lumen.admin.audit:render-diff-detail diff)))))
   
   ;; --- IMPERSONATION ---
   (GET "/impersonate/:id" (req id)
      (lumen.modules.auth.service:impersonate-user req id)
      (respond-ok "/" :msg "Impersonation active."))

   ;; --- SQL RUNNER ---
   (GET "/sql" (req)
	:scopes '("read:sql-runner")
      (lumen.admin.tools:render-sql-console req))

   (POST "/sql" (req)
	 :scopes '("write:sql-runner")
      (let ((query (alist-get (lumen.core.http:ctx-get req :form) "query")))
        ;; TODO: Vérifier que l'user est SUPER ADMIN (rôle ou flag spécifique)
        
        ;; Safety check basique (Anti-Oups)
        (if (or (search "DROP" (string-upcase query))
                (search "TRUNCATE" (string-upcase query)))
            (lumen.admin.tools:render-sql-console req nil "Opérations destructives interdites via la console web.")
            
            (handler-case
                (let ((res (lumen.data.db:query-a query)))
                  (lumen.core.http:ctx-set! req :last-query query) ;; Pour le réafficher
                  (lumen.admin.tools:render-sql-console req res))
              (error (e)
                (lumen.core.http:ctx-set! req :last-query query)
                (lumen.admin.tools:render-sql-console req nil (princ-to-string e)))))))
   )
  )

(defun respond-ok (target-url &key (msg "Opération réussie"))
  (let ((resp (lumen.core.http:respond-html "")))
             
    ;; 1. Redirection HTMX
    (setf (lumen.core.http:resp-headers resp)
          (lumen.utils:ensure-header 
           (lumen.core.http:resp-headers resp) 
           "HX-Redirect" 
           target-url))
             
    ;; 2. Toast (Optionnel)
    (setf (lumen.core.http:resp-headers resp)
          (lumen.utils:ensure-header 
           (lumen.core.http:resp-headers resp) 
           "HX-Trigger" 
           (cl-json:encode-json-to-string 
            `((:show-message . ((:type . "success") (:message . ,msg)))))))
             
    resp))

;; Dans l'initialisation du module ou une fonction setup
(defun setup-default-kpis ()
  (lumen.admin.dashboard:register-kpi 
   :users "Utilisateurs" "bi-people" "primary"
   (lambda () (pomo:query "SELECT count(*) FROM users" :single)))

  (lumen.admin.dashboard:register-kpi 
   :tenants "Organisations" "bi-buildings" "info"
   (lambda () (pomo:query "SELECT count(*) FROM tenants" :single))))
   
;; Appelez ceci au chargement du module
(setup-default-kpis)
