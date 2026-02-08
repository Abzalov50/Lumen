(defpackage :lumen.modules.landing
  (:use :cl :spinneret :lumen.utils :lumen.core.http :lumen.view.html
   :lumen.core.middleware :lumen.data.db :lumen.data.dao
   :lumen.data.repo.core :lumen.data.repo.query :lumen.core.auth)
  (:import-from :lumen.http.crud :make-entity-crud-guard)
  (:import-from :lumen.modules.socle :user)
  (:import-from :lumen.dev.module #:defmodule)
  (:export ))

(in-package :lumen.modules.landing)

(defmodule :landing
  :path-prefix "/"
  :doc "Page d'accueil par défaut avec navigation contextuelle (User/Admin)."

  ;; Pas d'entités propres, juste de l'affichage
  
  :routes
  ((GET "/" (req)
     (lumen.core.http:respond-html
      (render-landing-page req)))))

;; --- VUE (Localisée dans le même fichier pour la simplicité du module par défaut) ---

(defun get-current-user-details (req)
  "Récupère les infos de l'utilisateur connecté via la session ou la DB."
  (let* ((uid (lumen.modules.auth.service:current-user-id req))
	(tid (ctx-get req :tenant-id))
	(ctx (list :tenant-id tid)))
    (when uid
      ;; On utilise le repo pour récupérer les infos fraîches (Nom, Rôle...)
      (lumen.data.db:ensure-connection
        (lumen.data.repo.core:repo-show 'user ctx uid)))))

(defun render-navbar (req user)
  "Génère le HTML de la barre de navigation avec gestion de l'impersonation."
  (with-html-string
    
    ;; --- 1. BANNIÈRE D'AVERTISSEMENT (Impersonation) ---
    (when (lumen.modules.auth.service:is-impersonating-p req)
      (:div :class "bg-warning text-dark text-center py-2 fw-bold sticky-top border-bottom border-dark"
            :style "z-index: 1050;" ;; Toujours au-dessus
            (:div :class "container d-flex justify-content-center align-items-center"
              (:i :class "bi bi-exclamation-triangle-fill me-2")
              (:span "MODE IMPERSONATION ACTIF : Vous agissez en tant qu'un autre utilisateur.")
              (:button :class "btn btn-sm btn-dark ms-3" 
                       :hx-post "/auth/stop-impersonation" 
                       ;; Important : On recharge la page après le stop pour rétablir la session Admin
                       :hx-on--after-request "window.location.reload()" 
                       "Revenir à mon compte"))))

    ;; --- 2. BARRE DE NAVIGATION STANDARD ---
    (:nav :class "navbar navbar-expand-lg navbar-light bg-white border-bottom shadow-sm px-4"
      (:div :class "container-fluid"
        ;; Logo
        (:a :class "navbar-brand fw-bold text-primary" :href "/" 
            (:i :class "bi bi-lightning-charge-fill me-2") "Lumen App")

        ;; Toggle Mobile
        (:button :class "navbar-toggler" :type "button" :data-bs-toggle "collapse" :data-bs-target "#navbarContent"
          (:span :class "navbar-toggler-icon"))

        ;; Contenu Droite
        (:div :class "collapse navbar-collapse justify-content-end" :id "navbarContent"
          (:ul :class "navbar-nav mb-2 mb-lg-0 align-items-center"
            
            (if user
                ;; --- CAS CONNECTÉ ---
                (:li :class "nav-item dropdown"
                  (:a :class "nav-link dropdown-toggle d-flex align-items-center gap-2" 
                      :href "#" :id "userDropdown" :role "button" :data-bs-toggle "dropdown"
                    
                    ;; Avatar (Initiale)
                    (:div :class "rounded-circle bg-primary text-white d-flex align-items-center justify-content-center"
                          :style "width: 32px; height: 32px; font-size: 14px;"
                          (subseq (lumen.utils:alist-get user :firstname) 0 1))
                    
                    ;; Nom complet
                    (:span :class "fw-medium"
                           (format nil "~A ~A" 
                                   (lumen.utils:alist-get user :firstname) 
                                   (lumen.utils:alist-get user :lastname))))
                  
                  (:ul :class "dropdown-menu dropdown-menu-end shadow border-0 mt-2" :aria-labelledby "userDropdown"
                    ;; Header Role
                    (:li :class "dropdown-header" 
                         (format nil "Connecté en tant que ~A" (lumen.utils:alist-get user :role)))
                    
                    ;; Lien Admin
                    (when (equal (lumen.utils:alist-get user :role) "admin")
                      (:li (:a :class "dropdown-item" :href "/admin" 
                               (:i :class "bi bi-shield-lock me-2") "Administration")))
                    
                    (:li (:a :class "dropdown-item" :href "/auth/me" 
                             (:i :class "bi bi-person me-2") "Mon Profil"))
                    
                    (:li (:hr :class "dropdown-divider"))
                    
                    ;; Logout
                    (:li (:button :class "dropdown-item text-danger" 
                                  :hx-post "/auth/logout"
                                  :hx-target "body" 
                                  (:i :class "bi bi-box-arrow-right me-2") "Se déconnecter"))))
                
                ;; --- CAS NON CONNECTÉ ---
                (:li :class "nav-item"
                  (:a :class "btn btn-outline-primary" :href "/auth/login" "Se connecter")))))))))

(defun render-landing-page (req)
  "Assemble la page complète."
  (let* ((user (get-current-user-details req))
         (title "Bienvenue sur Lumen"))
    (format t "~&[LANDING PAGE] USER: ~A~%" user)
    (with-html-string
      (:doctype)
      (:html :lang "fr"
        (:head
          (:meta :charset "utf-8")
          (:title title)
          (lumen.view.html:render-core-assets)) ;; Bootstrap + HTMX + Icons
        
        (:body :class "bg-light" :style "min-height: 100vh; display: flex; flex-direction: column;"
          
          ;; Injection de la Navbar
          (:raw (render-navbar req user))
          
          ;; Contenu Principal (Hero)
          (:main :class "flex-grow-1 container d-flex align-items-center justify-content-center"
            (:div :class "text-center"
              (:h1 :class "display-4 fw-bold mb-3" "Développez plus vite.")
              (:p :class "lead text-muted mb-4" 
                  "Votre application Lumen tourne correctement.")
              
              (if user
                  (:div :class "d-flex gap-2 justify-content-center"
                    (:a :class "btn btn-primary btn-lg" :href "/dashboard" "Accéder au Dashboard")
                    ;; Si admin, un raccourci direct vers la gestion des utilisateurs
                    (when (equal (lumen.utils:alist-get user :role) "admin")
                      (:a :class "btn btn-outline-secondary btn-lg" :href "/api/socle/users" "Gérer les Utilisateurs")))
                  
                  ;; Si pas connecté
                  (:div 
                    (:a :class "btn btn-primary btn-lg px-5" :href "/auth/login" "Commencer")))))
          
          ;; Footer minimal
          (:footer :class "py-3 text-center text-muted border-top bg-white"
            (:small "Powered by Lumen Framework"))
          
          (lumen.view.html:render-core-js))))))
