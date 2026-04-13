(defpackage :lumen.modules.auth.view
  (:use :cl :spinneret :lumen.utils :lumen.core.http :lumen.data.db :lumen.data.dao
   :lumen.view.html :lumen.data.repo.core :lumen.data.repo.query
    )
  (:export :render-login-page :render-signup-page
        :render-signup-form :render-login-form)) ;; J'ai corrigé un doublon dans vos exports

(in-package :lumen.modules.auth.view)

(defun render-signup-form (&key values error)
  "Formulaire d'inscription HTMX."
  (let ((email     (alist-get values "email"))
        (firstname (alist-get values "firstname"))
        (lastname  (alist-get values "lastname"))
        (company   (alist-get values "company")))
    
    (with-html-string
      ;; --- REFACTORISÉ : hx-post dynamique ---
      (:form :hx-post (lumen.app.app:app-path "/auth/signup") 
             :hx-swap "outerHTML"
             :class "needs-validation"
        
        (:div :class "text-center mb-4"
          (:h1 :class "h3 mb-3 fw-normal" "Créer un compte")
          (:p :class "text-muted" "Démarrez votre essai gratuit de 14 jours."))

        ;; Alert Erreur
        (when error
          (:div :class "alert alert-danger" (:i :class "bi bi-exclamation-circle me-2") error))

        ;; Champs
        (:div :class "form-floating mb-3"
          (:input :type "text" :class "form-control" :name "company" :id "iComp" :placeholder "Société" :value company :required t)
          (:label :for "iComp" "Nom de l'organisation"))

        (:div :class "row g-2 mb-3"
          (:div :class "col-6 form-floating"
            (:input :type "text" :class "form-control" :name "firstname" :id "iFn" :placeholder "Prénom" :value firstname :required t)
            (:label :for "iFn" "Prénom"))
          (:div :class "col-6 form-floating"
            (:input :type "text" :class "form-control" :name "lastname" :id "iLn" :placeholder "Nom" :value lastname :required t)
            (:label :for "iLn" "Nom")))

        (:div :class "form-floating mb-3"
          (:input :type "email" :class "form-control" :name "email" :id "iEm" :placeholder "Email" :value email :required t)
          (:label :for "iEm" "Email professionnel"))

        (:div :class "form-floating mb-4"
          (:input :type "password" :class "form-control" :name "password" :id "iPw" :placeholder "Mot de passe" :required t :minlength "8")
          (:label :for "iPw" "Mot de passe (8+ caractères)"))

        (:button :class "w-100 btn btn-lg btn-success" :type "submit" 
                 (:i :class "bi bi-rocket-takeoff me-2") "Démarrer")
        
        (:p :class "mt-4 text-center"
          "Déjà un compte ? " 
          ;; --- REFACTORISÉ : href dynamique ---
          (:a :href (lumen.app.app:app-path "/auth/login") "Se connecter"))))))

(defun render-signup-page ()
  "Page wrapper."
  (with-html-string
    (:doctype)
    (:html
      (:head
        (:meta :charset "utf-8")
        (:title "Inscription - Lumen")
        (lumen.view.html:render-core-assets))
      (:body :class "bg-light d-flex align-items-center justify-content-center" 
             :style "height: 100vh;"
        (:main :class "form-signin w-100 m-auto shadow p-4 bg-white rounded" :style "max-width: 400px;"
          (:raw (render-signup-form)))
        (:div :id "toast-container" :class "toast-container position-fixed bottom-0 end-0 p-3")
        (lumen.view.html:render-core-js)))))

(defun render-login-form (&key email error)
  "Génère le fragment HTML du formulaire (pour affichage initial ou swap HTMX)."
  (with-html-string
    ;; --- REFACTORISÉ : hx-post dynamique ---
    (:form :hx-post (lumen.app.app:app-path "/auth/login") 
           :hx-swap "outerHTML"
           
      ;; Logo ou Titre
      (:div :class "text-center mb-4"
        (:h1 :class "h3 mb-3 fw-normal" "Connexion"))

      ;; Feedback Erreur
      (when error
        (:div :class "alert alert-danger d-flex align-items-center" :role "alert"
          (:i :class "bi bi-exclamation-triangle-fill me-2")
          (:div error)))

      ;; Email
      (:div :class "form-floating mb-3"
        (:input :type "email" :class "form-control" :id "floatingInput" 
                :name "email" :placeholder "name@example.com" :value email :required t)
        (:label :for "floatingInput" "Adresse Email"))

      ;; Mot de passe
      (:div :class "form-floating mb-3"
        (:input :type "password" :class "form-control" :id "floatingPassword" 
                :name "password" :placeholder "Password" :required t)
        (:label :for "floatingPassword" "Mot de passe"))

      ;; Actions
      (:button :class "w-100 btn btn-lg btn-primary" :type "submit" 
               (:i :class "bi bi-box-arrow-in-right me-2") "Se connecter")
      
      (:p :class "mt-5 mb-3 text-muted text-center" "© Lumen Framework"))))

(defun render-login-page (&key (title "Login"))
  "Génère la page complète (Layout minimaliste)."
  (with-html-string
    (:doctype)
    (:html
      (:head
        (:meta :charset "utf-8")
        (:title title)
        ;; On inclut les assets Core (Bootstrap, HTMX...)
        (lumen.view.html:render-core-assets))
      (:body :class "text-center bg-light d-flex align-items-center justify-content-center" 
             :style "height: 100vh; padding-top: 40px; padding-bottom: 40px;"
        
        ;; Conteneur centré
        (:main :class "form-signin w-100 m-auto" :style "max-width: 330px; padding: 15px;"
          ;; On injecte le formulaire initial
          (:raw (render-login-form)))
        
        ;; Conteneur Toast pour les notifications
        (:div :id "toast-container" :class "toast-container position-fixed bottom-0 end-0 p-3")
        (lumen.view.html:render-core-js)))))
