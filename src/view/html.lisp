(defpackage :lumen.view.html
  (:use :cl :spinneret)
  (:import-from :lumen.core.http :*request* :req-headers)
  (:export :def-layout :with-layout :render-page :htmx-request-p :htmx-target
	   :render-core-assets :render-core-js))

(in-package :lumen.view.html)

;; Configuration de Spinneret pour le HTML5
(defparameter *default-doctype* "!DOCTYPE html")

;;; 1. Helper HTMX

(defun htmx-request-p (&optional (req lumen.core.http:*request*))
  "Retourne T si la requête vient de HTMX (Header HX-Request)."
  (and req 
       (cdr (assoc "hx-request" (req-headers req) :test #'string-equal))))

(defun htmx-target (&optional (req lumen.core.http:*request*))
  (cdr (assoc :hx-target (lumen.core.http:req-headers req) :test #'eq)))

;;; 2. Gestion des Assets (Remplacement de vos %tag-js/css)

(defun render-core-assets ()
  "Rend uniquement les dépendances front communes à Lumen.

Les feuilles de style et scripts propres à une application doivent être
déclarés dans le layout de cette application."
  (with-html
    (:link :rel "stylesheet"
           :href "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css")

    (:link :rel "stylesheet"
           :href "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.0/font/bootstrap-icons.css")

    (:script :src "https://unpkg.com/htmx.org@1.9.10"
             :defer t)

    (:script :src "https://unpkg.com/htmx.org/dist/ext/json-enc.js"
             :defer t)))

(defun render-core-js ()
  "Rend uniquement les scripts communs à Lumen."
  (with-html
    (:script
     :src "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js")))

;;; 3. Layout Master
;;; Cette macro est magique : elle gère le "Full page" vs "Partial"

(defmacro with-layout ((&key (title "Lumen App") (active-nav :home)) &body body)
  `(if (and (htmx-request-p) (string= (htmx-target) "entity-table-container"))
       ;; Cas HTMX : On renvoie juste le contenu (le fragment)
       ;; On garde juste le titre pour que le navigateur mette à jour l'onglet
       (with-html-string
         (:title ,title)
         ,@body)
       
       ;; Cas Normal : On renvoie toute la structure HTML
       (with-html-string
         (:doctype)
         (:html :lang "fr"
		(:head
		 (:meta :charset "utf-8")
		 (:meta :name "viewport" :content "width=device-width, initial-scale=1")
		 (:title ,title)
		 (render-core-assets)
		 ;; --- AJOUT : Script global de fermeture de modale ---
		 ;; Plus de quotes, plus d'échappement, plus de bugs.
		 (:script (:raw "
              function closeModal() {
                  document.getElementById('modal-container').innerHTML = '';
              }
              // Fermeture via la touche Echap
              document.addEventListener('keydown', function(e) {
                  if (e.key === 'Escape') closeModal();
              });
            ")))
		(:body :hx-boost "true" :class "bg-light"
		       ;;:hx-target "#main-content" :hx-select "#main-content" 
		       ;; Barre de navigation standard
		       (:nav :class "navbar navbar-expand-lg navbar-dark bg-dark mb-4"
			     (:div :class "container-fluid"
				   (:a :class "navbar-brand" :href "/" "Lumen 2.0")
				   ;; ... Menu généré dynamiquement selon active-nav ...
				   ))
             
		       ;; Conteneur principal qui sera remplacé par HTMX
		       (:main :id "main-content" :class "container"
			      ,@body)
		       ;; "Cible" pour les modales. HTMX va remplir cette div vide.
		       (:div :id "modal-container")
		       (:div :id "toast-container" :style "z-index: 1055;"
			     :class "toast-container position-fixed bottom-0 end-0 p-3")
		       ;; Footer
		       (:footer :class "text-center py-3 text-muted"
				(:small "Powered by Common Lisp & HTMX"))
		       (render-core-js))))))
