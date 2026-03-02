(defpackage :lumen.admin.view
  (:use :spinneret :common-lisp :lumen.view.html :lumen.utils)
  
  (:export :render-admin-layout :render-dashboard :render-data-grid
	   :render-entity-form))

(in-package :lumen.admin.view)

(defun render-entity-form (entity-sym record error-msg)
  "Affiche le formulaire Admin via le composant générique lumen.view.form."
  
  (let* ((entity-name (string-downcase entity-sym))
         (is-edit (not (null record)))
         ;; URLs
         (list-url   (format nil "/admin/list/~A" entity-name))
         (action-url (if is-edit 
                         (format nil "/admin/edit/~A/~A" entity-name (alist-get record :id))
                         (format nil "/admin/create/~A" entity-name)))
         (delete-url (when is-edit 
                       (format nil "/admin/delete/~A/~A" entity-name (alist-get record :id)))))
    
    (with-html-string
      (:div :class "container-fluid"
            
            ;; En-tête de page Admin
            (:div :class "d-flex align-items-center mb-4"
                  (:a :href list-url 
                      :class "btn btn-outline-secondary me-3"
                      (:i :class "bi bi-arrow-left"))
                  (:h2 :class "h4 mb-0" 
                       (if is-edit 
                           (format nil "Modifier ~A" (string-capitalize entity-name))
                           (format nil "Créer ~A" (string-capitalize entity-name)))))
        
            ;; Message d'erreur global (si retourné par le controller)
            (when error-msg
              (:div :class "alert alert-danger mb-4" 
                    (:i :class "bi bi-exclamation-triangle-fill me-2") error-msg))
        
            ;; Appel du Formulaire Générique
            (:raw 
             (lumen.view.form:render-entity-form 
              entity-sym
              :values record          ;; Les données (ou nil pour create)
              :action action-url      ;; URL de soumission
              :method :POST           ;; Toujours POST pour l'instant (HTMX gère le reste)
              :submit-text (if is-edit "Mettre à jour" "Créer")
              :cancel-url list-url    ;; Bouton annuler
              :delete-url delete-url  ;; Bouton supprimer (affiché seulement si non nil)
              
              ;; Options HTMX
              :hx-swap "none"         ;; Le serveur fera une redirection HX-Redirect
              ))))))

#|
(defun render-entity-form (entity-sym record error-msg)
  "Affiche le formulaire Création/Édition complet."
  (let* ((fields (lumen.admin.form:get-form-fields entity-sym record))
         (entity-name (string-downcase entity-sym))
         (is-edit (not (null record)))
         (action-url (if is-edit 
                         (format nil "/admin/edit/~A/~A" entity-name (alist-get record :id))
                         (format nil "/admin/create/~A" entity-name))))
    
    (with-html-string
      (:div :class "container-fluid"
            ;; En-tête avec bouton retour
            (:div :class "d-flex align-items-center mb-4"
		  (:a :href (format nil "/admin/list/~A" entity-name) 
		      :class "btn btn-outline-secondary me-3"
		      (:i :class "bi bi-arrow-left"))
		  (:h2 :class "h4 mb-0" 
		       (if is-edit 
			   (format nil "Modifier ~A" (string-capitalize entity-name))
			   (format nil "Créer ~A" (string-capitalize entity-name)))))
        
            ;; Alert Erreur
            (when error-msg
              (:div :class "alert alert-danger" 
                    (:i :class "bi bi-exclamation-triangle-fill me-2") error-msg))
        
            ;; Carte Formulaire
            (:div :class "card shadow-sm" :style "max-width: 800px;"
		  (:div :class "card-body p-4"
			(:form :method "POST"
			       :hx-post action-url
			       :hx-swap "none" ;; On attend une redirection, pas de remplacement de HTML
			       :enctype "multipart/form-data" ;; Prêt pour le futur upload de fichiers
              
			       ;; Rendu automatique des champs
			       (dolist (f fields)
				 (:raw (lumen.admin.form:render-widget f)))
              
			       (:div :class "d-flex justify-content-between align-items-center mt-5"
				     ;; Zone Danger (Delete)
				     (if is-edit
					 (:button :type "button" :class "btn btn-outline-danger"
						  :hx-delete (format nil "/admin/delete/~A/~A" entity-name (alist-get record :id))
						  :hx-confirm "Êtes-vous sûr de vouloir supprimer cet élément ?"
						  :hx-target "body" ;; Recharge toute la page pour revenir à la liste
						  (:i :class "bi bi-trash me-2") "Supprimer")
					 (:div)) ;; Spacer vide
                
				     ;; Bouton Save
				     (:button :type "submit" :class "btn btn-primary px-4"
					      (:i :class "bi bi-check-lg me-2") "Enregistrer")))))))))
|#

(defun render-data-grid (entity-sym items total page per-page sort-col sort-dir search)
  "Génère le tableau HTML + Pagination + Actions de masse (Batch)."
  (let* ((columns  (lumen.admin.grid:get-display-columns entity-sym))
         (actions  (lumen.admin.registry:get-actions entity-sym)) ;; On récupère les actions (Delete, CSV...)
	 (base-url (format nil "/admin/list/~A" (string-downcase entity-sym)))
         (action-url (format nil "/admin/action/~A" (string-downcase entity-sym))) ;; URL commune
         (total-pages (ceiling total per-page)))
    (print columns)
    
    (with-html-string
      
      ;; --- FORMULAIRE GLOBAL POUR LES ACTIONS DE MASSE ---
      (:form :id "batch-form"
             ;;:hx-post (format nil "/admin/action/~A" (string-downcase entity-sym))
             ;;:hx-target "#admin-grid"
             ;;:hx-swap "outerHTML" ;; On remplace toute la grille après l'action
	     :method "POST" 
             :action action-url ;; Fallback standard (pour l'export CSV)
        
        ;; 1. BARRE D'OUTILS (Recherche + Nouveau)
        (:div :class "d-flex justify-content-between align-items-center mb-3"
          (:div :class "input-group" :style "max-width: 300px;"
            (:span :class "input-group-text bg-white" (:i :class "bi bi-search"))
            (:input :type "text" :class "form-control" :placeholder "Rechercher..."
                    :name "search" :value search
                    ;; HTMX pour la recherche live
                    :hx-get base-url 
                    :hx-trigger "keyup changed delay:500ms" 
                    :hx-target "#admin-grid"
                    :hx-include "[name='sort'], [name='dir']")) 
          
          (:div
            (:a :href (format nil "/admin/create/~A" (string-downcase entity-sym)) 
                :class "btn btn-primary"
                (:i :class "bi bi-plus-lg me-2") "Nouveau")))

        ;; 2. LE TABLEAU
        (:div :class "card shadow-sm mb-5" ;; Marge en bas pour ne pas cacher la pagination avec la barre d'action
          (:div :class "table-responsive"
            (:table :class "table table-hover table-striped align-middle mb-0"
              (:thead :class "table-light"
                (:tr
                  ;; A. CHECKBOX HEADER (SELECT ALL)
                  (:th :width "40" :class "text-center"
                       (:input :class "form-check-input" :type "checkbox" 
                               :onclick "toggleAll(this)"))
                  
                  ;; B. COLONNES TRIABLES
                  (dolist (col columns)
                    (let* ((col-key (first col))
                           (is-sorted (eq col-key sort-col))
                           (next-dir (if (and is-sorted (eq sort-dir :asc)) :desc :asc))
                           (icon (cond ((not is-sorted) "bi-arrow-down-up text-muted opacity-25")
                                       ((eq sort-dir :asc) "bi-sort-down-alt text-primary")
                                       (t "bi-sort-up text-primary"))))
                      
                      (:th :style "cursor: pointer; white-space: nowrap;"
                           :hx-get (format nil "~A?page=~A&sort=~A&dir=~A&search=~A" 
                                           base-url page (string-downcase col-key) next-dir (or search ""))
                           :hx-target "#admin-grid"
                           (string-capitalize (string-downcase col-key))
                           (:i :class (format nil "bi ~A ms-1 small" icon)))))))
              
              (:tbody
                (if items
                    (dolist (row items)
                      (:tr :style "cursor: pointer;"
                           ;; Clic sur la ligne -> Édition
                           :onclick (format nil "window.location='/admin/edit/~A/~A'" 
                                            (string-downcase entity-sym) (lumen.utils:alist-get row :id))
                        
                        ;; C. CHECKBOX LIGNE
                        (:td :class "text-center"
                             (:input :class "form-check-input row-select" :type "checkbox" 
                                     :name "ids" :value (lumen.utils:alist-get row :id)
                                     ;; IMPORTANT: stopPropagation évite de déclencher le onclick de la ligne (redirect)
                                     :onclick "event.stopPropagation(); updateBatchBar()"))
                        
                        ;; D. CELLULES
                        (dolist (col columns)
                          (:td (:raw (lumen.admin.grid:render-cell-content 
                                      entity-sym (first col) 
                                      (lumen.utils:lookup row (first col)) 
                                      row))))))
                    
                    ;; CAS VIDE
                    (:tr (:td :colspan (1+ (length columns)) :class "text-center py-5 text-muted" 
                              (:i :class "bi bi-inbox fs-1 d-block mb-2") "Aucune donnée trouvée"))))))
        
        ;; 3. PAGINATION
        (when (> total-pages 1)
          (:nav :class "mt-3 d-flex justify-content-between align-items-center"
            (:small :class "text-muted" (format nil "Affichage ~A - ~A sur ~A" 
                                                (1+ (* (1- page) per-page)) 
                                                (min total (* page per-page)) 
                                                total))
            (:ul :class "pagination mb-0"
              (:li :class (if (= page 1) "page-item disabled" "page-item")
                   (:button :class "page-link" 
                            :hx-get (format nil "~A?page=~A&sort=~A&dir=~A&search=~A" 
                                            base-url (1- page) (or sort-col "") (or sort-dir "") (or search ""))
                            :hx-target "#admin-grid"
                            (:i :class "bi bi-chevron-left")))
              (:li :class "page-item active" (:span :class "page-link" page))
              (:li :class (if (>= page total-pages) "page-item disabled" "page-item")
                   (:button :class "page-link" 
                            :hx-get (format nil "~A?page=~A&sort=~A&dir=~A&search=~A" 
                                            base-url (1+ page) (or sort-col "") (or sort-dir "") (or search ""))
                            :hx-target "#admin-grid"
                            (:i :class "bi bi-chevron-right"))))))

        ;; 4. BARRE D'ACTIONS FLOTTANTE (Batch Bar)
        (:div :id "batch-bar" 
              :class "fixed-bottom bg-white shadow-lg p-3 border-top d-none"
              :style "left: 280px; z-index: 1000; transition: transform 0.3s ease-in-out;"
          (:div :class "container-fluid d-flex justify-content-between align-items-center"
            
            ;; Compteur
            (:div :class "d-flex align-items-center"
              (:span :class "badge bg-dark me-2" :id "selected-count" "0")
              (:span :class "fw-bold text-muted" "élément(s) sélectionné(s)"))
            
            ;; Boutons d'actions
            (:div :class "d-flex gap-2"
              (dolist (act actions)
                (let* ((act-key (first act))
                      (act-lbl (second act))
                      (act-danger (getf (cddr act) :danger))
                      (act-icon (getf (cddr act) :icon))
		      ;; On détecte si c'est un export CSV pour désactiver HTMX
                      (is-export (eq act-key :export-csv)))
                  (:button :type "submit" :name "action" :value (string-downcase act-key)
                           :class (if act-danger "btn btn-danger" "btn btn-outline-secondary")
			   ;; A. CAS EXPORT CSV : PAS D'ATTRIBUTS HTMX
                           ;; Le navigateur fera un POST standard -> Le serveur renvoie le fichier -> Le navigateur télécharge.
                           
                           ;; B. CAS DELETE / AUTRE : ON FORCE HTMX SUR LE BOUTON
                           ;; Note: Quand un bouton HTMX est dans un form, il inclut auto les données du form.
			   (unless is-export
                             (list :hx-post action-url
                                   :hx-target "#admin-grid"
                                   ;; On confirme via HTMX ou JS natif
                                   :hx-confirm (when act-danger "Êtes-vous sûr de vouloir supprimer ces éléments ?")
                                   :hx-swap "outerHTML"))
                           ;; Confirmation JS pour les actions dangereuses
                           ;;:onclick (when act-danger "return confirm('Êtes-vous sûr ? Cette action est irréversible.');")
                           (when act-icon (:i :class (format nil "bi ~A me-2" act-icon)))
                           (when (and act-danger (not act-icon)) (:i :class "bi bi-trash me-2"))
                           act-lbl)))))))

      ;; 5. JAVASCRIPT UI (Intégré pour l'interactivité immédiate)
      (:script (:raw "
        // Fonction pour cocher/décocher tout
        function toggleAll(source) {
          document.querySelectorAll('.row-select').forEach(c => c.checked = source.checked);
          updateBatchBar();
        }

        // Fonction pour mettre à jour la barre flottante
        function updateBatchBar() {
          let n = document.querySelectorAll('.row-select:checked').length;
          document.getElementById('selected-count').innerText = n;
          let bar = document.getElementById('batch-bar');
          
          if(n > 0) {
            bar.classList.remove('d-none');
            bar.classList.add('animate__fadeInUp'); // Si vous utilisez Animate.css
          } else {
            bar.classList.add('d-none');
          }
        }
      "))))))

(defun render-sidebar (menu current-url)
  (with-html-string
    (:div :class "d-flex flex-column flex-shrink-0 p-3 text-white bg-dark" 
          :style "width: 280px; height: 100vh; position: fixed; left: 0; top: 0; overflow-y: auto;"
      
      (:a :href "/admin" :class "d-flex align-items-center mb-3 mb-md-0 me-md-auto text-white text-decoration-none"
          (:i :class "bi bi-speedometer2 fs-4 me-2")
          (:span :class "fs-4" "Lumen Admin"))
      (:hr)
      
      (:ul :class "nav nav-pills flex-column mb-auto"
        ;; Lien Dashboard
        (:li :class "nav-item"
             (:a :href "/admin" :class (if (string= current-url "/admin") "nav-link active" "nav-link text-white")
                 (:i :class "bi bi-house me-2") "Dashboard"))
        
        ;; Boucle sur les Modules introspectés
        (dolist (group menu)
          (:li :class "mt-3 text-uppercase text-secondary small fw-bold px-3" (getf group :section))
          (dolist (item (getf group :items))
            (:li 
              (:a :href (getf item :href) 
                  :class (if (search (getf item :href) current-url) "nav-link active" "nav-link text-white")
                  (:i :class (format nil "~A me-2" (getf item :icon)))
                  (getf item :label)))))))))

(defun render-admin-layout (req &key title content)
  (print "IN ADMIN LAYOUT")
  (let* ((user (lumen.modules.auth.service:current-uid req)) ;; Pseudo-code
         (menu (lumen.admin.introspection:collect-admin-menu))
         (url  (lumen.core.http:req-path req)))
    
    (print user)
    (print content)
    (print "**********")
    (with-html-string
      (:doctype)
      (:html
       (:head (:meta :charset "utf-8") (:title (format nil "~A - Admin" title))
              (lumen.view.html:render-core-assets)) ;; Bootstrap + HTMX
        
       (:body
        ;; 1. SIDEBAR FIXE
        (:raw (render-sidebar menu url))
          
        ;; 2. MAIN CONTENT WRAPPER
        (:div :style "margin-left: 280px;"
            
              ;; Header / Topbar
              (:header :class "p-3 mb-3 border-bottom bg-white d-flex justify-content-between align-items-center"
		       (:span :class "fw-bold text-muted" title)
              
		       (:div :class "d-flex align-items-center gap-3"
			     ;; --- TENANT SWITCHER (Placeholder Phase 1) ---
			     (:div :class "dropdown"
				   (:button :class "btn btn-outline-secondary btn-sm dropdown-toggle" :data-bs-toggle "dropdown"
					    (:i :class "bi bi-layers me-1") "Tenant: Global")
				   (:ul :class "dropdown-menu"
					(:li (:a :class "dropdown-item" :href "#" "Global View"))
					(:li (:a :class "dropdown-item" :href "#" "Acme Corp"))))
                
			     ;; User Menu
			     (:a :href "/" :class "btn btn-sm btn-link" "Retour au site")))
            
              ;; Page Content
              (:main :class "container-fluid p-4"
		     (:raw content)))
	(:div :id "toast-container" :style "z-index: 1055;"
	      :class "toast-container position-fixed bottom-0 end-0 p-3")
          
        (lumen.view.html:render-core-js))))))

(defun render-dashboard (req stats)
  (render-admin-layout 
   req 
   :title "Tableau de bord"
   :content 
   (with-html-string
     (:div :class "row"
       (dolist (stat stats)
         (:div :class "col-md-3 mb-4"
           (:div :class (format nil "card text-white bg-~A h-100" (getf stat :color))
             (:div :class "card-body"
               (:div :class "d-flex justify-content-between align-items-center"
                 (:div 
                   (:h5 :class "card-title" (getf stat :label))
                   (:h2 :class "display-6 fw-bold" (getf stat :value)))
                 (:i :class (format nil "~A fs-1 opacity-50" (getf stat :icon))))))))))))
