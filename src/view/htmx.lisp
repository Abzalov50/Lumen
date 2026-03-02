(in-package :cl)

(defpackage :lumen.view.htmx
  (:use :cl :spinneret :lumen.http.crud :lumen.view.html :lumen.view.components)
  (:import-from :lumen.core.http 
                :req-query :ctx-from-req :respond-html :respond-htmx-redirect
                :req-path :req-method :respond-404 :respond-422)
  (:import-from :lumen.utils :alist-get :to-kebab-case)
  (:import-from :lumen.data.dao 
                :entity-metadata :validate-entity-payload 
                :entity-validation-error :entity-validation-errors)
  (:export :mount-htmx-resource
           ;; Méthodes génériques pour le theming
           :render-htmx-index
           :render-htmx-form
   :render-htmx-show
   :add-htmx-trigger))

(in-package :lumen.view.htmx)

(defun add-htmx-trigger (triggers &optional (req lumen.core.http:*request*))
  "Ajoute un ou plusieurs événements au header HX-Trigger.
   TRIGGERS peut être une string simple ou une alist/plist pour du JSON."
  (let ((value (if (stringp triggers)
                   triggers
                   (cl-json:encode-json-to-string triggers))))
    (lumen.core.http:add-header "HX-Trigger" value)))

;;; ===========================================================================
;;; 1. UI METADATA HELPERS
;;; ===========================================================================

(defun %get-ui-meta (entity-sym key default)
  "Récupère une info UI depuis les métadonnées de l'entité.
   Suppose que defentity stocke une clé :ui dans ses métadonnées."
  (let* ((md (lumen.data.dao:entity-metadata entity-sym))
         (ui (getf md :ui)))
    (or (getf ui key) default)))

(defun %ui-label (entity-sym)
  (or (%get-ui-meta entity-sym :label nil)
      (string-capitalize (string-downcase entity-sym))))

(defun %ui-plural (entity-sym)
  (or (%get-ui-meta entity-sym :plural nil)
      (format nil "~As" (%ui-label entity-sym))))

(defun %ui-new-text (entity-sym)
  "Génère 'Nouveau Projet' ou 'Nouvelle Tâche' selon le genre."
  (let ((gender (%get-ui-meta entity-sym :gender :male)) ;; :male ou :female
        (label  (%ui-label entity-sym)))
    (format nil "~A ~A" 
            (if (eq gender :female) "Nouvelle" "Nouveau") 
            label)))

(defun %ui-edit-text (entity-sym)
  (format nil "Modifier ~A" (%ui-label entity-sym)))

(defun %columns->select-list (columns entity-sym)
  "Déduit la liste des champs à sélectionner à partir des colonnes affichées.
   Ajoute toujours la Primary Key (ex: :id) pour les actions."
  (let ((pk (or (getf (lumen.data.dao:entity-metadata entity-sym) :primary-key) :id))
        (fields '()))
    
    ;; 1. On parcourt les colonnes
    (dolist (c columns)
      (let ((key (getf c :key)))
        ;; On ignore les colonnes virtuelles (ex: :__actions__)
        (unless (member key '(:__actions__) :test #'eq)
          ;; On suppose que la clé de colonne = le champ DB (simplification)
          ;; Si vous avez des champs calculés, il faudrait une logique plus fine.
          (push key fields))))
    
    ;; 2. On ajoute la PK (obligatoire pour les liens Edit/Delete)
    (push pk fields)
    
    ;; 3. On dédoublonne
    (remove-duplicates fields :test #'eq)))

;;; ===========================================================================
;;; 2. VIEW RENDERERS (Customisables via defmethod)
;;; ===========================================================================
(defgeneric render-htmx-index (entity-sym items base-url columns
                               &key req id count page total-pages sort dir q title
				 current-filters layout mode search-url filter-fields
				 persistence-id force-partial)
  (:documentation "Affiche la page liste avec support natif HTMX (Swap OOB)."))

;; 2. Implémentation de la Méthode
(defmethod render-htmx-index (entity-sym items base-url columns
                              &key req (id "entity-table") (count 0) (page 1) (total-pages 1)
                                   (sort "created_at") (dir "desc") (q "") title
                                   (current-filters nil) (layout t) (mode :default)
                                   (search-url nil) (filter-fields nil) (persistence-id nil)
                                   (force-partial nil))
  
  (let* ((headers (if req (lumen.core.http:req-headers req) nil))
         (is-htmx (or force-partial (cdr (assoc "hx-request" headers :test #'string-equal))))
         (target  (cdr (assoc "hx-target" headers :test #'string-equal)))
         
         (container-id (format nil "~A-container" id))
         (tbody-id (format nil "~A-body" id))
         (form-id (format nil "~A-form" id))
         
         (sort-str (string-downcase (or sort "created_at")))
         (dir-str (string-upcase (or dir "desc")))
         (data-endpoint (if (eq mode :remote) (or search-url (format nil "~A/rows" base-url)) base-url))
         (current-data-url (format nil "~A?q=~A&sort=~A&dir=~A" data-endpoint q sort-str dir-str))
         (all-fields (lumen.data.dao:entity-fields entity-sym))
         (visible-filters (if filter-fields
                              (remove-if-not (lambda (f) (member (getf f :col) filter-fields)) all-fields)
                              (remove-if (lambda (f) (or (getf f :hidden?) (member (getf f :col) '(:id :password :secret) :test #'eq))) all-fields))))

    (labels ((render-rows ()
               (spinneret:with-html-string 
                 (if items
                     (dolist (row items)
                       (lumen.view.table::render-row row columns base-url))
                     (:tr (:td :colspan (length columns) :class "text-center py-5 text-muted" "Aucun élément trouvé.")))))
             
             (render-content-block ()
               ;; On capture le contenu du tableau dans une string
               (spinneret:with-html-string
                 (:form :id form-id :class "mb-3" :method "GET" :action base-url
                        (:input :type "hidden" :name "sort" :value sort-str)
                        (:input :type "hidden" :name "dir" :value dir-str)
                        (:div :class "row g-2 mb-2"
                              (:div :class "col-md-4"
                                    (:div :class "input-group"
                                          (:span :class "input-group-text bg-white" (:i :class "bi bi-search"))
                                          (:input :type "search" :name "q" :class "form-control" 
                                                  :placeholder "Recherche..." :value q
                                                  :hx-get data-endpoint :hx-trigger "keyup changed delay:500ms, search"
                                                  :hx-target (format nil "#~A" tbody-id) :hx-swap "innerHTML"
                                                  :hx-include (format nil "#~A" form-id))))
                              (:div :class "col-auto ms-auto"
                                    (:button :type "button" :class "btn btn-outline-secondary btn-sm"
                                             :onclick (format nil "localStorage.removeItem('~A'); window.location.href='~A';" (or persistence-id "") base-url)
                                             (:i :class "bi bi-x-lg me-1") "Effacer")))
                        (when visible-filters
                          (:details :class "border rounded px-3 py-2 bg-white shadow-sm mt-2"
                                    :open (if (> (length current-filters) 0) "true" nil)
                                    (:summary :class "small text-primary fw-bold cursor-pointer mb-2" :style "cursor: pointer; list-style: none;"
                                              (:i :class "bi bi-funnel me-1") "Filtres avancés")
                                    (:div :class "row g-3"
                                          (dolist (field visible-filters)
                                            (lumen.view.components::render-filter-widget 
                                             field current-filters :hx-get data-endpoint :hx-target (format nil "#~A" tbody-id) 
                                             :hx-include (format nil "#~A" form-id)))))))

                 (:div :class "card shadow-sm"
                       (:div :class "table-responsive"
                             (:table :class "table table-hover align-middle mb-0"
                                     (:thead :class "table-light"
                                             (:tr (dolist (c columns)
                                                    (lumen.view.table::render-header-col c sort-str dir-str data-endpoint container-id))))
                                     (:tbody :id tbody-id
                                             :hx-get (if (eq mode :remote) current-data-url "")
                                             :hx-trigger (if (eq mode :remote) "load" "")
                                             :hx-target "this" :hx-include (format nil "#~A" form-id)
                                             (if (eq mode :remote)
                                                 (:tr (:td :colspan (length columns) :class "text-center py-5"
                                                           (:div :class "spinner-border text-primary")))
                                                 (:raw (render-rows))))))
                       (:div :id (format nil "~A-pagination" id)
                             (unless (eq mode :remote)
                               (lumen.view.table::render-pagination 
                                (list :page page :total-pages total-pages :total-items count)
                                data-endpoint container-id (format nil "#~A" form-id)))))
                 
                 (when persistence-id
                   (:script (:raw (format nil "(function(){ try { const k='~A', f=document.getElementById('~A'); const s=JSON.parse(localStorage.getItem(k)); if(s) Object.keys(s).forEach(n=>{ const el=f.querySelector(`[name='${n}']`); if(el) el.value=s[n]; }); document.body.addEventListener('htmx:configRequest', e=>{ if(e.detail.elt.closest(`#${f.id}`)) localStorage.setItem(k, JSON.stringify({...JSON.parse(localStorage.getItem(k)||'{}'), ...e.detail.parameters})); }); } catch(e){} })();" persistence-id form-id)))))))

      ;; --- RETOUR ---
      (cond
        ;; Cas A: Recherche -> Lignes seules
        ((and is-htmx (string-equal target tbody-id))
         (render-rows))

        ;; Cas B & C: Pagination ou Page complète -> On renvoie TOUJOURS LE WRAPPER
        (t
         (let ((full-html 
                 (spinneret:with-html-string 
                   (:div :id container-id ;; LE WRAPPER EST TOUJOURS LÀ
                         (:raw (render-content-block))))))
           
           (if (and layout (not is-htmx)) ;; Layout seulement si ce n'est pas du HTMX
               (lumen.view.html:with-layout (:title (or title "Liste"))
                 (:div :class "container-fluid py-4"
                       (:div :class "d-flex justify-content-between align-items-center mb-4"
                             (:h2 (:i :class "bi bi-table me-2") (or title "Liste"))
                             (:a :class "btn btn-primary" :href (format nil "~A/new" base-url) (:i :class "bi bi-plus-lg") " Nouveau"))
                       (:raw full-html)))
               full-html)))))))
#|
(defgeneric render-htmx-index (entity-sym items base-url columns
                               &key count page total-pages sort dir q title
                               current-filters layout mode search-url filter-fields persistence-id)
  (:documentation "Affiche la page liste. Si :layout nil, affiche seulement le composant table+filtres.")
  
  (:method (entity-sym items base-url columns
                                &key (count 0) (page 1) (total-pages 1) 
                                     (sort :created_at) (dir :desc) (q "") title
                                     (current-filters nil)
                                     (layout t)
                                     (mode :default)     ;; :default | :remote
                                     (search-url nil)   ;; URL pour les rows en mode remote
                                     (filter-fields nil) ;; Liste des colonnes à filtrer
                                     (persistence-id nil))
  (let* ((page-title (or title (%ui-plural entity-sym)))
         (sort-str (string-downcase sort))
         (dir-str (string-upcase dir))
         (fields (lumen.data.dao:entity-fields entity-sym))
         ;; Filtrage intelligent des champs de recherche
         (visible-filters 
          (if filter-fields
              (remove-if-not (lambda (f) (member (getf f :col) filter-fields)) fields)
              (remove-if (lambda (f) 
                           (or (getf f :hidden?) 
                               (member (getf f :col) '(:id :tenant-id :password :secret) :test #'eq)))
                         fields)))
         (data-url (or search-url (format nil "~A/rows" base-url)))
         (pid (or persistence-id (format nil "table-state-~A" (string-downcase entity-sym)))))

    (flet ((render-dynamic-content ()
             (spinneret:with-html
               (:div :id "entity-table-container"
                     ;; --- FORMULAIRE UNIFIÉ ---
                     (:form :id "table-filters-form" :class "mb-3"
                            :hx-get (if (eq mode :remote) data-url base-url)
                            :hx-target (if (eq mode :remote) "#entity-table-container-body" "#entity-table-container")
                            :hx-swap (if (eq mode :remote) "innerHTML" "outerHTML")
                            :hx-trigger "submit, change delay:300ms"
                            
                            ;; État persistant (Hidden)
                            (:input :type "hidden" :name "sort" :value sort-str)
                            (:input :type "hidden" :name "dir" :value dir-str)
                            (:input :type "hidden" :name "page" :value page)

                            (:div :class "row g-2 mb-2"
                                  (:div :class "col-md-4"
                                        (:div :class "input-group"
                                              (:span :class "input-group-text bg-white" (:i :class "bi bi-search"))
                                              (:input :type "search" :name "q" :class "form-control" 
                                                      :placeholder "Recherche globale..." :value q)))
                                  
                                  ;; BOUTON RESET
                                  (:div :class "col-auto ms-auto"
                                        (:button :type "button" :class "btn btn-outline-secondary btn-sm"
                                                 :onclick "const f=this.form; f.reset(); f.querySelectorAll('input[type=hidden]').forEach(i=>i.value=''); htmx.trigger(f, 'submit')"
                                                 (:i :class "bi bi-x-lg me-1") "Effacer")))

                            ;; FILTRES DANS DETAILS
                            (when visible-filters
                              (:details :class "border rounded px-3 py-2 bg-white shadow-sm"
                                        :open (if (> (length current-filters) 0) "true" nil)
                                        (:summary :class "small text-primary fw-bold cursor-pointer mb-2" 
                                                  :style "cursor: pointer; list-style: none;"
                                                  (:i :class "bi bi-funnel me-1") "Filtres avancés")
                                        (:div :class "row g-3"
                                              (dolist (f visible-filters)
                                                (lumen.view.components:render-filter-widget f current-filters))))))

                     ;; --- APPEL AU DATAGRID ---
                     (lumen.view.table:render-datagrid items columns 
                                                      :id "entity-table-container"
                                                      :source-url data-url
                                                      :mode mode
                                                      :current-sort sort-str 
                                                      :current-dir dir-str
                                                      :pagination (list :page page :total-pages total-pages :total-items count)
                                                      :empty-message (format nil "Aucun élément de type '~A' trouvé." (%ui-label entity-sym))))
               
               ;; SCRIPT DE PERSISTENCE (Spécifique au mode Remote)
               (when (eq mode :remote)
                 (:script (:raw (format nil "
                    (function() {
                      const KEY = '~A';
                      const form = document.getElementById('table-filters-form');
                      // Restauration...
                      const saved = JSON.parse(localStorage.getItem(KEY) || '{}');
                      Object.keys(saved).forEach(k => {
                        const el = form.querySelector(`[name='${k}']`);
                        if(el) el.value = saved[k];
                      });
                      // Sauvegarde sur chaque requête...
                      document.body.addEventListener('htmx:configRequest', (e) => {
                        if(e.detail.elt.closest('#table-filters-form')) localStorage.setItem(KEY, JSON.stringify(e.detail.parameters));
                      });
                      // Helper Tri
                      window.updateSortAndSubmit = (elt, key, formId) => {
                        const f = document.getElementById(formId);
                        const s = f.querySelector('[name=sort]');
                        const d = f.querySelector('[name=dir]');
                        if(s.value === key) d.value = (d.value === 'ASC' ? 'DESC' : 'ASC');
                        else { s.value = key; d.value = 'ASC'; }
                        htmx.trigger(f, 'submit');
                      };
                    })();
                 " pid)))))))

      ;; --- WRAPPER LAYOUT (Rétrocompatibilité Admin) ---
      (if layout
          (lumen.view.html:with-layout (:title page-title)
             (:div :class "container-fluid py-4"
                   (unless (lumen.view.html:htmx-target)
                     (:div :class "d-flex justify-content-between align-items-center mb-4"
                           (:h2 (:i :class "bi bi-table me-2") page-title)
                           (:a :class "btn btn-primary" :href (format nil "~A/new" base-url)
                               (:i :class "bi bi-plus-lg") " " (%ui-new-text entity-sym))))
                   (render-dynamic-content)))
          (render-dynamic-content))))))
|#
#|
(defgeneric render-htmx-index (entity-sym items base-url columns
                               &key count page total-pages sort dir q title
                               current-filters layout)
  (:documentation "Affiche la page liste. Si :layout nil, affiche seulement le composant table+filtres.")
  
  (:method (entity-sym items base-url columns
            &key (count 0) (page 1) (total-pages 1) 
                 (sort :created_at) (dir :desc) (q "") title
                 (current-filters nil)
                 (layout t)) ;; <--- Valeur par défaut T
    
    (let* ((page-title (or title (%ui-plural entity-sym)))
           (sort-str (string-downcase sort))
           (fields    (lumen.data.dao:entity-fields entity-sym))
           (dir-str   (string-upcase dir)))

      (format t "~&[HTMX-INDEX] Layout ? ~A~%" layout)

      ;; --- 1. FONCTION LOCALE : LE CONTENU PUR ---
      (flet ((render-dynamic-content ()
               (spinneret:with-html
                 (:div :id "entity-table-container"
                   ;; Formulaire (Recherche + Filtres)
                   (:form :class "mb-3"
                          :hx-get base-url 
                          :hx-target "#entity-table-container" 
                          :hx-swap "outerHTML" 
                          :hx-trigger "submit, change delay:300ms"
                          
                          ;; Preservation des tris
                          (:input :type "hidden" :name "sort" :value sort-str)
                          (:input :type "hidden" :name "order" :value (format nil "~A:~A" sort-str dir-str))
                          
                          ;; Barre de recherche et filtres
                          (:div :class "row g-2 mb-2"
                            (:div :class "col-md-4"
                              (:div :class "input-group"
                                (:span :class "input-group-text bg-white" (:i :class "bi bi-search"))
                                (:input :type "search" :name "q" :class "form-control" 
                                        :placeholder "Recherche globale..." :value q)))
                            
                            (:div :class "col-auto ms-auto"
                              (:button :type "button" 
                                       :class "btn btn-outline-secondary btn-sm"
                                       :hx-get base-url 
                                       :hx-target "#entity-table-container" 
                                       :hx-swap "outerHTML"
                                       :hx-push-url "true"
                                       (:i :class "bi bi-x-lg me-1") "Effacer")))

                          ;; Filtres dynamiques
                          (:details :class "border rounded px-3 py-2 bg-white shadow-sm"
                                    :open (if (> (length current-filters) 0) "true" nil)
                                    (:summary :class "small text-primary fw-bold cursor-pointer mb-2" 
                                              :style "cursor: pointer; list-style: none;"
                                              (:i :class "bi bi-funnel me-1") "Filtres par colonnes")
                                    (:div :class "row g-3"
                                          (dolist (f fields)
                                            (unless (or (getf f :hidden?) 
                                                        (member (getf f :col) '(:id :tenant-id :password :secret) :test #'eq))
                                              (lumen.view.components:render-filter-widget f current-filters))))))
                   
                   ;; La Grille de Données
                   (lumen.view.table:render-datagrid items columns 
                                                     :id "entity-table-container"
                                                     :source-url base-url
                                                     :current-sort sort-str 
                                                     :current-dir dir-str
                                                     :pagination (list :page page :total-pages total-pages :total-items count)
                                                     :empty-message (format nil "Aucun élément de type '~A' trouvé." (%ui-label entity-sym)))))))

        ;; --- 2. LOGIQUE D'AFFICHAGE ---
        (if layout
            ;; MODE PAGE COMPLETE (Admin standard)
            (let ((target (lumen.view.html:htmx-target)))
              (with-layout (:title page-title)
                (unless target 
                  ;; Breadcrumbs et Titre H2 seulement si pas HTMX
                  (:nav :aria-label "breadcrumb" :class "mb-5"
                    (:ol :class "breadcrumb"
                      (:li :class "breadcrumb-item" (:a :href "/" "Accueil"))
                      (:li :class "breadcrumb-item active" page-title)))
                  
                  (:div :class "d-flex justify-content-between align-items-center mb-4"
                    (:h2 (:i :class "bi bi-table me-2") page-title)
                    (:a :class "btn btn-primary" 
                        :href (format nil "~A/new" base-url)
                        (:i :class "bi bi-plus-lg") " " (%ui-new-text entity-sym))))
                
                ;; Contenu
                (render-dynamic-content)))
            
            ;; MODE PARTIEL (Intégration Dashboard)
            ;; On rend juste le contenu dynamique, sans wrapper, sans titre H2
            (render-dynamic-content))))))
|#

(defgeneric render-htmx-show (entity-sym item base-url &key title)
  (:method (entity-sym item base-url &key title)
    (let* ((label    (or title (format nil "Détails : ~A" (%ui-label entity-sym))))
           (fields   (lumen.data.dao:entity-fields entity-sym))
           ;;(base-url (format nil "/~As" (string-downcase entity-sym))) 
           (id       (lumen.utils:lookup item :id)))

      (with-html-string
        ;; Spinneret gère l'imbrication parfaitement tant que les parenthèses sont bonnes
        (:div :class "modal fade show" 
              :id "entity-details-modal"
              :tabindex "-1" 
              :role "dialog"
              :aria-modal "true"
              :style "display: block; background-color: rgba(0,0,0,0.5); z-index: 2000;"
              ;; APPEL JS SIMPLE : Spinneret n'aura aucun problème avec ça
              :onclick "if(event.target===this) closeModal()"
          
          (:div :class "modal-dialog modal-lg modal-dialog-centered" 
                :role "document"
                ;; Empêche la fermeture si on clique DANS la boîte
                :onclick "event.stopPropagation()"
            
            (:div :class "modal-content shadow"
              
              ;; HEADER
              (:div :class "modal-header"
                (:h5 :class "modal-title" label)
                (:button :type "button" :class "btn-close" 
                         :onclick "closeModal()" ;; PROPRE
                         :aria-label "Close"))
              
              ;; BODY
              (:div :class "modal-body"
                (:table :class "table table-borderless align-middle m-0"
                  (:tbody
                   (dolist (f fields)
                     (let* ((col (getf f :col)) (type (getf f :type))
                            (label (or (getf f :label) (string-capitalize (string-downcase col))))
                            (choices (getf f :choices)) (hidden? (getf f :hidden?))
                            (ref (getf f :references)) (val (lumen.utils:lookup item col)))
                       
                       (unless (or hidden? (member col '(:password :secret :meta :tenant-id) :test #'eq))
                         (:tr
                          (:th :scope "row" :class "text-muted w-25 fw-normal ps-3" label)
                          (:td :class "fw-medium"
                           (cond
                             (ref (let ((rk (intern (string-upcase (format nil "~A-RESOLVED" col)) :keyword)))
                                    (or (lumen.utils:lookup item rk) val)))
                             ((null val) (:span :class "text-muted fst-italic" "N/A"))
                             ((eq type :boolean) (if val "Oui" "Non"))
                             (choices (let ((p (assoc val choices :test (lambda (v k) (string-equal (format nil "~A" v) (format nil "~A" k))))))
                                        (if p (cdr p) val)))
                             ((member type '(:date :datetime :timestamp)) (lumen.utils:%val->date-display val))
                             (t (format nil "~A" val)))))))))))
              
              ;; FOOTER
              (:div :class "modal-footer bg-light"
                (:a :href (format nil "~A/~A/edit" base-url id)
                    :class "btn btn-primary"
                    (:i :class "bi bi-pencil me-1") "Modifier")
                
                (:button :type "button" :class "btn btn-secondary" 
                         :onclick "closeModal()"
                         "Fermer")))))))))

(defmethod render-htmx-form ((entity-sym symbol) action method cancel-url &key values errors title)
  ;; 1. On génère le HTML du FORMULAIRE SEUL (Le contenu pur)
  (let ((form-content 
         (with-html-string
           (lumen.view.form:render-entity-form entity-sym 
              :action action 
              :method method
              :values values
              :errors errors
              :submit-text "Enregistrer"
              :cancel-url cancel-url
              ;; Important : Le formulaire doit se remplacer lui-même
              :hx-swap "outerHTML"))))
    ;;(print (list :form-content form-content))
    ;; 2. LOGIQUE DE DÉCISION
    (if (htmx-request-p)
        ;; CAS HTMX (Erreur de validation) : 
        ;; On renvoie JUSTE le formulaire.
        ;; HTMX va remplacer l'ancien <form> (qui est dans la card) par ce nouveau <form>.
        ;; La Card existante reste en place. Pas d'imbrication.
        (with-html-string 
          (:raw form-content))
        
        ;; CAS STANDARD (Accès direct via URL) :
        ;; On doit construire la page complète avec la Card.
        (lumen.view.html:with-layout (:title title)
           (:div :class "row justify-content-center"
             (:div :class "col-md-8 col-lg-6"
               (:div :class "card shadow-sm"
                 (:div :class "card-header bg-white py-3"
                   (:h4 :class "mb-0 card-title" title))
                 (:div :class "card-body"
                   ;; On injecte le formulaire ici
                       (:raw form-content)))))))))

(defmethod render-htmx-form ((entity-sym symbol) action method cancel-url &key values errors title)
  ;; 1. CAPTURE HYBRIDE INFALLIBLE
  (let ((form-content 
         (spinneret:with-html-string
           (let ((result (lumen.view.form:render-entity-form entity-sym 
                             :action action 
                             :method method
                             :values values
                             :errors errors
                             :submit-text "Enregistrer"
                             :cancel-url cancel-url
                             :hx-swap "outerHTML")))
             ;; Sécurité : Si render-entity-form retourne explicitement la chaîne 
             ;; au lieu de l'écrire dans le flux, on la force dans le flux via :raw
             (when (and (stringp result) (> (length result) 0))
               (:raw result))))))
    
    ;; Vérification anti-panique dans la console
    (when (str:blank? form-content)
      (format t "~&[WARN] render-entity-form a généré un contenu vide pour ~A~%" entity-sym))

    ;; 2. LOGIQUE DE DÉCISION
    (if (htmx-request-p)
        ;; CAS HTMX : On retourne simplement la chaîne HTML capturée
        form-content
        
        ;; CAS STANDARD : On englobe dans le layout complet
        (lumen.view.html:with-layout (:title title)
             (:div :class "row justify-content-center"
               (:div :class "col-md-8 col-lg-6"
                 (:div :class "card shadow-sm"
                   (:div :class "card-header bg-white py-3"
                     (:h4 :class "mb-0 card-title" title))
                   (:div :class "card-body"
                     ;; On injecte le formulaire capturé
                     (:raw form-content)))))))))

;;; ===========================================================================
;;; 3. HANDLERS (Logique de contrôle)
;;; ===========================================================================
(defun %compute-joins-and-selects (entity-sym columns)
  "Analyse les colonnes pour construire les JOINS et SELECTS optimisés."
  (let* ((md (lumen.data.dao:entity-metadata entity-sym))
         (main-table (getf md :table))
         (fields (lumen.data.dao:entity-fields entity-sym))
         (selects '())
         (joins '()))

    ;; 1. On sélectionne toujours l'ID de la table principale (pour les actions)
    (push (format nil "~A.id" main-table) selects)

    (dolist (c columns)
      (let* ((key (getf c :key))
             ;; On retrouve la définition du champ correspondant
             (field (find key fields :key (lambda (f) (getf f :col))))
             (ref (getf field :references))      ;; ex: "projects"
             (ref-col (getf field :ref-col "id"))) ;; ex: "name"

        (cond
          ;; --- CAS FK : On construit le JOIN ---
          ((and field ref)
           (let* ((col-str (string-downcase key))
                  ;; Alias unique pour la table jointe (évite collision si 2 FK vers users)
                  ;; ex: table "projects" devient alias "join_project_id"
                  (table-alias (format nil "join_~A" col-str))
                  ;; Alias pour la colonne de résultat
                  ;; ex: "project_id_resolved"
                  (col-alias (format nil "~A_resolved" col-str)))
             
             ;; Ajout du SELECT pour l'ID original (pour l'édition)
             (push (format nil "~A.~A" main-table col-str) selects)
             
             ;; Ajout du SELECT pour le Libellé (ex: projects.name AS project_id_resolved)
             ;; On passe une string brute pour gérer le "AS" si %build-select le supporte,
             ;; sinon on passe une liste (:col :as :alias) selon votre implémentation DB.
             (push (list (format nil "~A.~A" table-alias ref-col) :as (intern (string-upcase col-alias) :keyword)) 
                   selects)
             
             ;; Ajout du JOIN
             ;; LEFT JOIN projects AS join_project_id ON join_project_id.id = tasks.project_id
             (push (list :type :left
                         :table ref
                         :as table-alias
                         :on (format nil "~A.id = ~A.~A" table-alias main-table col-str))
                   joins)))

          ;; --- CAS STANDARD ---
          ((and field (not ref))
           (push (format nil "~A.~A" main-table (string-downcase key)) selects)))))
    
    ;; On retourne les listes prêtes pour repo-index
    (values (nreverse selects) (nreverse joins))))


(defun %handle-htmx-index (req entity-sym base-url auth-guard order-whitelist
			   &key columns title)
  ;; 1. SÉCURITÉ THREAD : On s'assure que *request* est dispo pour htmx-request-p
  (let ((lumen.core.http:*request* req))
    
    (lumen.http.crud::with-crud-error-handling
      (when auth-guard (funcall auth-guard req :op :index))

      (let* ((ctx (ctx-from-req req))
             (qp  (req-query req))
             (raw-filters (lumen.http.crud::%parse-filter-params qp))
             (table-name (getf (lumen.data.dao:entity-metadata entity-sym) :table))
             
             ;; 2. FILTRES PROPRES (Ignore tenant_id + chaînes vides)
             (filters 
               (loop for (key . val) in raw-filters
                     unless (member key '("tenant_id" "tenant-id") :test #'string=)
                     when (and (stringp val) (> (length val) 0)) ;; Chaînes vides ignorées
                     collect (cons
                              (cond 
                                ((string= key "q") key)
                                ;; GESTION INTELLIGENTE DES SUFFIXES
                                ((or (search "_gte" key) (search "_lte" key))
                                 (let* ((suffix (if (search "_gte" key) "_gte" "_lte"))
                                        (base (subseq key 0 (- (length key) 4))) ;; "amount"
                                        (real-col (if (find #\. base) base (format nil "~A.~A" table-name base))))
                                   ;; On reconstruit : "tasks.amount_gte"
                                   (format nil "~A~A" real-col suffix)))
                                
                                ((find #\. key) key)
                                (t (format nil "~A.~A" table-name key)))
                              val)))
             
             (final-cols (or columns (lumen.view.table::%derive-columns-from-entity entity-sym))))
        
        (multiple-value-bind (auto-selects auto-joins) 
            (%compute-joins-and-selects entity-sym final-cols)
          
          (multiple-value-bind (page psize limit) (lumen.http.crud::%parse-pagination-params qp)
            
            ;; 3. LOGIQUE DE TRI
            (let* ((p-sort  (cdr (assoc "sort" qp :test #'string-equal)))
                   (p-dir   (cdr (assoc "dir"  qp :test #'string-equal)))
                   (p-order (cdr (assoc "order" qp :test #'string-equal)))
                   (raw-order 
                    (cond ((and p-sort (> (length p-sort) 0))
                           (list (list p-sort (if (string-equal p-dir "DESC") :desc :asc))))
                          ((and p-order (> (length p-order) 0))
                           (lumen.http.crud::%parse-order p-order))
                          (t nil)))
                   (default-order (lumen.http.crud::%derive-default-order entity-sym))
                   (base-order (or (lumen.http.crud::%ensure-order-whitelist 
                                    raw-order 
                                    (or order-whitelist 
                                        (mapcar (lambda (f) (getf f :col)) 
                                                (lumen.data.dao:entity-fields entity-sym))))
                                   default-order))
                   (final-order 
                     (mapcar (lambda (pair)
                               (let ((col (first pair)) (dir (second pair)))
                                 (list (if (find #\. (string col)) 
                                           col 
                                           (format nil "~A.~A" table-name (string-downcase col))) 
                                       dir)))
                             base-order)))

              ;; 4. APPEL REPO
              (let ((result (lumen.data.repo.core:repo-index 
                             entity-sym ctx
                             :filters filters :select auto-selects :joins auto-joins
                             :order final-order :page page :page-size psize :limit limit)))
                (let* ((items (if (and (listp result) (keywordp (car result))) (getf result :items result) result))
                       (count (if (and (listp result) (keywordp (car result))) (getf result :count 0) (length items)))
                       (final-limit (or psize limit 20))
                       (total-pages (ceiling count final-limit))
                       (primary-sort (first base-order))
                       (ui-q (cdr (assoc "q" qp :test #'string=)))
                       (active-filters 
                        (remove-if (lambda (p) 
                                     (member (car p) '("q" "sort" "dir" "order" "page" "limit") :test #'string-equal))
                                   qp)))
                  ;; 5. RENDU ET HEADER VARY
                  (let ((resp (respond-html
                               (render-htmx-index entity-sym items base-url final-cols
                                                  :count count :page (or page 1) :total-pages total-pages
                                                  :sort (if primary-sort (first primary-sort) :created_at)
                                                  :dir (if primary-sort (second primary-sort) :desc)
                                                  :q (or ui-q "") :title title
                                                  :current-filters active-filters))))
                    ;; 1. VARY : Indispensable pour que les proxies comprennent que HX-Request change la donne
                    (setf (lumen.core.http:resp-headers resp)
                          (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "Vary" "HX-Request"))
                    
                    ;; 2. CACHE-CONTROL : GLOBAL (On force le navigateur à toujours vérifier)
                    ;; On applique ça à TOUTE la réponse, pas seulement si c'est HTMX.
                    ;; Cela empêche le navigateur de servir la "Page Complète" périmée quand on fait un appel HTMX.
                    (setf (lumen.core.http:resp-headers resp)
                          (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) 
                                                     "Cache-Control" "no-store, no-cache, must-revalidate, max-age=0"))
                    
                    ;; 3. PRAGMA (Pour les vieux navigateurs/proxies, ceinture et bretelles)
                    (setf (lumen.core.http:resp-headers resp)
                          (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "Pragma" "no-cache"))
                    
                    resp))))))))))

(defun %handle-htmx-save (req entity-sym base-url index-url id method)
  "Gère CREATE (POST) et UPDATE (PUT) avec gestion d'erreurs UI."
  (let* ((ctx (ctx-from-req req))
         ;; IMPORTANT: Avec HTMX/Form, les données sont dans req-params (body form), pas json
         (payload (getf ctx :form))
         (is-patch (or (eq method :PUT) (eq method :PATCH)))
         ;;(base-url (format nil "/~As" (string-downcase entity-sym)))
	 )

    (format t "~&[HTMX-SAVE] Entity: ~A | Id: ~A~%" entity-sym id)
    (format t "~&[HTMX-SAVE] CTX: ~A~%PAYLOAD: ~A~%IS PATCH ? ~A~%" ctx payload is-patch)
    
    ;; 1. Validation de surface (API Layer)
    (let ((errors (validate-entity-payload entity-sym payload :partial is-patch)))
      (format t "~&[HTMX-SAVE] PAYLOAD ERRORS ? ~A~%" errors)
      (if errors
          ;; Erreur de validation -> On réaffiche le formulaire avec les erreurs (422)
          (respond-html 
           (render-htmx-form entity-sym 
                             (if is-patch (format nil "~A/~A" base-url id) base-url)
                             method
			     index-url
                             :values payload ;; On garde ce que l'user a saisi
                             :errors errors
                             :title (if is-patch (%ui-edit-text entity-sym) (%ui-new-text entity-sym)))
           :status 422)
          
          ;; 2. Exécution Repo
          (handler-case
              (progn
                (if is-patch
                    (lumen.data.repo.core:repo-patch entity-sym ctx id payload)
                    (lumen.data.repo.core:repo-create entity-sym ctx payload))
		(format t "~&[HTMX-SAVE] SUCCÈS !~%")
		
                ;; Succès -> Redirection Client-Side via HTMX
                ;; --- SUCCÈS : RÉPONSE À ÉVÉNEMENTS ---
                (let ((resp (lumen.core.http:respond-html ""))) ;; Corps vide
                  
                  ;; On construit le header HX-Trigger avec un JSON
                  ;; Cela déclenche 2 événements côté client :
                  ;; 1. showMessage : Affiche le Toast
                  ;; 2. entitySaved : Signal pour rafraîchir la grille (si nécessaire)
                  ;; 3. closeModal  : Signal pour fermer la modale (si vous l'utilisez)
                  
                  (let ((trigger-json 
                         (cl-json:encode-json-to-string 
                          `((:show-message . ((:type . "success")
                                               (:message . ,(format nil "~A enregistré avec succès."
                                                                    (%ui-label entity-sym)))))
                            (:entity-saved . t)
                            (:close-modal . t)))))
                    
                    (setf (lumen.core.http:resp-headers resp)
                          (lumen.utils:ensure-header 
                           (lumen.core.http:resp-headers resp) 
                           "HX-Trigger" 
                           trigger-json))

		    (setf (lumen.core.http:resp-headers resp)
                          (lumen.utils:ensure-header 
                           (lumen.core.http:resp-headers resp) 
                           "HX-Request" 
                           "true")))
                  
                  ;; Optionnel : Si vous n'êtes pas dans une modale et voulez VRAIMENT rediriger
                  (setf (lumen.core.http:resp-headers resp)
                         (lumen.utils:ensure-header (lumen.core.http:resp-headers resp) "HX-Location" index-url))
                  
                  resp))
            
            ;; Erreur Métier (ex: doublon email)
            (entity-validation-error (e)
               (respond-html 
                (render-htmx-form entity-sym 
                                  (if is-patch (format nil "~A/~A" base-url id) base-url)
                                  method
				  index-url
                                  :values payload
                                  :errors (entity-validation-errors e)
                                  :title "Erreur")
                :status 422)))))))

;; --- Handlers de Formulaires (GET) ---
(defun %handle-htmx-show (req entity-sym base-url auth-guard id &key title)
  (lumen.http.crud::with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :show))
    
    (let* ((ctx (ctx-from-req req))
	   (md (lumen.data.dao:entity-metadata entity-sym))
           (table (lumen.data.repo.query::%ident (getf md :table))) ;; ex: "tasks"
           
           ;; --- ID QUALIFIÉ ---
           ;; On construit "tasks.id" au lieu de juste :id
           (pk-qualified (format nil "~A.id" table))
	   
           ;; ASTUCE : On utilise repo-index car il gère déjà 
           ;; les JOINS et la résolution des noms (project-id-resolved).
           ;; On filtre simplement sur l'ID unique.
           (cols (lumen.view.table::%derive-columns-from-entity entity-sym))
           
           ;; Calcul automatique des joins comme pour le tableau
           (auto-selects (nth-value 0 (%compute-joins-and-selects entity-sym cols)))
           (auto-joins   (nth-value 1 (%compute-joins-and-selects entity-sym cols)))
           
           (result (lumen.data.repo.core:repo-index 
                    entity-sym ctx
                    :filters `((= ,pk-qualified ,id)) ;; <-- Filtre sur l'ID
                    :select auto-selects
                    :joins auto-joins
                    :page-size 1)) ;; On ne veut qu'un résultat
           
           ;; repo-index retourne {:items (...) :count ...} ou juste la liste
           (items (if (and (listp result) (keywordp (car result))) (first (getf result :items result)) result))
           (item  (first items)))
      
      (if item
          (respond-html
           (render-htmx-show entity-sym item base-url :title title))
          (respond-404)))))

(defun %handle-htmx-new (req entity-sym base-url index-url auth-guard &key title)
  (declare (ignore base-url))
  (lumen.http.crud::with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :create))
    (let (;;(base-url (format nil "/~As" (string-downcase entity-sym)))
	  )
     ;; (print "IN %handle-htmx-new")
      (respond-html
       (render-htmx-form entity-sym index-url :POST index-url
                         :title (or title (%ui-new-text entity-sym)))))))

(defun %handle-htmx-edit (req entity-sym base-url index-url auth-guard id &key title)
  (lumen.http.crud::with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :show)) ;; Scope Read requis pour voir le form
    (let* ((ctx (ctx-from-req req))
           (item (lumen.data.repo.core:repo-show entity-sym ctx id))
	   (action-url (format nil "~A/~A" base-url id))
           ;;(base-url (format nil "/~As" (string-downcase entity-sym)))
	   )
      (if item
          (respond-html
           (render-htmx-form entity-sym action-url :PUT index-url
                             :values item
                             :title (or title (%ui-edit-text entity-sym))))
          (respond-404)))))

;; --- Handlers d'Actions (POST/PUT/DELETE) ---

(defun %handle-htmx-create (req entity-sym action-url index-url auth-guard)
  (lumen.http.crud::with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :create))
    (%handle-htmx-save req entity-sym action-url index-url nil :POST)))

(defun %handle-htmx-update (req entity-sym action-url index-url auth-guard id)
  (lumen.http.crud::with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :patch))
    (%handle-htmx-save req entity-sym action-url index-url id :PUT)))

(defun %handle-htmx-delete (req entity-sym auth-guard id)
  (lumen.http.crud::with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :delete))
    (let ((ctx (ctx-from-req req)))
      (lumen.data.repo.core:repo-delete entity-sym ctx id)
      ;; Réponse vide (200 OK) -> HTMX supprime la ligne du tableau
      (respond-html "" :status 200))))

;;; ===========================================================================
;;; 4. ROUTE GENERATOR (Macro)
;;; ===========================================================================

(defun mount-htmx-resource (entity-sym &key (base "") (name nil) 
                                            (columns nil) (title nil)
                                            (order-whitelist '())
                                            (auth-guard nil)
                                            (host nil)
                                            (actions '(:index :show :create :edit :delete)))
  "Génère une LISTE de formulaires (construct-route ...) pour l'interface HTMX.
   Analogue à mount-crud! mais pour le HTML."
  
  (let* ((md (lumen.data.dao:entity-metadata entity-sym))
         (_ (unless md (error "Métadonnées introuvables pour ~A dans mount-htmx-resource!" entity-sym)))
         
         ;; Calcul des segments d'URL
         (tbl (getf md :table))
         (seg (or name tbl (format nil "~As" (string-downcase (symbol-name entity-sym))))) ;; Pluriel par défaut
         
         ;; Note: 'base' par défaut est vide "" pour avoir "/tasks". 
         ;; Si base est "/admin", on aura "/admin/tasks".
         (base-root  (if (string= base "") (format nil "/~a" seg) (format nil "~a/~a" base seg)))
         (base-new   (format nil "~a/new" base-root))
         (base-item  (format nil "~a/:id" base-root))
         (base-edit  (format nil "~a/:id/edit" base-root))
         
         (forms '()))
    (declare (ignore _))

    (format t "~&[HTMX-RESOURCE] BASE: ~A~%SEG: ~A~%BASE ROOT: ~A~%BASE NEW: ~A~%" base seg base-root base-item)
    (format t "~&[HTMX-RESOURCE] ACTIONS: ~A~%" actions)
    ;; Helper local pour gérer le host
    (flet ((route-spec (p) (if host `(:host ,host :path ,p) p)))

      ;; 7. NEW FORM (GET /tasks/new)
      (when (member :create actions)
        (push `(lumen.core.router:construct-route (:GET ,(route-spec base-new) (req) :host ,host)
		 (%handle-htmx-new req ',entity-sym ,base-new ,base-root ,auth-guard :title ,title))
              forms))
      
      ;; 1. INDEX (GET /tasks)
      (when (member :index actions)
        (push `(lumen.core.router:construct-route (:GET ,(route-spec base-root) (req) :host ,host)
		 (%handle-htmx-index req ',entity-sym ,base-root ,auth-guard ',order-whitelist 
                                     :columns ,columns :title ,title))
              forms))

      ;; 2. CREATE ACTION (POST /tasks)
      (when (member :create actions)
        (push `(lumen.core.router:construct-route (:POST ,(route-spec base-root) (req) :host ,host)
                 (%handle-htmx-create req ',entity-sym ,base-root ,base-root ,auth-guard))
              forms))

      ;; 3. SHOW
      ;; Note : L'URL est la même que PUT/DELETE (/tasks/:id), mais en GET.
      (when (member :show actions)
        (push `(lumen.core.router:construct-route (:GET ,(route-spec base-item) (req id) :host ,host)
		 (%handle-htmx-show req ',entity-sym ,base-root ,auth-guard id :title ,title))
              forms))

      ;; 4. UPDATE ACTION (PUT /tasks/:id)
      (when (member :edit actions)
        (push `(lumen.core.router:construct-route (:PUT ,(route-spec base-item) (req id) :host ,host)
                   (%handle-htmx-update req ',entity-sym ,base-root ,base-root ,auth-guard id))
              forms))

      ;; 5. DELETE ACTION (DELETE /tasks/:id)
      (when (member :delete actions)
        (push `(lumen.core.router:construct-route (:DELETE ,(route-spec base-item) (req id) :host ,host)
		 (%handle-htmx-delete req ',entity-sym ,auth-guard id))
              forms))

      ;; 6. EDIT FORM (GET /tasks/:id/edit)
      (when (member :edit actions)
        (push `(lumen.core.router:construct-route (:GET ,(route-spec base-edit) (req id) :host ,host)
		(%handle-htmx-edit req ',entity-sym ,base-root ,base-root ,auth-guard id :title ,title))
              forms))
      )

    ;; On retourne la liste dans le bon ordre pour defmodule
    (nreverse forms)))
