(in-package :cl)

(defpackage :lumen.view.components
  (:use :cl :spinneret :lumen.utils)
  (:import-from :lumen.core.http 
                :req-query :ctx-from-req :respond-html :respond-htmx-redirect
                :req-path :req-method :respond-404 :respond-422)
  (:import-from :lumen.utils :alist-get :to-kebab-case)  
  (:export :render-filter-widge :render-input :respond-with-htmx-events
   :set-flash-toast :inject-flash-toast
   :render-tabs :render-form-widget :render-metadata-tabs))

(in-package :lumen.view.components)

(defun render-input (name label &key (value "") (type "text") (required nil) (placeholder ""))
  "Génère un groupement label + input Bootstrap 5 standard."
  (spinneret:with-html
    (:div :class "mb-3"
          (:label :for name :class "form-label fw-bold small text-muted" label)
          (:input :type type 
                  :name name 
                  :id name 
                  :value (or value "")
                  :class "form-control shadow-sm"
                  :required required
                  :placeholder placeholder))))

(defun render-filter-widget (field current-filters &key hx-get hx-target hx-include)
  "Génère le widget HTML approprié pour un champ donné avec support HTMX temps réel.
   hx-get, hx-target, hx-include sont les attributs pour le rechargement partiel."
  (let* ((col     (getf field :col))
         (name    (string-downcase col)) ;; ex: "status"
         (label   (or (getf field :label) (string-capitalize name)))
         (type    (getf field :type))
         ;; On s'attend à ce que :choices soit une liste de paires (val . label) ou de valeurs simples
         (choices (getf field :choices)))

    (spinneret:with-html
      (:div :class "col-md-3"
            (:label :class "form-label small text-muted fw-bold" :for name label)
            
            (cond
              ;; --- CAS 1 : SELECT SIMPLE ou MULTI (Si choices fourni) ---
              ;; Pour le multi-select, on n'utilise PAS le mode temps réel (hx-trigger="change")
              ;; car c'est pénible pour l'utilisateur. On laisse le bouton "Filtrer" (submit implicite)
              ;; ou on met un délai. Ici, on reste sur du select simple pour l'exemple.
              (choices
               (let* ((selected-val (cdr (assoc name current-filters :test #'equal))))
                 (:select :class "form-select form-select-sm" 
                          :name name :id name
                          ;; Attributs HTMX pour recharger le tableau au changement
                          :hx-get hx-get 
                          :hx-target hx-target 
                          :hx-include hx-include
                          :hx-trigger "change"
                          
                          (:option :value "" "Tout")
                          (dolist (c choices)
                            (let* ((val (if (consp c) (car c) c))
                                   (lbl (if (consp c) (cdr c) c))
                                   (val-str (format nil "~A" val)))
                              (:option :value val-str
                                       :selected (equal val-str selected-val)
                                       lbl))))))

              ;; --- CAS 2 : INTERVALLES (Date & Nombres) ---
              ;; Génère deux champs : min (gte) et max (lte)
              ((member type '(:integer :float :number :date :datetime :timestamp))
               (let* ((name-min (format nil "~A_gte" name))
                      (name-max (format nil "~A_lte" name))
                      ;; Récupération des valeurs actuelles
                      (val-min  (cdr (assoc name-min current-filters :test #'equal)))
                      (val-max  (cdr (assoc name-max current-filters :test #'equal)))
                      (is-date  (member type '(:date :datetime :timestamp)))
                      (input-type (if is-date "date" "number")))
                 
                 (:div :class "input-group input-group-sm"
                       ;; Min
                       (:input :type input-type :class "form-control" 
                               :name name-min 
                               :placeholder (if is-date "Du..." "Min") 
                               :value val-min
                               ;; HTMX : délai pour éviter le spam pendant la saisie
                               :hx-get hx-get :hx-target hx-target :hx-include hx-include
                               :hx-trigger "keyup changed delay:800ms")
                       
                       ;; Séparateur
                       (:span :class "input-group-text text-muted bg-light" "-")
                       
                       ;; Max
                       (:input :type input-type :class "form-control" 
                               :name name-max 
                               :placeholder (if is-date "Au..." "Max") 
                               :value val-max
                               :hx-get hx-get :hx-target hx-target :hx-include hx-include
                               :hx-trigger "keyup changed delay:800ms"))))

              ;; --- CAS 3 : BOOLÉEN ---
              ((eq type :boolean)
               (let ((val (cdr (assoc name current-filters :test #'equal))))
                 (:select :class "form-select form-select-sm" :name name
                          :hx-get hx-get :hx-target hx-target :hx-include hx-include
                          :hx-trigger "change"
                          
                          (:option :value "" "Tout")
                          (:option :value "true"  :selected (equal val "true") "Oui")
                          (:option :value "false" :selected (equal val "false") "Non"))))

              ;; --- CAS 4 : TEXTE (Recherche "ILike") ---
              (t
               (let ((val (cdr (assoc name current-filters :test #'equal))))
                 (:input :type "text" :class "form-control form-control-sm" 
                         :name name 
                         :value val
                         :placeholder "..."
                         ;; HTMX avec délai
                         :hx-get hx-get :hx-target hx-target :hx-include hx-include
                         :hx-trigger "keyup changed delay:500ms"))))))))

;; -------------------------------
;; Gestion des Toats
;; -------------------------------
;; 1. Pour les actions AVEC rafraîchissement (Redirection ou chargement d'une nouvelle grille)
(defun set-flash-toast (req type message)
  "Mémorise un toast en session pour le prochain affichage de page."
  (lumen.http.session:session-set! req :flash-msg `((:type . ,type) (:message . ,message))))

;; 2. Pour consommer le flash (À utiliser dans tes routes GET comme /rows)
(defun inject-flash-toast (req resp)
  "Injecte le flash de la session dans les headers."
  (let ((flash (lumen.http.session:session-get req :flash-msg)))
    (when flash
      (lumen.core.http:res-set-header! resp "HX-Trigger" 
        (cl-json:encode-json-to-string 
         ;; Ici on construit l'enveloppe finale pour HTMX
         `((:show-message . ,flash))))
      (lumen.http.session:session-set! req :flash-msg nil)))
  resp)

;; 3. Pour les actions IMMÉDIATES (Sans rafraîchissement complet, ex: une suppression inline)
(defun respond-with-htmx-events (html &key toast-type toast-msg close-modal refresh-grid (status 200))
  "Génère une réponse HTML et attache automatiquement les headers HTMX demandés."
  (let ((resp (lumen.core.http:respond-html html :status status))
        (events nil))
    (when toast-msg
      (push `(:show-message . ((:type . ,(or toast-type "info")) 
                               (:message . ,toast-msg))) 
            events))
    (when close-modal (push '(:close-modal . t) events))
    (when refresh-grid (push '(:entity-saved . t) events))
    (when events
      (lumen.core.http:res-set-header! resp "HX-Trigger" 
                                       (cl-json:encode-json-to-string events)))
    resp))

(defun render-form-widget (field)
  "Génère le HTML d'un widget de formulaire à partir de ses métadonnées (plist).
   Propriétés supportées : :type, :name, :label, :value, :width (1-12), :required, :options, etc."
  (let* ((type  (getf field :type :text))
        (name  (getf field :name))
        (label (getf field :label))
        (val   (getf field :value))
        (req   (getf field :required))
        (raw-width (getf field :width 12)) ;; Pleine largeur par défaut (col-12)
	;; Si c'est un nombre, on force 100% sur mobile (col-12) et la largeur souhaitée sur PC (col-md-X)
         (width-class (if (numberp raw-width) 
                          (format nil "col-12 col-md-~A" raw-width)
                          raw-width))
        (opts  (getf field :options)))
    
    (spinneret:with-html
      (:div :class width-class
            (cond
              ;; --- CHAMPS TEXTE & NOMBRES ---
              ((member type '(:text :number :date) :test #'eq)
               (:label :class "form-label fw-bold text-muted small" label)
               (:input :type (string-downcase (symbol-name type)) 
                       :name name :class "form-control shadow-sm" 
                       :required req :value val :placeholder (getf field :placeholder)))
              
              ;; --- ZONE DE TEXTE ---
              ((eq type :textarea)
               (:label :class "form-label fw-bold text-muted small" label)
               (:textarea :name name :class "form-control shadow-sm" 
                          :rows (getf field :rows 3) :required req (or val "")))
              
              ;; --- LISTE DÉROULANTE ---
              ((eq type :select)
               (:label :class "form-label fw-bold text-muted small" label)
               (:select :name name :class "form-select shadow-sm" :required req
                        (when (getf field :empty-option) (:option :value "" "Choisir..."))
                        (dolist (opt opts)
                          (:option :value (car opt) :selected (equal val (car opt)) (cdr opt)))))
              
              ;; --- FICHIER (Upload) ---
              ((eq type :file)
               (:div :class "border rounded p-3 text-center bg-light position-relative shadow-sm"
                     (:label :class "form-label fw-bold text-dark d-block" label)
                     (:input :type "file" :name name :class "form-control form-control-sm" :accept (getf field :accept))
                     (when val 
                       (:div :class "mt-2" (:span :class "badge bg-success" (:i :class "bi bi-check-circle me-1") "Fichier enregistré")))))
              
              ;; --- TABLEAU DYNAMIQUE (1-à-N HTMX) ---
              ((eq type :dynamic-table)
               (:div :class "d-flex justify-content-between align-items-center mb-3 mt-2"
                     (:h6 :class "m-0 text-primary fw-bold" (getf field :title))
                     (:button :type "button" :class "btn btn-sm btn-outline-primary"
                              :hx-get (getf field :add-url)
                              :hx-target (format nil "#~A" (getf field :target-id))
                              :hx-swap "beforeend"
                              (:i :class "bi bi-plus-lg me-1") (getf field :add-label)))
               
               (:div :id (getf field :target-id) :class "w-100"
                     (if val
                         (dolist (row-html val) (:raw row-html))
                         (:div :class "alert alert-light border border-dashed text-center text-muted"
                               "Cliquez sur « Ajouter » pour commencer."))))
              
              ;; --- HTML BRUT CUSTOM ---
              ((eq type :custom) (:raw (getf field :content))))))))

(defun render-metadata-tabs (group-id tabs-meta)
  "Convertit un dictionnaire de métadonnées en un composant à onglets.
   `tabs-meta` est une liste de la forme : ((label-onglet . liste-de-champs) ...)"
  (let ((tabs-html
         (loop for (tab-label . fields) in tabs-meta
               for i from 0
               collect (list :id (format nil "~A-tab-~A" group-id i)
                             :label tab-label
                             :content (spinneret:with-html-string
                                        (:div :class "row g-3"
                                              (dolist (f fields)
                                                (render-form-widget f))))))))
    
    ;; Fait appel au composant `render-tabs` générique défini précédemment
    (render-tabs group-id tabs-html :style :pills :container-class "bg-white p-4 rounded shadow-sm border")))

(defun render-tabs (group-id tabs &key (style :tabs) (container-class "bg-white p-4 rounded shadow-sm border"))
  "Génère un système d'onglets Bootstrap 5 générique.
   `tabs` doit être une liste de property lists (plists) avec :
   - :id (string) identifiant unique de l'onglet
   - :label (string) texte affiché sur le bouton
   - :content (string) le HTML pré-rendu du contenu
   - :active (boolean) optionnel, force cet onglet à s'ouvrir par défaut.
   
   Exemple d'appel :
   (render-tabs \"mon-groupe\" 
      (list (list :id \"tab1\" :label \"Info\" :content \"<p>Hello</p>\")))"
  
  (spinneret:with-html-string
    
    ;; --- 1. BARRE DE NAVIGATION (Les boutons) ---
    (:ul :class (format nil "nav ~A mb-4 flex-column flex-md-row gap-2 gap-md-0 bg-light p-1 rounded border shadow-sm" 
                        (if (eq style :pills) "nav-pills nav-fill" "nav-tabs"))
         :id group-id :role "tablist"
         
         (loop for tab in tabs
               for i from 0
               for id = (getf tab :id)
               for label = (getf tab :label)
               ;; Le premier onglet est actif par défaut si aucun n'est forcé
               for is-active = (or (getf tab :active) (= i 0))
               
               do (:li :class "nav-item" :role "presentation"
                       (:button :class (format nil "nav-link fw-bold ~A" (if is-active "active" ""))
                                :id (format nil "~A-tab" id)
                                :data-bs-toggle "tab"
                                :data-bs-target (format nil "#~A" id)
                                :type "button" :role "tab"
                                label))))
    
    ;; --- 2. CONTENU DES ONGLETS ---
    (:div :class (format nil "tab-content ~A" container-class) :id (format nil "~A-content" group-id)
          
          (loop for tab in tabs
                for i from 0
                for id = (getf tab :id)
                for content = (getf tab :content)
                for is-active = (or (getf tab :active) (= i 0))
                
                do (:div :class (format nil "tab-pane fade ~A" (if is-active "show active" ""))
                         :id id :role "tabpanel"
                         ;; On injecte le HTML pré-calculé sans l'échapper
                         (:raw content))))))
