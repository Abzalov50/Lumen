(defpackage :lumen.view.form
  (:use :cl :spinneret :lumen.data.db :lumen.utils)
  (:import-from :lumen.data.dao :entity-fields :entity-metadata)
  (:export :render-entity-form))

(in-package :lumen.view.form)

;;; ----------------------------------------------------------------------------
;;; Récupération des Options DB (Foreign Keys)
;;; ----------------------------------------------------------------------------
(defun %fetch-reference-options (table-name label-col)
  "Exécute: SELECT id, label-col FROM table-name"
  ;; Attention aux injections SQL ici si table-name vient de l'utilisateur.
  ;; Comme ça vient de defentity (code développeur), c'est considéré "sûr".
  (lumen.data.db:ensure-connection
    (let* ((sql (format nil "SELECT id, ~A FROM ~A ORDER BY ~A ASC" 
			label-col table-name label-col))
	   (rows (pomo:query sql))
	   (rows (mapcar #'(lambda (x)
			     (cons (car x) (cadr x)))
			 rows))
	   )
      
      rows)))

;;; 1. UTILITAIRES DE MAPPING TYPE -> HTML

(defun %lisp-type->html-type (type)
  "Convertit le type de champ Lumen en type d'input HTML."
  (case type
    (:integer "number")
    (:float   "number")
    (:email   "email")
    (:date    "date")
    (:time    "time")
    (:password "password")
    (:boolean "checkbox")
    (t "text")))

(defun %get-field-value (col-name values)
  "Récupère la valeur dans l'alist payload (supporte key string ou keyword)."
  (let ((val (or (cdr (assoc col-name values :test #'eq))         ;; Keyword
                 (cdr (assoc (string-downcase col-name) values :test #'equalp))))) ;; String
    val))

(defun %has-error-p (col-name errors)
  (when errors
    (lumen.utils:col-get errors (string-downcase col-name))))

;;; 2. RENDU DES WIDGETS INDIVIDUELS

(defun %render-input-field (field val has-error)
  (let* ((col   (getf field :col))
         (name  (string-downcase col))
         (type  (getf field :type))
         (ftype (%lisp-type->html-type type))
         (req?  (getf field :required?))
         (min   (getf field :min))
         (max   (getf field :max))
         (step  (getf field :step))
         (attrs (getf field :attrs))
         
         ;; --- CONVERSION DATE ICI ---
         ;; Si c'est une date, on force le format YYYY-MM-DD.
         ;; Sinon, on garde la valeur brute.
         (final-val (if (and val (eq type :date))
                        (%val->date-input val)
                        val)))

    (if (eq type :boolean)
        ;; Cas Spécial : Checkbox
        (with-html
          (:div :class "form-check mb-3"
            (:input :type "checkbox" 
                    :name name 
                    :id name 
                    :class (format nil "form-check-input ~A" (if has-error "is-invalid" ""))
                    ;; Checkbox utilise l'attribut :checked, pas :value
                    :checked (if val t nil) 
                    :disabled (getf field :disabled?)
                    :attrs attrs)
            (:label :class "form-check-label" :for name (getf field :label))))
        
        ;; Cas Général : Input standard
        (with-html
          (:input :type ftype 
                  :name name 
                  :id name
                  :class (format nil "form-control ~A" (if has-error "is-invalid" ""))
                  ;; On utilise final-val ici
                  :value final-val
                  :placeholder (getf field :placeholder)
                  :required req?
                  :min min :max max :step step
                  :readonly (getf field :readonly?)
                  :disabled (getf field :disabled?)
                  :attrs attrs)))))

(defun %render-select-field (field val has-error)
  (let* ((col       (getf field :col))
         (name      (string-downcase col))
         (req?      (getf field :required?))
         ;; 1. Choix statiques définis dans defentity (:choices)
         (static-opts (getf field :choices))
         ;; 2. Choix dynamiques via FK (:references)
         (ref-table   (getf field :references))
         (ref-col     (getf field :ref-col))
         (db-opts     (when ref-table 
                        (%fetch-reference-options ref-table (or ref-col "name"))))
         ;; On fusionne (priorité aux options DB, ou on cumule ?)
	 (x (print static-opts))
	 (x (print db-opts))
	    
         (final-opts  (append static-opts db-opts)))
    
    (with-html
      (:select :name name 
               :id name
               :class (format nil "form-select ~A" (if has-error "is-invalid" ""))
               :required req?
        ;; Option vide
        (:option :value "" :selected (null val) "Sélectionner...")

	
        (dolist (c final-opts)
          (let ((opt-val (if (consp c) (car c) c))
                (opt-lbl (if (consp c) (cdr c) c)))
            (:option :value opt-val 
                     ;; Comparaison robuste (UUID string vs UUID obj vs Integer)
                     :selected (string-equal (format nil "~A" opt-val) 
                                             (format nil "~A" val))
                     opt-lbl)))))))

(defun %render-textarea-field (field val has-error)
  (let* ((col   (getf field :col))
         (name  (string-downcase col))
         (req?  (getf field :required?))
         (attrs (getf field :attrs)))
    
    (with-html
      (:textarea :name name 
                 :id name
                 :class (format nil "form-control ~A" (if has-error "is-invalid" ""))
                 :required req?
                 :rows (or (getf attrs :rows) 5) ;; Défaut 5 lignes
                 :placeholder (getf field :placeholder)
                 :readonly (getf field :readonly?)
                 :disabled (getf field :disabled?)
                 ;; Pour textarea, la valeur est le contenu du tag, pas un attribut value
                 (or val "")))))

;;; 3. GÉNÉRATEUR PRINCIPAL

(defun render-entity-form (entity-sym &key values errors action (method :POST) 
                                           (submit-text "Enregistrer") 
                                           cancel-url
                                           hx-target hx-swap hx-on-success)
  (let ((fields (lumen.data.dao:entity-fields entity-sym)))
    (with-html
      (:form :method "POST" :action action
             :class "needs-validation"
             :novalidate t
             :hx-post (when (eq method :POST) action)
             :hx-put  (when (or (eq method :PATCH) (eq method :PUT)) action)
             :hx-target hx-target 
             :hx-swap hx-swap
             :hx-on--after-request (when hx-on-success (format nil "if(event.detail.successful) ~A" hx-on-success))
         
        (dolist (field fields)
          ;; On ignore les champs techniques et hidden
          (unless (or (getf field :hidden?) (getf field :readonly?)
                      (member (getf field :col) '(:id :created-at :updated-at :tenant-id)))
            
            (let* ((col-name (getf field :col))
                   (label    (or (getf field :label) (string-capitalize (string-downcase col-name))))
                   (val      (lumen.utils:lookup values col-name))
                   (err      (%has-error-p col-name errors))
                   (choices    (getf field :choices))
                   (references (getf field :references))
                   (input-type (getf field :input-type))
                   (type       (getf field :type)))
	      
              (:div :class "mb-3"
                ;; Pour les inputs classiques, on affiche le label ici
                ;; (Pour les booléens, le label est souvent à côté de la checkbox dans le helper)
                (unless (eq type :boolean)
                  (:label :for (string-downcase col-name) :class "form-label" label))
                
                ;; --- DISPATCHER INTELLIGENT ---
                (cond
                  ;; 1. SELECT (Statique ou FK)
                  ((or choices references)
                   (%render-select-field field val err))
                  
                  ;; 2. TEXTAREA
                  ((eq input-type :textarea)
                   (%render-textarea-field field val err))
                  
                  ;; 3. INPUT STANDARD (y compris Boolean/Checkbox)
                  (t
                   (%render-input-field field val err)))
                
                ;; Feedback erreur
                (when err (:div :class "invalid-feedback d-block" (first err)))
                ;; Aide
                (when (getf field :help) (:div :class "form-text" (getf field :help)))))))

        ;; --- ZONE BOUTONS ---
        (:div :class "mt-4 d-flex align-items-center gap-2"
          ;; Bouton Submit
          (:button :type "submit" :class "btn btn-primary"
                   (:i :class "bi bi-check-lg") " " submit-text)
          
          ;; 2. BOUTON ANNULER (AJOUT)
          (when cancel-url
            (:a :href cancel-url
                :class "btn btn-link text-decoration-none text-muted"
                ;; Si on est dans un contexte HTMX, on peut vouloir éviter le rechargement complet
                ;; :hx-get cancel-url :hx-target "#main-content" 
                "Annuler")))))))
