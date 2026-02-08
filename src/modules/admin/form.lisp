(defpackage :lumen.admin.form
  (:use :common-lisp :spinneret :lumen.utils)
  (:export :get-form-fields :render-widget :normalize-post-params))

(in-package :lumen.admin.form)

;; --- 1. RÉCUPÉRATION DES OPTIONS FK ---

(defun %fetch-fk-options (ref-table)
  "Récupère une liste ((id . label)) pour un <select>."
  ;; TODO Phase 4 : Remplacer par un endpoint AJAX pour les grosses tables
  (lumen.data.db:ensure-connection
    (let* ((rows (lumen.data.db:query-a (format nil "SELECT * FROM ~A LIMIT 100" ref-table))))
      (mapcar (lambda (row)
                (cons (alist-get row :id)
                      ;; Même heuristique que pour la Grid
                      (or (alist-get row :name) (alist-get row :label) (alist-get row :email)
                          (format nil "~A ~A" (alist-get row :firstname) (alist-get row :lastname))
                          (alist-get row :code))))
              rows))))

;; --- 2. INTROSPECTION DES CHAMPS ---

(defun get-form-fields (entity-sym &optional record)
  "Retourne la liste des définitions de champs pour le formulaire."
  (let ((fields (lumen.data.dao:entity-fields entity-sym))
        (is-edit (not (null record))))
    
    (loop for f in fields
          ;; On ignore les champs système (ID, timestamps, hashs, tenant_id automatique)
          unless (member (getf f :col) '(:id :created_at :updated_at :pw_hash :pw_salt :pw_iters :tenant_id) :test #'eq)
          collect 
          (let* ((col   (getf f :col))
                 (val   (if record (lumen.utils:lookup record col) nil))
                 (ref   (getf f :references))
                 (type  (getf f :type))
                 (input-type (getf f :input-type))) ;; Ex: :password défini dans l'entité
            
            (list :name col
                  :label (or (getf f :label) (string-capitalize (string-downcase col)))
                  :value val
                  :type  (cond
                           (input-type input-type)    ;; Priorité à la config explicite
                           (ref :select)              ;; C'est une FK
                           ((eq type :boolean) :checkbox)
                           ((eq type :text) :textarea)
                           ((eq type :jsonb) :json)
                           (t :text))                 ;; Défaut
                  :required (and (getf f :required?) 
                                 (not (and is-edit (eq input-type :password)))) ;; PW optionnel en edit
                  :options (when ref (%fetch-fk-options ref))
                  :help (getf f :help))))))

;; --- 3. RENDU DES WIDGETS ---

(defun render-widget (field)
  "Génère le HTML pour un champ donné."
  (let ((name  (string-downcase (getf field :name)))
        (val   (getf field :value))
        (label (getf field :label))
        (type  (getf field :type))
        (req   (getf field :required))
        (opts  (getf field :options))
        (help  (getf field :help)))
    (with-html-string
      (:div :class "mb-3"
        
        ;; LABEL (Sauf pour checkbox qui a son propre layout)
        (unless (eq type :checkbox)
          (:label :class "form-label fw-bold" :for name 
                  label (when req (:span :class "text-danger ms-1" "*"))))
        
        ;; INPUTS : Utilisation de COND au lieu de CASE pour éviter le bug Spinneret
        (cond
          ;; TEXT / PASSWORD / DATE / EMAIL
          ((member type '(:text :password :email :date))
           (:input :type (string-downcase type) 
                   :class "form-control" 
                   :name name 
                   :id name 
                   :value (if (eq type :password) "" val) 
                   :required req))
          
          ;; TEXTAREA
          ((eq type :textarea)
           (:textarea :class "form-control" :name name :id name :rows "4" :required req (or val "")))
          
          ;; SELECT (FK & Enums)
          ((eq type :select)
           (:select :class "form-select" :name name :id name :required req
             (:option :value "" "Sélectionner...")
             (dolist (opt opts)
               (let ((opt-val (car opt)) (opt-lbl (cdr opt)))
                 (if (and val (string= (string opt-val) (string val)))
                     (:option :value opt-val :selected t opt-lbl)
                     (:option :value opt-val opt-lbl))))))
          
          ;; CHECKBOX
          ((eq type :checkbox)
           (:div :class "form-check form-switch"
             (:input :class "form-check-input" :type "checkbox" :name name :id name :checked (if val t nil))
             (:label :class "form-check-label" :for name label)))
          
          ;; JSON
          ((eq type :json) ;; ou :jsonb
           (:textarea :class "form-control font-monospace" :name name :id name :rows "6" 
                      (if val (cl-json:encode-json-to-string val) "{}"))))
        
        ;; HELP TEXT
        (when help
          (:div :class "form-text" help))))))

(defun normalize-post-params (entity-sym params)
  "Nettoie les paramètres POST avant envoi au Repo (Strings vides -> nil, Checkboxes, etc)."
  (loop for (key . val) in params
        collect 
        (let* ((field-def (find key (lumen.data.dao:entity-fields entity-sym) 
                                :key (lambda (f) (string-downcase (string (getf f :col)))) 
                                :test #'string=))
               (type (getf field-def :type)))
          
          (cons key 
                (cond
                  ;; String vide -> NIL (pour éviter les erreurs de parsing sur vide)
                  ((and (stringp val) (zerop (length val))) nil)
                  
                  ;; JSON/JSONB : On parse la string pour en faire un objet Lisp
                  ;; Le DAO se chargera de re-encoder cet objet proprement.
                  ((and (or (eq type :json) (eq type :jsonb)) 
                        (stringp val))
                   (handler-case 
                       (cl-json:decode-json-from-string val)
                     (error () val))) ;; Fallback si l'user a écrit n'importe quoi
                  
                  (t val))))))
