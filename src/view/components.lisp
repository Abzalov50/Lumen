(in-package :cl)

(defpackage :lumen.view.components
  (:use :cl :spinneret :lumen.utils)
  (:import-from :lumen.core.http 
                :req-query :ctx-from-req :respond-html :respond-htmx-redirect
                :req-path :req-method :respond-404 :respond-422)
  (:import-from :lumen.utils :alist-get :to-kebab-case)  
  (:export :render-filter-widget))

(in-package :lumen.view.components)

(defun render-filter-widget (field current-filters)
  "Génère le widget HTML approprié pour un champ donné (Input, Select, etc.).
current-filters est l'alist complète des filtres actifs."
  (let* ((col     (getf field :col))
         (name    (string-downcase col)) ;; ex: "status"
         (label   (or (getf field :label) (string-capitalize name)))
         (type    (getf field :type))
         (choices (getf field :choices))) ;; ex: (("todo" . "À faire") ...)

    (with-html
      (:div :class "col-md-3"
        (:label :class "form-label small text-muted" :for name label)
        
        (cond
          ;; CAS 1 : MULTI-SELECT (Enum)
          ;; Le navigateur envoie status=A&status=B.
          ;; Dans current-filters, on peut avoir plusieurs entrées pour la même clé.
          (choices
           (let* ((selected-values 
                   ;; On récupère toutes les valeurs pour cette clé
                   (loop for (k . v) in current-filters
                         when (string= k name) collect v)))
             (:select :class "form-select form-select-sm" 
                      :name name :id name 
                      :multiple "multiple" ;; <-- MULTI
                      :size (min 5 (1+ (length choices))) ;; Hauteur auto adaptée
                      
               (dolist (c choices)
                 (let ((val (format nil "~A" (car c)))
                       (disp (cdr c)))
                   (if (member val selected-values :test #'string-equal)
                       (:option :value val :selected t disp)
                       (:option :value val disp)))))))

          ;; CAS 2 : INTERVALLES (Date & Nombres)
          ;; On génère deux champs : name_gte (Min) et name_lte (Max)
          ((member type '(:integer :float :number :date :datetime :timestamp))
           (let* ((name-min (format nil "~A_gte" name))
                  (name-max (format nil "~A_lte" name))
                  (val-min  (cdr (assoc name-min current-filters :test #'string=)))
                  (val-max  (cdr (assoc name-max current-filters :test #'string=)))
                  (is-date  (member type '(:date :datetime :timestamp)))
                  (input-type (if is-date "date" "number")))
             
             (:div :class "input-group input-group-sm"
               ;; Min
               (:input :type input-type :class "form-control" 
                       :name name-min :placeholder (if is-date "Du..." "Min") 
                       :value val-min)
               ;; Séparateur visuel
               (:span :class "input-group-text text-muted" "à")
               ;; Max
               (:input :type input-type :class "form-control" 
                       :name name-max :placeholder (if is-date "Au..." "Max") 
                       :value val-max))))

          ;; CAS 3 : BOOLÉEN (Reste un select simple ou Radio)
          ((eq type :boolean)
           (let ((val (cdr (assoc name current-filters :test #'string=))))
             (:select :class "form-select form-select-sm" :name name
               (:option :value "" "Tout")
               (:option :value "true"  :selected (string-equal val "true") "Oui")
               (:option :value "false" :selected (string-equal val "false") "Non"))))

          ;; CAS 4 : TEXTE (Recherche "Contient")
          (t
           (let ((val (cdr (assoc name current-filters :test #'string=))))
             (:input :type "text" :class "form-control form-control-sm" 
                     :name name :value val))))))))
