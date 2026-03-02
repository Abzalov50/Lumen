(defpackage :lumen.admin.dashboard
  (:use :common-lisp :spinneret :lumen.utils)
  (:export :register-kpi :get-kpis :render-kpi-cards))

(in-package :lumen.admin.dashboard)

(defstruct kpi
  id label icon color provider-fn)

(defvar *kpi-registry* (make-hash-table :test 'eq))

(defun register-kpi (id label icon color provider-fn)
  "Enregistre un indicateur. provider-fn doit retourner un entier ou une string."
  (setf (gethash id *kpi-registry*)
        (make-kpi :id id :label label :icon icon :color color :provider-fn provider-fn)))

(defun get-kpis ()
  "Récupère et calcule tous les KPIs."
  (let ((list nil))
    (maphash (lambda (k v) 
               ;; On exécute la fonction provider ici
               (let ((val (funcall (kpi-provider-fn v))))
                 (push (list :label (kpi-label v)
                             :icon (kpi-icon v)
                             :color (kpi-color v)
                             :value val)
                       list)))
             *kpi-registry*)
    ;; Tri alphabétique pour la stabilité
    (sort list #'string< :key (lambda (x) (getf x :label)))))

(defun render-kpi-cards (kpis)
  "Génère le HTML pour une rangée de cartes KPI.
   KPIS est une liste de plists : ((:title 'Users' :value '120' :icon 'bi-people' :color 'primary' ...))"
  (with-html-string
    (:div :class "row g-3 mb-4"
      (dolist (kpi kpis)
        (let ((title (getf kpi :title))
              (value (getf kpi :value))
              (icon  (getf kpi :icon))
              (color (or (getf kpi :color) "primary")) ;; primary, success, warning, danger
              (trend (getf kpi :trend))                ;; ex: "+5%"
              (trend-dir (getf kpi :trend-dir)))       ;; :up, :down, :neutral
          
          (:div :class "col-12 col-sm-6 col-xl-3"
            (:div :class "card shadow-sm border-0 h-100"
              (:div :class "card-body d-flex align-items-center"
                
                ;; 1. L'ICÔNE (Cercle coloré avec opacité)
                (:div :class (format nil "rounded-circle bg-~A bg-opacity-10 p-3 me-3 d-flex align-items-center justify-content-center" color)
                      :style "width: 64px; height: 64px;"
                  (:i :class (format nil "bi ~A text-~A fs-3" icon color)))
                
                ;; 2. LE TEXTE
                (:div
                  (:h6 :class "card-subtitle text-muted text-uppercase mb-1" 
                       :style "font-size: 0.75rem; letter-spacing: 1px;"
                       title)
                  (:h3 :class "card-title mb-0 fw-bold" value)
                  
                  ;; 3. LA TENDANCE (Optionnel)
                  (when trend
                    (:div :class "mt-1 small"
                      (cond
                        ((eq trend-dir :up)
                         (:span :class "text-success fw-bold" 
                                (:i :class "bi bi-arrow-up-short") trend))
                        ((eq trend-dir :down)
                         (:span :class "text-danger fw-bold" 
                                (:i :class "bi bi-arrow-down-short") trend))
                        (t 
                         (:span :class "text-muted" trend)))
                      (:span :class "text-muted ms-1" "vs mois dernier"))))))))))))
