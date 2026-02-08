(defpackage :lumen.admin.registry
  (:use :common-lisp :lumen.utils)
  (:export :admin-view :make-admin-view :*admin-registry* :defadmin
	   :get-view :find-entity-module :view-actions :view-entity
	   :view-icon :view-label :view-actions :view-list-fields :view-form-fields
	   :get-actions :*default-actions*))

(in-package :lumen.admin.registry)

;; Structure de configuration pour une Entité
(defclass admin-view ()
  ((entity      :initarg :entity      :reader view-entity)      ;; Symbole (ex: 'user)
   (label       :initarg :label       :reader view-label)       ;; Nom affiché (ex: "Utilisateurs")
   (icon        :initarg :icon        :reader view-icon)        ;; Icone Bootstrap (ex: "bi-people")
   (list-fields :initarg :list-fields :reader view-list-fields) ;; Colonnes du tableau
   (form-fields :initarg :form-fields :reader view-form-fields) ;; Champs du formulaire
   (actions     :initarg :actions     :reader view-actions)))   ;; Actions de masse

(defvar *admin-registry* (make-hash-table :test 'eq)
  "Mappe un symbole d'entité (ex: :user) vers une instance de admin-view.")

;; Actions par défaut disponibles pour toutes les entités
(defparameter *default-actions*
  `((:delete "Supprimer la sélection" :danger t)
    (:export-csv "Exporter en CSV" :icon "bi-filetype-csv")))

(defun get-view (entity-sym)
  (gethash entity-sym *admin-registry*))

(defun get-actions (entity-sym)
  "Fusionne les actions par défaut et les actions spécifiques."
  (let ((view (get-view entity-sym)))
    (if view
        (append (view-actions view) *default-actions*)
        *default-actions*)))

(defmacro defadmin (entity-sym &rest args)
  "DSL pour configurer l'admin d'une entité."
  `(setf (gethash ,entity-sym *admin-registry*)
         (make-instance 'admin-view 
                        :entity ,entity-sym
                        :label (getf ',args :label)
                        :icon (getf ',args :icon "bi-box")
                        :list-fields (getf ',args :list-display)
                        :form-fields (getf ',args :fields))))
