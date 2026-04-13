(defpackage :lumen.admin.introspection
  (:use :common-lisp :lumen.utils)
  (:import-from :lumen.dev.module :*app-modules*) ;; On accède à la liste globale des modules
  (:export :collect-admin-menu :get-dashboard-stats))

(in-package :lumen.admin.introspection)

(defun guess-icon (entity-name)
  "Devine une icône selon le nom de l'entité."
  (let ((s (string-downcase (string entity-name))))
    (cond 
      ((search "user" s) "bi-people")
      ((search "tenant" s) "bi-buildings")
      ((search "log" s) "bi-list-columns")
      ((search "setting" s) "bi-gear")
      ((search "audit" s) "bi-clipboard-check")
      (t "bi-database")))) ;; Icône par défaut

(defun collect-admin-menu ()
  "Parcourt tous les modules et retourne une structure pour le menu latéral."
  (let ((menu '()))
    (mapcar 
     (lambda (mod-key)
       ;; On ignore les modules système ou sans entités
       (let* ((mod-def (lumen.dev.module:find-module mod-key))
	     (entities (lumen.dev.module:module-meta-entities mod-def))
             (mod-name (lumen.dev.module:module-meta-name mod-def)))
         (format t "~&[ADMIN INTRO] Nom Module: ~A~%" mod-name)
         (when entities
           (let ((items '()))
             (dolist (ent-sym entities)
	       (print ent-sym)
               (let* (;; 1. Check Config Manuelle (defadmin)
                      (view    (lumen.admin.registry:get-view ent-sym))
                      ;; 2. Fallback Zero-Config
                      (label   (if view (lumen.admin.registry:view-label view)
                                   (string-capitalize (string-downcase ent-sym))))
                      (icon    (if view (lumen.admin.registry:view-icon view)
                                   (guess-icon ent-sym))))
                 (format t "~&[ADMIN INTRO] Entité: ~A~%" ent-sym)
                 (push `(:symbol ,ent-sym :label ,label :icon ,icon
			 :href ,(lumen.app.app:app-path
				 (format nil "/admin/list/~A" (string-downcase ent-sym))))
                       items)))
             
             ;; On ajoute le groupe (Module) au menu s'il a des items
             (when items
               (push `(:section ,mod-name :items ,(nreverse items)) menu))))))
     (lumen.app.app:app-modules lumen.core.context:*current-app*))
    
    ;; On trie les modules pour que ce soit stable
    (sort menu #'string< :key (lambda (x) (getf x :section)))))

(defun get-dashboard-stats ()
  "Compte simple des lignes pour chaque table (Demo KPI)."
  ;; À implémenter : faire un count(*) sur chaque entité
  '((:label "Utilisateurs" :value "12" :icon "bi-people" :color "primary")
    (:label "Tenants"      :value "5"  :icon "bi-buildings" :color "success")
    (:label "Erreurs"      :value "0"  :icon "bi-exclamation-triangle" :color "danger")))
