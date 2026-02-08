(defpackage :lumen.admin.utils
  (:use :common-lisp)
  (:import-from :lumen.data.dao :*entity-registry*)
  (:export :resolve-entity-symbol :table-to-entity))

(in-package :lumen.admin.utils)

(defun resolve-entity-symbol (name-str)
  "Trouve le symbole de l'entité dans le registre à partir de son nom (String).
   Ex: 'user' -> MY-APP:USER"
  (let ((target (string-upcase name-str)))
    (maphash (lambda (key val)
               (declare (ignore val))
               ;; On compare le nom du symbole (sans le package)
               (when (string= (symbol-name key) target)
                 (return-from resolve-entity-symbol key)))
             *entity-registry*)
    ;; Si non trouvé, on lève une erreur explicite ou on retourne nil
    (error "Entité introuvable dans le registre : ~A" name-str)))

(defun table-to-entity (table-name)
  "Retourne le symbole de l'entité (ex: :user) correspondant au nom de la table SQL."
  (let ((target-table (string-downcase (string table-name))))
    
    ;; On parcourt tous les modules
    (maphash 
     (lambda (mod-key mod-def)
       (declare (ignore mod-key))
       (let ((entities (getf mod-def :entities)))
         ;; On parcourt toutes les entités du module
         (dolist (ent-sym entities)
           ;; Structure ent-def : (SYMBOL :table "nom" ...)
           (let* ((props   (gethash ent-sym lumen.data.dao::*entity-registry*))
                  (tbl     (getf props :table)))
             
             ;; Si on trouve la table, on retourne immédiatement le symbole
             (when (and tbl (string= (string-downcase tbl) target-table))
               (return-from table-to-entity ent-sym))))))
     
     (lumen.app.app:app-modules lumen.core.context:*current-app*))))
