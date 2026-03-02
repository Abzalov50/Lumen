(defpackage :lumen.admin.utils
  (:use :common-lisp)
  (:import-from :lumen.data.dao :*entity-registry*)
  (:export :resolve-entity-symbol :table-to-entity :normalize-post-params))

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
