(defpackage :lumen.admin.tools
  (:use :common-lisp :spinneret :lumen.utils)
  (:export :render-sql-console))

(in-package :lumen.admin.tools)

(defun render-sql-console (req &optional result error)
  (lumen.admin.view:render-admin-layout req
   :title "Console SQL"
   :content
   (with-html-string
     (:div :class "row"
       (:div :class "col-12"
         (:div :class "alert alert-warning" 
               (:i :class "bi bi-exclamation-triangle-fill me-2")
               "Zone dangereuse. Les requêtes sont exécutées directement sur la base.")
         
         (:form :method "POST" :action "/admin/sql"
           (:div :class "mb-3"
             (:textarea :class "form-control font-monospace bg-dark text-light" 
                        :name "query" :rows "5" :placeholder "SELECT * FROM users..."
                        (lumen.core.http:ctx-get req :last-query))) ;; Pour garder la requête
           (:button :type "submit" :class "btn btn-dark" "Exécuter SQL"))
         
         (when error
           (:div :class "alert alert-danger mt-3" error))
         
         (when result
           (:div :class "card mt-4 shadow-sm"
             (:div :class "card-header" (format nil "~A résultats" (length result)))
             (:div :class "table-responsive"
               (:table :class "table table-sm table-striped mb-0 font-monospace"
                 (let ((cols (mapcar #'car (first result)))) ;; Clés de la première ligne
                   (:thead (:tr (dolist (k cols) (:th k))))
                   (:tbody 
                     (dolist (row result)
                       (:tr (dolist (k cols) 
                              (:td (spinneret::escape-string 
                                    (princ-to-string (alist-get row k))))))))))))))))))
