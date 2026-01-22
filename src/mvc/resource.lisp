(in-package :cl)

(defpackage :lumen.mvc.resource
  (:use :cl)
  (:import-from :lumen.core.http :respond-json :respond-404)
  (:import-from :lumen.core.router :defroute)
  (:export :defresource))

(in-package :lumen.mvc.resource)

(defmacro defresource (name &key path)
  (let* ((base (or path (format nil "/api/~As" (string-downcase (symbol-name name)))))
	 (by-id (format nil "~A/:id" base)))
    `(progn
       (defroute GET ,base ()
	 (respond-json (list :data (list))))
       
       (defroute GET ,by-id ()
	 (let ((id (cdr (assoc "id" (slot-value req 'lumen.core.http::params)
			       :test #'string=))))
	   (if id (respond-json (list :data (list :id id))) (respond-404))))
       
       (defroute POST ,base ()
	 (respond-json (list :data (getf (slot-value req 'lumen.core.http::context)
					 :json))
		       :status 201))
       
       (defroute PUT ,by-id () (respond-json (list :ok t)))
       
       (defroute DELETE ,by-id () (respond-json (list :ok t))))))
