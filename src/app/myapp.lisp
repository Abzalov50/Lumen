(in-package :cl)

(defpackage :myapp
  (:use :cl)
  (:import-from :lumen.core.http :respond-text :respond-json :respond-404)
  (:import-from :lumen.core.middleware :my-compose :*app* :logger :json-only :query-parser :set-app! :->mw :static :cors)
  (:import-from :lumen.core.router :defroute :router-dispatch :with-params)
  (:import-from :lumen.core.body :body-parser)
  (:import-from :lumen.core.server :start :stop)
  (:import-from :lumen.mvc.resource :defresource)
  (:export :boot :halt))

(in-package :myapp)

;; Active le debug si besoin
(setf lumen.core.middleware:*debug* t)  ;; ou NIL en prod

(defparameter *prj-dir* (asdf:component-pathname (asdf:find-system :lumen)))
(defparameter *src-dir* (merge-pathnames #P"src/" *prj-dir*))
(defparameter *app-dir* (merge-pathnames #P"app/" *src-dir*))

;; Routes simples
(defroute GET "/favicon.ico" ()
  (respond-404 "no favicon"))
(defroute GET "/health" () (respond-text "ok"))
(defroute GET "/hello/:name" ()
  (with-params (name)
    (respond-json (list :message (format nil "Bonjour, ~A" name)))))

(defroute GET "/hello/:name/:age" ()
  (with-params (name age)
    (respond-json (list :message (format nil "Bonjour, ~A (~A ans)" name age)))))

#|
(defroute GET "/health-json" ()
  (respond-json '((:status . "ok") (:uptime . 123))))
|#

(defroute GET "/health-json" (req)
  (let ((q (lumen.core.http:req-query req))) ; q = alist
    (format t "~&[diag] req-query type: ~A~%" (type-of (lumen.core.http:req-query req)))
    (respond-json `((:status . "ok")
                    (:echo   . ,q)))))

(defroute GET "/big-json" ()
  (let* ((items (loop for i from 1 to 200
                      collect (list (cons :id i)
                                    (cons :name (format nil "item-~D" i))))))
    (respond-json (list (cons :status "ok")
                        (cons :count (length items))
                        (cons :items items)))))

;; Route de diagnostic
(defroute GET "/_pipeline" ()
  (lumen.core.http:respond-json
   `((:stack . ,lumen.core.middleware:*pipeline-signature*))))

;; Ressource REST de démo
(defresource user :path "/api/users")

;; Dossier statique
(defparameter *static* (lumen.core.middleware:static
                        :prefix "/public"
                        :dir (merge-pathnames #P"public/" *app-dir*)
                        :auto-index t
                        :try-index "index.html"
                        :redirect-dir t))

(format t "~&[lumen] routes count = ~D~%" (length lumen.core.router::*routes*))

(defun boot (&key (port 8080))
  (lumen.core.server:start :port port
			   :handler (lambda (req)
				      (format t "~&IN MY APP HANDLER...~%")
				      (funcall lumen.core.middleware:*app* req))))

(defun boot (&key (port 8080))
  ;; Compose la pile
  (lumen.core.middleware:set-app!
   (->mw "cors"    (lumen.core.middleware:cors))
   (->mw "query-parser"   #'lumen.core.middleware:query-parser)
   (->mw "json-only(/api)" (lumen.core.middleware:json-only :prefixes '("/api")))
   (->mw "logger"         #'lumen.core.middleware:logger)
   (->mw "body-parser"    #'lumen.core.body:body-parser)
   (->mw "router-dispatch" #'lumen.core.router:router-dispatch)
   (->mw "static"  *static*))

  ;; Affiche la pile au boot
  (format t "~&[lumen] pipeline: ~{~A~^ -> ~}~%" lumen.core.middleware:*pipeline-signature*)

  ;; Démarre le serveur
  (lumen.core.server:start
   :port port
   :handler (lambda (req)
              (funcall lumen.core.middleware:*app* req))))

(defun halt ()
  (lumen.core.server:stop))

;; Astuce REPL : (myapp:boot :port 8080)
