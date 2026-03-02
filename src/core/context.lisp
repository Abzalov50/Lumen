(in-package :cl)

(defpackage :lumen.core.context
  (:use :cl :alexandria)
  (:import-from :uiop :getenv)
  (:export
   :*current-app* :*current-db-config* :*db-pool*))

(in-package :lumen.core.context)

(defvar *current-app* nil "L'instance de l'application en cours de traitement.")
(defvar *current-db-config* nil "La config de la Base de Données utilisée par la requête en cours.")
(defvar *db-pool* nil "Pool de connexions DB utilisé par la requête en cours.")

