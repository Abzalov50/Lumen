(in-package :cl)

(defpackage :lumen.core.context
  (:use :cl :alexandria)
  (:import-from :uiop :getenv)
  (:export
   :*current-app*))

(in-package :lumen.core.context)

(defvar *current-app* nil "L'instance de l'application en cours de traitement.")

