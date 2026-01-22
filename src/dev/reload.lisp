#|
(in-package :lumen.dev)

(defparameter *watch-thread* nil)
(defparameter *watching* nil)

(defun %now () (get-universal-time))

(defun reload-lumen ()
  "Recompile + recharge le système Lumen via ASDF."
  (asdf:load-system :lumen :force t))
|#
