(in-package :cl)

(defpackage :lumen.core.shutdown
  (:use :cl)
  (:import-from :bordeaux-threads :make-lock :with-lock-held)
  (:export
   :with-graceful-shutdown
   :request-shutdown
   :draining-p
   :register-connection :unregister-connection
   :register-sse :unregister-sse
   :register-ws  :unregister-ws))

(in-package :lumen.core.shutdown)

(defparameter *draining?* nil)
(defparameter *deadline*  0)
(defparameter *lock* (make-lock "lumen-shutdown"))

(defparameter *connections* (make-hash-table :test 'eq))
(defparameter *sse*         (make-hash-table :test 'eq))
(defparameter *ws*          (make-hash-table :test 'eq))

(defun draining-p () *draining?*)
(defun %now () (truncate (/ (get-internal-real-time) internal-time-units-per-second)))

;; --- FONCTIONS SÉCURISÉES PAR VERROU (THREAD-SAFE) ---

(defun register-connection (token) 
  (with-lock-held (*lock*)
    (setf (gethash token *connections*) t))
  token)

(defun unregister-connection (token) 
  (with-lock-held (*lock*)
    (remhash token *connections*))
  (values))

(defun register-sse (token) 
  (with-lock-held (*lock*)
    (setf (gethash token *sse*) t))
  token)

(defun unregister-sse (token) 
  (with-lock-held (*lock*)
    (remhash token *sse*))
  (values))

(defun register-ws (token) 
  (with-lock-held (*lock*)
    (setf (gethash token *ws*) t))
  token)

(defun unregister-ws (token) 
  (with-lock-held (*lock*)
    (remhash token *ws*))
  (values))

(defun request-shutdown (&key (grace-seconds 20))
  (with-lock-held (*lock*)
    (setf *draining?* t
          *deadline* (+ (%now) grace-seconds)))
  (values))

(defun %past-deadline-p () (and *deadline* (>= (%now) *deadline*)))

(defun %close-all (ht close-fn)
  (maphash (lambda (k v) (declare (ignore v)) (ignore-errors (funcall close-fn k))) ht)
  (clrhash ht))

(defun %drain-loop ()
  (loop until (%past-deadline-p) do (sleep 0.2))
  ;; On ferme tout ce qui reste (SSE/WS/sockets)
  (%close-all *sse* #'close)
  (%close-all *ws*  #'close)
  (%close-all *connections* #'close)
  (values))

(defmacro with-graceful-shutdown ((&key (signals '(:int :term)) (grace-seconds 20)) &body body)
  "Installe des handlers de signaux; passe en mode vidange et coupe tout à l’échéance."
  `(let ((installers
          (handler-case
              (progn
                (require :trivial-signal)
                (loop for s in ,signals
                      collect (cons s
                                    (funcall (symbol-function
                                              (intern "INSTALL-SIGNAL-HANDLER" :trivial-signal))
                                             s
                                             (lambda (&rest _)
                                               (declare (ignore _))
                                               (request-shutdown :grace-seconds ,grace-seconds))))))
            (error () nil))))
     (unwind-protect
          (progn ,@body)
       (when *draining?* (%drain-loop)))))
