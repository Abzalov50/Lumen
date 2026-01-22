(in-package :cl)

(defpackage :lumen.core.error
  (:use :cl)
  (:export :http-error :make-http-error :*error-handler*
   :application-error :db-safe-business-error
   :application-error-message :application-error-code))

(in-package :lumen.core.error)


(define-condition http-error (error)
  ((status :initarg :status :reader http-status)
   (payload :initarg :payload :reader http-payload))
  (:report (lambda (c s) (format s "HTTP ~A ~S" (http-status c) (http-payload c)))))

(define-condition application-error (error)
  ((message :initarg :message 
            :reader application-error-message
            :type string)
   (code    :initarg :code 
            :reader application-error-code
            :initform 400)) ;; Par défaut 400 Bad Request
  
  ;; Le :report est crucial pour que (format nil "~A" err) fonctionne
  (:report (lambda (condition stream)
             (format stream "~A" (application-error-message condition))))
  (:documentation "Erreur métier qui ne doit pas être capturée comme une erreur système."))


(defun make-http-error (status &optional (payload nil))
  (make-condition 'http-error :status status :payload payload))


(defparameter *error-handler*
  (lambda (e)
    (declare (ignore e))
    (values 500
	    '(("Content-Type" . "application/json; charset=utf-8"))
	    (cl-json:encode-json-to-string
	     (list :error (list :type "internal" :message "Internal Server Error"))))))

(define-condition db-safe-business-error (lumen.core.error:application-error)
  ((message :initarg :message :reader db-error-message))
  (:report (lambda (condition stream)
             (format stream "~A" (db-error-message condition)))))
