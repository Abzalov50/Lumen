(in-package :cl)

(defpackage :lumen.core.tls-acme
  (:use :cl)
  (:import-from :lumen.core.http :respond-text :response)
  (:export :acme-challenge))
(in-package :lumen.core.tls-acme)

(defmacro define-middleware (name (req-sym next-sym) &body body)
  `(defun ,name (,next-sym) (lambda (,req-sym) ,@body)))

(defun %trim (s) (string-trim '(#\/) s))

(define-middleware acme-challenge (req next)
  (let* ((root (or (lumen.core.config:cfg-get :tls/acme-root) ""))
         (path (lumen.core.http:req-path req)))
    (if (and root (search "/.well-known/acme-challenge/" path))
        (let* ((token (subseq path (1+ (position #\/ path :from-end t))))
               (file  (merge-pathnames token
                                       (truename (ensure-directories-exist root)))))
          (if (and (probe-file file) (pathnamep file))
              (make-instance 'response
                             :status 200
                             :headers '(("Content-Type" . "text/plain; charset=utf-8"))
                             :body (with-open-file (in file :direction :input :external-format :utf-8)
                                     (let ((s (make-string (file-length in))))
                                       (read-sequence s in) s)))
              (funcall next req)))
        (funcall next req))))
