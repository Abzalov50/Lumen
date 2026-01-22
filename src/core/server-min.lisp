;;;; --- server-min.lisp ---
(in-package :cl)

(defpackage :lumen.test
  (:use :cl :alexandria)
  (:export :start :stop))

(in-package :lumen.test)

(defvar *srv* nil)
(defvar *thr* nil)

(defun write-200 (s)
  (let ((head "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
        (body "ok"))
    (write-string head s) (write-string body s) (finish-output s)))

(defun handle (sock client)
  (declare (ignore sock))
  (let ((s (usocket:socket-stream client)))
    (unwind-protect
         (progn
           (format t "~&[ultra] accepted ~S~%" client)
           (let ((head "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
                 (body "ok"))
             (write-string head s)
             (write-string body s)
             (finish-output s))
           (format t "~&[ultra] sent ok~%"))
      ;; Variante : shutdown puis close *socket*, pas le flux
      (ignore-errors (usocket:socket-shutdown client :direction :output))
      (ignore-errors (usocket:socket-close client))
      ;; NE PAS (close s) ici
      )))

(defun loop-accept (socket)
  (loop
    (handler-case
        (multiple-value-bind (client addr port) (usocket:socket-accept socket)
          (declare (ignore addr port))
          (format t "~&[ultra] accept ok: ~S~%" client)
          (bt:make-thread (lambda () (handle socket client)) :name "ultra-client"))
      (error (e)
        (format *error-output* "~&[ultra] ERROR in accept: ~A~%" e)))))

(defun start-ultra (&key (port 54321))
  (when *srv* (ignore-errors (usocket:socket-close *srv*)))
  (setf *srv* (usocket:socket-listen "127.0.0.1" port :reuse-address t))
  (format t "~&[ultra] listening on 127.0.0.1:~A~%" port)
  (setf *thr* (bt:make-thread (lambda () (loop-accept *srv*))
                              :name (format nil "ultra-accept-~A" port)))
  t)

(defun stop ()
  (when *server-socket*
    (ignore-errors (usocket:socket-close *server-socket*))
    (setf *srv* nil))
  (when *accept-thread*
    (bt:destroy-thread *thr*)
    (setf *thr* nil))
  t)
