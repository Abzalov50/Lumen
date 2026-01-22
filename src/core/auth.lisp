(in-package :cl)

(defpackage :lumen.core.auth
  (:use :cl :alexandria)
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :ctx-get :ctx-set!)
  (:import-from :lumen.utils :-> :->> :str-prefix-p :ensure-header :parse-http-date
		:format-http-date)
  (:import-from :lumen.core.jwt :jwt-encode :jwt-decode)
  (:import-from :lumen.core.router :defroute)
  (:export :hash-password :verify-password))

(in-package :lumen.core.auth)

(defparameter *pbkdf2-iters* 120000)

;; HEX
(defun bytes->hex (u8)
  (with-output-to-string (s)
    (loop for b across u8 do (format s "~2,'0X" b))))
(defun hex->bytes (hex)
  (let* ((len (/ (length hex) 2))
         (u8 (make-array len :element-type '(unsigned-byte 8))))
    (loop for i from 0 below len
          for j = (* i 2)
          do (setf (aref u8 i) (parse-integer hex :start j :end (+ j 2) :radix 16)))
    u8))

;; Base64-url (recommandé pour JSON/API)
(defun bytes->b64url (u8)
  (cl-base64:usb8-array-to-base64-string u8 :uri t))
(defun b64url->bytes (s)
  (cl-base64:base64-string-to-usb8-array s :uri t))

(defun rand-bytes (n)
  (let ((v (make-array n :element-type '(unsigned-byte 8))))
    (dotimes (i n) (setf (aref v i) (random 256))) v))

;; password : STRING
;; salt     : (unsigned-byte 8) vector
;; retourne une clé dérivée de longueur = digest-length(SHA-256) (32 octets)
(defun pbkdf2-hmac-sha256 (password salt iterations)
  (multiple-value-bind (dk _salt)
      (ironclad:pbkdf2-hash-password
       (trivial-utf-8:string-to-utf-8-bytes password)
       :salt salt
       :digest 'ironclad:sha256
       :iterations iterations)
    (declare (ignore _salt))  ; on a passé le salt nous-mêmes
    dk))

;; Retourne 3 valeurs : salt, iters, derived-key
(defun hash-password (plain &key (iters *pbkdf2-iters*) (salt (rand-bytes 16)))
  (values salt iters (pbkdf2-hmac-sha256 plain salt iters)))

(defun verify-password (plain salt iters stored-dk)
  (let ((dk (pbkdf2-hmac-sha256 plain salt iters)))
    (equalp dk stored-dk)))
