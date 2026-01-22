(in-package :cl)

(defpackage :lumen.core.session
  (:use :cl :alexandria)
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :ctx-get :ctx-set!)
  (:import-from :lumen.utils :str-prefix-p :ensure-header :alist-set :hmac-sha256)
  (:import-from :ironclad :mac-digest :make-hmac)
  (:export :session-id :session-data :session-get :session-set! :session-del! :verify-signed-sid :make-session-id :store-get :store-put! :store-del! :sign-sid :rand-bytes :*session-ttl* :*session-cookie* :*secure-cookie* :*session-store*))

(in-package :lumen.core.session)

(defparameter *session-cookie* "lumen.sid")
(defparameter *session-ttl*    3600) ; secondes
(defparameter *secure-cookie*  nil)  ; mettre T en prod (HTTPS)
(defparameter *session-store*  (make-hash-table :test #'equal)) ; id -> (alist :data … :exp …)

#|
(defun hmac-sha256 (key message)
  (let ((hmac (make-hmac (babel:string-to-octets key :encoding :utf-8) 'sha256)))
    (update-hmac hmac (babel:string-to-octets message :encoding :utf-8))
(hmac-digest hmac)))
|#

(defun hex (u8)
  (with-output-to-string (s)
    (dotimes (i (length u8))
      (format s "~2,'0X" (aref u8 i)))))

(defun rand-bytes (n)
  (let ((v (make-array n :element-type '(unsigned-byte 8))))
    (dotimes (i n) (setf (aref v i) (random 256))) v))

(defun make-session-id ()
  (hex (rand-bytes 16))) ; 128 bits

;; ---------- store mémoire ----------
(defun %now () (get-universal-time))

(defun store-put! (sid data ttl)
  (setf (gethash sid *session-store*)
        (list (cons :data data)
              (cons :exp  (+ (%now) (or ttl *session-ttl*))))))

(defun store-get (sid)
  (let ((cell (gethash sid *session-store*)))
    (when cell
      (let ((exp (cdr (assoc :exp cell))))
        (if (and exp (> exp (%now)))
            (cdr (assoc :data cell))
            (progn (remhash sid *session-store*) nil))))))

(defun store-del! (sid) (remhash sid *session-store*))

;; ---------- cookie signé ----------
(defun sign-sid (sid secret)
  (let* ((bytes (trivial-utf-8:string-to-utf-8-bytes sid))
         (sig   (hmac-sha256 (trivial-utf-8:string-to-utf-8-bytes secret) bytes)))
    (format nil "~A.~A" sid (hex sig))))

(defun verify-signed-sid (cookie-value secret)
  "Retourne SID si cookie-value est bien signé avec SECRET, sinon NIL."
  (let ((pos (position #\. cookie-value)))
    (when pos
      (let* ((sid (subseq cookie-value 0 pos))
             (expected (sign-sid sid secret)))
        (when (string= expected cookie-value)
          sid)))))

;; ---------- API session ----------
(defun session-id (req) (lumen.core.http:ctx-get req :session-id))
(defun session-data (req) (lumen.core.http:ctx-get req :session))
(defun session-get  (req key &key (test #'eq))
  (cdr (assoc key (session-data req) :test test)))
(defun session-set! (req key value &key (test #'eq))
  (let* ((alist (session-data req))
         (new   (lumen.utils:alist-set alist key value :test test)))
    (lumen.core.http:ctx-set! req :session new) new))
(defun session-del! (req key &key (test #'eq))
  (let ((alist (remove key (session-data req) :key #'car :test test)))
    (lumen.core.http:ctx-set! req :session alist) alist))
