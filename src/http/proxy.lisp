(defpackage :lumen.http.proxy
  (:use :cl)
  (:import-from :lumen.core.http 
                :request :response 
                :req-method :req-path :req-headers :req-query 
                :req-body-stream :req-params)
  (:export :proxy-pass))

(in-package :lumen.http.proxy)

;;; 1. UTILITAIRES DE RECONSTRUCTION

(defun %reconstruct-uri (req)
  "Recombine le path et la query string."
  (let ((p (req-path req))
        (q (req-query req)))
    (if (and q (> (length q) 0))
        (format nil "~A?~A" p q)
        p)))

(defun %clean-headers-for-upstream (headers)
  "Nettoie les headers (Alist) pour l'upstream."
  (remove-if (lambda (pair)
               (member (car pair) 
                       ;; ON A RETIRÉ "host" DE CETTE LISTE !
                       '("content-length" "transfer-encoding" "connection") 
                       :test #'string-equal))
             headers))

(defun %safe-read-body (req)
  "Lit le body depuis le stream en respectant STRICTEMENT le Content-Length.
   Retourne un tableau d'octets ou NIL."
  (let* ((headers (req-headers req))
         (len-str (cdr (assoc "content-length" headers :test #'string-equal)))
         (len (and len-str (parse-integer len-str :junk-allowed t)))
         (stream (req-body-stream req)))
    
    (cond
      ;; Cas A : Pas de contenu (GET, ou POST vide)
      ((or (null len) (zerop len)) 
       nil)
      
      ;; Cas B : Contenu avec longueur connue
      (t 
       (let ((buffer (make-array len :element-type '(unsigned-byte 8))))
         (read-sequence buffer stream)
         buffer)))))

;;; 2. CŒUR DU PROXY
;; Note: On autorise "accept-encoding" et "content-encoding" à passer.
;; Le proxy va streamer les données compressées (GZIP) sans essayer de les lire, c'est infiniment plus rapide.
;; LE BOUCLIER ANTI-GZIP EST LA CLÉ
;; LE BOUCLIER ANTI-GZIP (Toujours vital pour que Dexador et le navigateur ne s'emmêlent pas les pinceaux)
(defparameter *hop-by-hop*
  '("connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
    "te" "trailers" "transfer-encoding" "upgrade" "content-length"
    "accept-encoding" "content-encoding"))

;;; ===================================================================
;;; LA MACRO STANDARD
;;; ===================================================================
(defmacro define-proxy-module (module-name host path-prefix target-url)
  (let* ((methods '(:GET :POST :PUT :DELETE :PATCH))
         (paths (if (string= path-prefix "/")
                    '("/" "/*")
                    (list path-prefix (format nil "~A/*" path-prefix))))
         (routes '()))
    (dolist (m methods)
      (dolist (p paths)
        (push `(,m ,p (req) (funcall (lumen.http.proxy:proxy-pass ,target-url) req)) routes)))
    `(defmodule ,module-name
       :host ,host
       :routes ,(reverse routes))))

;;; ===================================================================
;;; LE PROXY SYNCHRONE PERFORMANT (Natif Lumen)
;;; ===================================================================
(defun proxy-pass (target-base-url &key (timeout 30) strip-prefix)
  (lambda (req)
    (handler-case
        (let* ((method (intern (string-upcase (string (lumen.core.http:req-method req))) "KEYWORD"))
               (raw-uri (%reconstruct-uri req))
               
               ;; --- NOUVEAU : LOGIQUE DE NETTOYAGE DU PRÉFIXE ---
               ;; Si l'URL est /api/users et le strip-prefix est "/api", l'URI finale devient "/users"
               (final-uri (if (and strip-prefix (string= (subseq raw-uri 0 (length strip-prefix)) strip-prefix))
                              (let ((stripped (subseq raw-uri (length strip-prefix))))
                                (if (string= stripped "") "/" stripped))
                              raw-uri))
               
               (final-url (format nil "~A~A" (string-right-trim "/" target-base-url) final-uri))
               (raw-req-headers (lumen.core.http:req-headers req))
               (clean-req-headers '()))

          ;; On prévient Lumen que le tuyau entrant est propre pour le Keep-Alive
          (lumen.core.http:ctx-set! req :body-consumed t)

          (flet ((process-header (k v)
                   (let ((h (string-downcase (string k))))
                     (unless (member h *hop-by-hop* :test #'string-equal)
                       (if (listp v)
                           (dolist (vv v) (push (cons h (princ-to-string vv)) clean-req-headers))
                           (push (cons h (princ-to-string v)) clean-req-headers))))))
            (if (hash-table-p raw-req-headers)
                (maphash #'process-header raw-req-headers)
                (dolist (kv raw-req-headers)
                  (process-header (car kv) (cdr kv)))))

          (let* ((headers (append clean-req-headers
                                  (list (cons "X-Forwarded-Proto" "https")
                                        (cons "X-Forwarded-Port" "443"))))
                 (cl-header (if (hash-table-p raw-req-headers)
                                (gethash "content-length" raw-req-headers)
                                (cdr (assoc "content-length" raw-req-headers :test #'string-equal))))
                 (has-body (and cl-header (> (or (parse-integer (princ-to-string cl-header) :junk-allowed t) 0) 0)))
                 (in-body (if has-body (%safe-read-body req) nil)))

            (multiple-value-bind (out-body status out-hdrs-ht)
                (handler-case
                    (dex:request final-url
                                 :method method
                                 :headers headers
                                 :content in-body
                                 :use-connection-pool nil
                                 :keep-alive nil
                                 :connect-timeout timeout
                                 :read-timeout timeout
                                 :max-redirects 0
                                 :force-binary t)
                  (dex:http-request-failed (e)
                    (values (dex:response-body e) (dex:response-status e) (dex:response-headers e))))

              (let ((lumen-headers '()))
                (when out-hdrs-ht
                  (maphash (lambda (k v)
                             (let ((h (string-downcase (string k))))
                               (unless (member h *hop-by-hop* :test #'string-equal)
                                 (let ((v-list (if (listp v) v (list v))))
                                   (dolist (vv v-list)
                                     (push (cons h (princ-to-string vv)) lumen-headers))))))
                           out-hdrs-ht))

                (let ((final-body 
                       (cond ((or (null out-body) (eq out-body nil)) 
                              (make-array 0 :element-type '(unsigned-byte 8)))
                             ((typep out-body '(simple-array (unsigned-byte 8) (*))) 
                              out-body)
                             ((typep out-body '(vector (unsigned-byte 8)))
                              (let* ((len (length out-body))
                                     (arr (make-array len :element-type '(unsigned-byte 8))))
                                (replace arr out-body)
                                arr))
                             ((stringp out-body)
                              (ignore-errors (trivial-utf-8:string-to-utf-8-bytes out-body)))
                             (t (make-array 0 :element-type '(unsigned-byte 8))))))

                  (make-instance 'lumen.core.http:response
                                 :status status
                                 :headers lumen-headers
                                 :body final-body))))))
      
      (error (e)
        (format t "~&[FATAL PROXY ERROR] ~A~%" e)
        (make-instance 'lumen.core.http:response
                       :status 502
                       :headers '(("content-type" . "text/plain"))
                       :body "502 Bad Gateway - Le backend a echoue.")))))
