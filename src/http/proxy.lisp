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
  "Nettoie les headers (Alist) pour l'upstream.
   On retire Host (recalculé par Dexador) et Content-Length (recalculé)."
  (remove-if (lambda (pair)
               (member (car pair) 
                       '("host" "content-length" "transfer-encoding" "connection") 
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

(defun proxy-pass (target-base-url &key (timeout 10))
  "Retourne un Handler (Lambda) compatible avec Lumen."
  (lambda (req)
    (let* ((method  (req-method req)) ;; Keyword ou String, Dexador accepte les deux
           ;; Construction de l'URL cible : Target + Path + Query
           (final-url (format nil "~A~A" 
                              (string-right-trim "/" target-base-url) 
                              (%reconstruct-uri req)))
           (headers (%clean-headers-for-upstream (req-headers req)))
           ;; Lecture sûre du body
           (body    (%safe-read-body req)))

      (handler-case
          (multiple-value-bind (resp-body status resp-headers)
              (dex:request final-url
                           :method method
                           :headers headers
                           :content body ;; ByteArray ou NIL
                           :use-connection-pool t
                           :keep-alive t
                           :connect-timeout timeout
                           :read-timeout timeout
                           :ignore-status t) ;; On veut traiter les 404/500 nous-mêmes
            
            ;; Conversion des headers Dexador (Hash-Table) vers Alist pour Lumen
            (let ((lumen-headers '()))
              (maphash (lambda (k v) 
                         (push (cons (string-downcase k) v) lumen-headers))
                       resp-headers)
              
              ;; On retourne un objet RESPONSE standard Lumen
              (make-instance 'lumen.core.http:response 
                             :status status
                             :headers lumen-headers
                             :body resp-body)))

        ;; Gestion des erreurs de connexion (Cible éteinte, DNS fail...)
        (usocket:socket-condition (e)
          (declare (ignore e))
          (lumen.core.http:respond-json 
           '((:error . "Bad Gateway") (:message . "Upstream unreachable"))
           :status 502))
        
        (error (e)
          (format t "~&[Proxy Error] ~A~%" e)
          (lumen.core.http:respond-json 
           '((:error . "Proxy Internal Error"))
           :status 500))))))
