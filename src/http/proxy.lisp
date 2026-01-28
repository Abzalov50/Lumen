(defpackage :lumen.http.proxy
  (:use :cl)
  (:import-from :lumen.core.http 
                :request-method :request-uri :request-headers :request-raw-body)
  (:export :proxy-pass))

(in-package :lumen.http.proxy)

(defun %clean-headers-for-proxy (headers)
  "Nettoie les headers entrants avant de les envoyer à la cible.
   On retire 'Host' car Dexador va le recalculer pour la cible.
   On retire 'Content-Length' car Dexador le gère selon le body envoyé."
  (let ((h (make-hash-table :test 'equal)))
    (loop for (k . v) in headers do
          (unless (member k '("host" "content-length" "transfer-encoding" "connection") 
                          :test #'string-equal)
            (setf (gethash k h) v)))
    h))

(defun proxy-pass (target-base-url &key (timeout 10))
  "Retourne un Handler Lumen qui relaie tout vers target-base-url."
  (lambda (req)
    (let* ((method  (request-method req))
           ;; On concatène l'URL cible avec l'URI de la requête (path + query)
           (url     (format nil "~A~A" 
                            (string-right-trim "/" target-base-url) 
                            (request-uri req)))
           (headers (%clean-headers-for-proxy (request-headers req)))
           (body    (request-raw-body req)))

      ;; Debug optionnel
      ;; (format t "~&[Proxy] Relaying ~A ~A -> ~A~%" method (request-uri req) url)

      (handler-case
          (multiple-value-bind (resp-body status resp-headers)
              (dex:request url
                           :method method
                           :headers headers
                           :content body
                           :use-connection-pool t
                           :keep-alive t
                           :connect-timeout timeout
                           :read-timeout timeout
                           ;; Important : ne pas signaler d'erreur Lisp pour les 4xx/5xx
                           :ignore-status t)
            
            ;; Conversion de la réponse pour Lumen
            ;; Dexador renvoie les headers sous forme de hash-table ou alist
            (let ((lumen-headers '()))
              (maphash (lambda (k v) 
                         (push (cons (string-downcase k) v) lumen-headers))
                       resp-headers)
              
              ;; On retourne (status headers body)
              (list status lumen-headers resp-body)))

        ;; Gestion des erreurs de connexion (Cible éteinte, DNS fail...)
        (usocket:socket-condition (e)
          (declare (ignore e))
          (lumen.core.http:respond-json 
           '((:error . "Bad Gateway") (:message . "Upstream unreachable"))
           :status 502))
        
        (error (e)
          (format t "~&[Proxy Error] ~A~%" e)
          (lumen.core.http:respond-json 
           '((:error . "Proxy Error"))
           :status 500))))))
