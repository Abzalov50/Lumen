(in-package :cl)

(defpackage :lumen.http.proxy-async
  (:use :cl :alexandria)
  (:export :define-proxy-module-async :proxy-pass-async :*proxy-worker-pool*))

(in-package :lumen.http.proxy-async)

;; Le bouclier anti-GZIP (Toujours vital)
(defparameter *hop-by-hop*
  '("connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
    "te" "trailers" "transfer-encoding" "upgrade" "content-length"
    "accept-encoding" "content-encoding"))

;; ============================================================================
;; LE WORKER POOL
;; C'est ici que la magie opère. Au lieu d'utiliser un thread par navigateur,
;; on utilise ce pool de 128 travailleurs UNIQUEMENT pour attendre les requêtes
;; vers vos backends (Django/Allegro). Le thread principal (Event Loop) n'est
;; JAMAIS bloqué.
;; ============================================================================
(defvar *proxy-worker-pool* nil)

(defun ensure-worker-pool ()
  (unless (and *proxy-worker-pool* (not (lparallel:kernel-name *proxy-worker-pool*)))
    (setf *proxy-worker-pool* (lparallel:make-kernel 128 :name "lumen-proxy-workers"))))

;; ============================================================================
;; LE PROXY ASYNCHRONE
;; ============================================================================
(defun proxy-pass-async (target-base-url &key (timeout 30))
  (ensure-worker-pool)
  (lambda (req)
    ;; ------------------------------------------------------------------------
    ;; CHANGEMENT DE PARADIGME :
    ;; Au lieu de retourner un objet `response` immédiatement, un handler 
    ;; asynchrone retourne une FONCTION (lambda) qui accepte un callback `responder`.
    ;; ------------------------------------------------------------------------
    (lambda (responder)
      (let* ((method (intern (string-upcase (string (lumen.core.http:req-method req))) "KEYWORD"))
             (uri (lumen.http.proxy::%reconstruct-uri req)) ;; On réutilise votre utilitaire
             (final-url (format nil "~A~A" (string-right-trim "/" target-base-url) uri))
             (raw-req-headers (lumen.core.http:req-headers req))
             (clean-req-headers '()))

        ;; 1. Préparation des headers entrants (Exécuté instantanément dans l'Event Loop)
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
               (cl-header (cdr (assoc "content-length" raw-req-headers :test #'string-equal)))
               (has-body (and cl-header (> (or (parse-integer (princ-to-string cl-header) :junk-allowed t) 0) 0)))
               (in-body (if has-body (lumen.http.proxy::%safe-read-body req) nil)))

          ;; 2. DÉLÉGATION AU WORKER POOL
          ;; On extrait la tâche lente (Dexador) de l'Event Loop
          (let ((lparallel:*kernel* *proxy-worker-pool*))
            (lparallel:future
              
              ;; --- DÉBUT DU THREAD OUVRIER (Isolé) ---
              (let ((lumen-response
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
                             (values (dex:response-body e) (dex:response-status e) (dex:response-headers e)))
                           (error (e)
                             (values (format nil "502 Bad Gateway - ~A" e) 502 nil)))

                       ;; Filtrage des headers de réponse et construction
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
                
                ;; --- RETOUR À L'EVENT LOOP ---
                ;; Le thread ouvrier a la réponse. Il dit au thread principal (cl-async)
                ;; d'exécuter le callback pour envoyer les données au client.
                (cl-async:with-event-loop (:catch-app-errors t)
                  (funcall responder lumen-response))))))))))
