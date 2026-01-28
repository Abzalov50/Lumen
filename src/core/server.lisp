(in-package :cl)

(defpackage :lumen.core.server
  (:use :cl :alexandria)
  (:import-from :lumen.utils :ensure-header)
  (:import-from :lumen.core.http :request :response :resp-status :resp-headers :resp-body :respond-sse)
  (:import-from :lumen.core.error :*error-handler*)  
  (:import-from :lumen.core.shutdown :register-connection :unregister-connection
   :draining-p)
  (:import-from :lumen.core.config :*profile*)
  (:export :start :stop))

(in-package :lumen.core.server)

;;; ---------------------------------------------------------------------------
;;; 1. STRUCTURE DE DONNÉES (Listener)
;;; ---------------------------------------------------------------------------

(defclass listener ()
  ((port          :initarg :port          :accessor listener-port)
   (socket        :initarg :socket        :accessor listener-socket)
   (accept-thread :initarg :accept-thread :accessor listener-thread)
   (handler       :initarg :handler       :accessor listener-handler)
   (ssl-p         :initarg :ssl-p         :initform nil :accessor listener-ssl-p)))

;; Variable globale interne pour le contexte SSL (partagé ou recréé)
(defparameter *ssl-context* nil)

;;; ---------------------------------------------------------------------------
;;; 2. UTILITAIRES SSL
;;; ---------------------------------------------------------------------------
(defun init-ssl-context (cert-file key-file key-password)
  "Initialise le contexte OpenSSL."
  (unless (and cert-file key-file)
    (error "SSL enabled but no certificate/key provided."))
  
  (let ((cert (namestring (%ensure-existing-pathname cert-file)))
        (pkey (namestring (%ensure-existing-pathname key-file))))
    
    (setf *ssl-context*
          (cl+ssl:make-context
           ;; Sécurité : On désactive les vieux protocoles
           :disabled-protocols (list cl+ssl:+ssl-op-no-sslv2+ cl+ssl:+ssl-op-no-sslv3+)
           ;; :min-proto-version cl+ssl:+tls1-2-version+ ;; Si dispo dans votre version CL+SSL
           :verify-mode cl+ssl:+ssl-verify-none+
           :certificate-chain-file cert
           :private-key-file        pkey
           :private-key-password    key-password))))

;; --- CONFIGURATION & LIMITES ---
(defparameter *port* 8080)
(defparameter *use-chunked* t) ; passe à T si tu veux tester le chunked
(defparameter *sse-use-chunked* nil) ; ← mets T plus tard si tu veux rebasculer sur chunked

;; Limites de sécurité (Anti-DoS)
(defparameter *max-concurrent-connections* 1000 "Nombre max de clients simultanés.")
(defparameter *current-connections* 0)
(defparameter *connections-lock* (bt:make-lock "server-conns-lock"))

(defparameter *keep-alive* t)
(defparameter *keep-alive-timeout* 5.0)  ; secondes d’inactivité max
(defparameter *keep-alive-max* 100)      ; requêtes max par connexion
(defparameter *read-timeout* 10.0 "Timeout pour lire les headers (secondes)")

(defparameter *handler*
  (lambda (req)
    (declare (ignore req))
    (make-instance 'response :status 500 :headers nil :body "")))

;; Sockets & Threads
(defparameter *server-socket* nil)       ; HTTP
(defparameter *accept-thread* nil)
(defparameter *server-socket-tls* nil)   ; HTTPS
(defparameter *accept-thread-tls* nil)

;; SSL Config
(defparameter *ssl-enabled* nil)
(defparameter *ssl-port* 8443)
(defparameter *ssl-context* nil)
(defparameter *ssl-cert-file* nil)
(defparameter *ssl-key-file* nil)
(defparameter *ssl-key-password* nil)  ; si la clé est chiffrée

(defun http11-p (ver) (and ver (search "HTTP/1.1" ver)))

(defun want-keep-alive-p (http-ver headers)
  "Politique standard : HTTP/1.1 → keep-alive par défaut ; HTTP/1.0 → close par défaut.
   Connection: close/keep-alive force la décision."
  (let* ((conn (header headers "connection")))
    (cond
      ((and conn (string-equal conn "close")) nil)
      ((and conn (string-equal conn "keep-alive")) t)
      ((http11-p http-ver) t)
      (t nil))))

(defun %ensure-existing-pathname (x)
  (etypecase x
    (pathname (or (probe-file x) (error "SSL: file not found: ~A" x)))
    (string   (or (probe-file x) (error "SSL: file not found: ~A" x)))))

(defun ->crlf (s) (concatenate 'string s "\r\n"))

(defun crlf (s)
  "Écrit une fin de ligne correcte quand le flux est ouvert avec :eol-style :crlf."
  (write-char #\Newline s))  ; pas de #\Return ici !

(defun %wr-crlf (flexi)
  (write-char #\Return flexi)  ; \r
  (write-char #\Newline flexi)) ; \n

(defun status-reason (status)
  (case status
    (200 "OK") (201 "Created") (204 "No Content")
    (400 "Bad Request") (401 "Unauthorized") (403 "Forbidden")
    (404 "Not Found") (500 "Internal Server Error")
    (t "OK")))

(defun %remove-header (headers name)
  (remove-if (lambda (h) (string-equal (car h) name)) headers))

(defun %write-headers (flexi headers)
  (dolist (h headers)
    (format flexi "~A: ~A" (car h) (cdr h))
    (crlf flexi)
    ))

(defun %write-chunk (flexi data)
  (etypecase data
    (string
     ;; On convertit TOUT DE SUITE en octets
     (let* ((bytes (trivial-utf-8:string-to-utf-8-bytes data))
            (n (length bytes)))
       (format flexi "~X" n) (%wr-crlf flexi)
       ;; On écrit les octets, pas la string
       (write-sequence bytes flexi)
       (%wr-crlf flexi)
       (finish-output flexi)))
    ((simple-array (unsigned-byte 8) (*))
     (let ((n (length data)))
       (format flexi "~X" n) (%wr-crlf flexi)
       (write-sequence data flexi)
       (%wr-crlf flexi)
       (finish-output flexi)))))

(defun %finish-chunked (flexi)
  (write-string "0" flexi)
  (%wr-crlf flexi)  ; \r\n
  (%wr-crlf flexi)  ; \r\n (ligne vide)
  (finish-output flexi))

(defun write-response (flexi resp &optional (method "GET") (keep-alive-p nil))
  (labels
      ((%rm (headers name) (remove-if (lambda (h) (string-equal (car h) name)) headers))
       (%has (headers name) (find name headers :key #'car :test #'string-equal))
       (%ensure (headers name val) (lumen.utils:ensure-header headers name val))
       (%emit-headers (hdrs)
         (dolist (h hdrs)
           (format flexi "~A: ~A" (car h) (cdr h))
           (crlf flexi))))
    (let* ((status  (lumen.core.http:resp-status resp))
           (headers (copy-list (lumen.core.http:resp-headers resp)))
           (body    (lumen.core.http:resp-body resp)))

      ;; Ligne de statut
      (format flexi "HTTP/1.1 ~A ~A" status (status-reason status)) (crlf flexi)

      ;; Connection (sauf WS 101)
      (unless (and (= status 101) (functionp body))
        (format flexi "Connection: ~A" (if keep-alive-p "keep-alive" "close")) (crlf flexi)
        (when keep-alive-p
          (format flexi "Keep-Alive: timeout=~D, max=~D"
                  (truncate *keep-alive-timeout*) *keep-alive-max*)
          (crlf flexi)))
      (cond
        ;; ===== WebSocket Upgrade =====
        ((and (= status 101) (functionp body))
         (%emit-headers headers)
         (crlf flexi) (finish-output flexi)
         (funcall body flexi))

        ;; ===== BODY = writer (SSE / streaming) =====
        ((functionp body)
         (let* ((ct-cell (%has headers "content-type"))
                (ctype   (and ct-cell (cdr ct-cell)))
                (is-sse  (and ctype (search "text/event-stream" (string-downcase ctype)))))
           (cond
             ;; --- SSE : RAW, ni Content-Length ni Transfer-Encoding	     
             (is-sse
              (setf headers (%rm headers "content-length"))
              (setf headers (%rm headers "transfer-encoding"))
              (unless ct-cell
                (setf headers (%ensure headers "Content-Type" "text/event-stream; charset=utf-8")))
	      ;;(format t "~&[send] ~A ~A headers=~S~%" method status headers)
              (%emit-headers headers)
              (crlf flexi)
              (unless (string= method "HEAD")
                (handler-case
                    (funcall body (lambda (chunk)
                                    (when chunk
                                      (etypecase chunk
                                        (string (write-string chunk flexi))
                                        ((simple-array (unsigned-byte 8) (*))
                                         (write-sequence chunk flexi)))
                                      (finish-output flexi))))
                  (error () nil))))

             ;; --- Non-SSE : forcer un seul mode
             (t
              (let ((use-chunked t))      ; ← tu peux rendre ça configurable
                (cond
                  (use-chunked
                   ;; chunked : pas de Content-Length
                   (setf headers (%rm headers "content-length"))
                   (setf headers (%ensure (%rm headers "transfer-encoding") "Transfer-Encoding" "chunked"))
                   ;; CT minimal si absent
                   (unless (%has headers "content-type")
                     (setf headers (%ensure headers "Content-Type" "application/octet-stream")))
		   ;;(format t "~&[send] ~A ~A headers=~S~%" method status headers)
                   (%emit-headers headers) (crlf flexi)
                   (unless (string= method "HEAD")
                     (handler-case
                         (progn
                           (funcall body (lambda (chunk) (when chunk (%write-chunk flexi chunk))))
                           (ignore-errors (%finish-chunked flexi)))
                       (error () (ignore-errors (%finish-chunked flexi))))))
                  (t
                   ;; RAW (non recommandé hors SSE)
                   (setf headers (%rm headers "content-length"))
                   (setf headers (%rm headers "transfer-encoding"))
                   (unless (%has headers "content-type")
                     (setf headers (%ensure headers "Content-Type" "application/octet-stream")))
		   ;;(format t "~&[send] ~A ~A headers=~S~%" method status headers)
                   (%emit-headers headers) (crlf flexi)
                   (unless (string= method "HEAD")
                     (handler-case
                         (funcall body (lambda (chunk)
                                         (when chunk
                                           (etypecase chunk
                                             (string (write-string chunk flexi))
                                             ((simple-array (unsigned-byte 8) (*))
                                              (write-sequence chunk flexi)))
                                           (finish-output flexi))))
                       (error () nil))))))))))

        ;; ===== BODY = string =====
        ((stringp body)
         (setf headers (%rm headers "transfer-encoding"))
         ;; 1. On convertit en octets
         (let* ((bytes (trivial-utf-8:string-to-utf-8-bytes body))
                (len   (length bytes)))
           ;; 2. On déclare la taille exacte de ces octets
           (setf headers (%ensure (%rm headers "content-length") "Content-Length" (write-to-string len)))
           (unless (%has headers "content-type")
             (setf headers (%ensure headers "Content-Type" "text/plain; charset=utf-8")))
           
           (%emit-headers headers) (crlf flexi)
           
           (unless (string= method "HEAD")
             ;; 3. CORRECTION ICI : On envoie les octets BRUTS. 
             ;; Flexi-stream ne touchera pas aux EOL car c'est un tableau d'octets.
             (write-sequence bytes flexi) 
             (finish-output flexi))))

        ;; ===== BODY = octets =====
        ((typep body '(simple-array (unsigned-byte 8) (*)))
         ;; Pas de TE chunked pour un corps à longueur connue
         (setf headers (%rm headers "transfer-encoding"))
         (let* ((len (length body)))
           (setf headers (%ensure (%rm headers "content-length") "Content-Length" (write-to-string len)))
	   ;;(format t "~&[send] ~A ~A headers=~S~%" method status headers)
           (%emit-headers headers) (crlf flexi)
           (unless (string= method "HEAD")
             (write-sequence body flexi)
             (finish-output flexi))))

        ;; ===== BODY = vide =====
        (t
         ;; Corps vide : Content-Length: 0, pas de TE
         (setf headers (%rm headers "transfer-encoding"))
         (setf headers (%ensure (%rm headers "content-length") "Content-Length" "0"))
	 ;;(format t "~&[send] ~A ~A headers=~S~%" method status headers)
         (%emit-headers headers) (crlf flexi)
         (finish-output flexi))))))

(defun read-request-line (stream)
  ;; Sécurité : on met un timeout sur la lecture
  (handler-case 
      (let ((line (read-line stream nil nil)))
        (when line
          (let* ((parts (uiop:split-string line :separator " ")))
            (values (nth 0 parts) (nth 1 parts) (nth 2 parts)))))
    (error () nil)))

(defun ensure-binary-stream (raw)
  "Retourne un stream BINAIRE (octets). Sur SBCL, reconstruit le FD-stream si 'raw' est caractère."
  #+sbcl
  (if (subtypep (stream-element-type raw) 'character)
      (let* ((fd (sb-sys:fd-stream-fd raw))
             ;; IMPORTANT : ne pas fermer 2x le fd → :auto-close NIL
             (bin (sb-sys:make-fd-stream fd :input t :output t
                                         :element-type '(unsigned-byte 8)
                                         :buffering :full
                                         :auto-close nil)))
        bin)
      raw)
  #-sbcl
  raw)

(defun make-http-stream (client &key ssl)
  "Retourne un flux FLEXI (caractère) au-dessus du flux usocket ou SSL.
   ROBUSTESSE : Vérifie que client n'est pas NIL."
  (unless client 
    (error "make-http-stream called with NIL client"))
  
  (let* ((raw (usocket:socket-stream client))
         (base (if ssl
                   (cl+ssl:with-global-context (*ssl-context*)
                     (cl+ssl:make-ssl-server-stream
                      raw
                      :cipher-list cl+ssl:*default-cipher-list*))
                   raw))
         (xfmt (flexi-streams:make-external-format :utf-8 :eol-style :crlf)))
    (flexi-streams:make-flexi-stream base :external-format xfmt)))

(defun header (headers name)
  "Retourne la première valeur du header NAME (ou NIL)."
  (cdr (assoc (string-downcase name) headers :test #'string=)))

(defun header* (headers name)
  "Retourne la liste de toutes les valeurs du header NAME."
  (loop for (k . v) in headers
        when (string= k (string-downcase name))
        collect v))

(defun read-headers (stream)
  "Lit les en-têtes HTTP depuis STREAM et retourne une alist (\"nom\" . \"valeur\").
   - Noms en minuscules, insensibles à la casse HTTP.
   - Doublons conservés (push d'une nouvelle paire pour chaque occurrence).
   - Lignes de continuation (obs-fold) concaténées à la dernière valeur."
  (labels ((blank-line-p (s) (and s (= (length s) 0)))
           (trim (s) (string-trim '(#\Space #\Tab #\Return #\Newline) s)))
    (loop
      with headers = '()
      with last-cell = nil
      for line = (read-line stream nil nil)
      do
        (cond
          ;; EOF sans ligne vide: on retourne ce qu'on a
          ((null line)
           (return (nreverse headers)))

          ;; Ligne vide: fin des en-têtes
          ((blank-line-p line)
           (return (nreverse headers)))

          ;; Ligne de continuation (obs-fold): commence par espace ou tab
          ((and (> (length line) 0)
                (member (char line 0) '(#\Space #\Tab)))
           (when last-cell
             (setf (cdr last-cell)
                   (concatenate 'string (cdr last-cell) " " (trim line)))))

          ;; Nouvelle ligne d'en-tête
          (t
           (let* ((pos (position #\: line))
                  (raw-name (if pos (subseq line 0 pos) line))
                  (raw-val  (if pos (subseq line (1+ pos)) "")))
             (let* ((name (string-downcase (trim raw-name)))
                    (val  (trim raw-val))
                    (cell (cons name val)))
               (push cell headers)
               (setf last-cell (car headers)))))))))

(defun handle-connection (socket client handler-fn &key ssl)
  (declare (ignore socket))
  
  ;; 1. Check de sécurité : Client valide ?
  (unless client 
    (format t "~&[WARN] handle-connection appelée avec client NIL.~%")
    (return-from handle-connection))

  ;; 2. Gestion du compteur de connexions
  (bt:with-lock-held (*connections-lock*)
    (incf *current-connections*))

  (let* ((flexi nil)
         (alive t)
         (req-count 0))
    
    (lumen.core.shutdown:register-connection client)
    
    (unwind-protect
         (handler-case
             (progn
               ;; Création du stream protégée
               (setf flexi (make-http-stream client :ssl ssl))
               
               (loop while alive do
                 ;; Timeout de lecture pour éviter les Slowloris
                 (unless (usocket:wait-for-input client :timeout *keep-alive-timeout*)
                   (setf alive nil) (return))
                 
                 (multiple-value-bind (method uri http-ver) (read-request-line flexi)
                   (unless method (setf alive nil) (return))
                   
                   ;; Traitement de la requête
                   (let* ((qpos (and uri (position #\? uri)))
                          (path-only (if qpos (subseq uri 0 qpos) (or uri "/")))
                          (query-str (and qpos (subseq uri (1+ qpos))))
                          (headers (read-headers flexi))
                          (req (make-instance 'lumen.core.http:request
                                              :method (string-upcase method)
                                              :path   path-only
                                              :headers headers
                                              :query  query-str
                                              :cookies nil :params nil
                                              :body-stream flexi :context (list)))
                          (resp (funcall handler-fn req))
                          
                          ;; Logique Keep-Alive
                          (want-ka (and *keep-alive* (want-keep-alive-p http-ver headers)))
                          (len-h (header headers "content-length"))
                          (len* (and len-h (parse-integer len-h :junk-allowed t)))
                          (consumed (or (null len*) (= len* 0)
                                        (lumen.core.http:ctx-get req :body-consumed)))
                          (ka-ok (and want-ka consumed
                                      (not (lumen.core.shutdown:draining-p))
                                      (< (incf req-count) *keep-alive-max*))))
                     
                     (write-response flexi resp method ka-ok)
                     (unless ka-ok (setf alive nil))))))
           
           ;; Catch-all pour éviter le crash du thread
           (error (e)
             (format t "~&[ERROR] Connection handler error: ~A~%" e)))
      
      ;; -- CLEANUP --
      (bt:with-lock-held (*connections-lock*)
        (decf *current-connections*))
      
      (lumen.core.shutdown:unregister-connection client)
      
      (when flexi (ignore-errors (finish-output flexi)))
      (ignore-errors (usocket:socket-shutdown client :output))
      (sleep 0.02)
      (ignore-errors (usocket:socket-close client))
      (when flexi (ignore-errors (close flexi))))))

(defun accept-loop (socket handler-fn &key ssl)
  (loop
    (handler-case
        (progn
          ;; Si on sature, on attend un peu pour libérer la pression
          (when (>= *current-connections* *max-concurrent-connections*)
            (sleep 0.1)
            (format t "~&[WARN] Max connections reached (~D). Pausing accept.~%" *max-concurrent-connections*)
            (continue))

          ;; Accept bloquant
          (let ((client (usocket:socket-accept socket :element-type '(unsigned-byte 8))))
            (if client
                (bt:make-thread (lambda ()
				  (handle-connection socket client handler-fn :ssl ssl))
                                :name (if ssl "lumen-client-tls" "lumen-client"))
                ;; C'est ICI que se trouvait votre bug : client pouvait être NIL
                (format t "~&[WARN] socket-accept returned NIL. Ignoring.~%"))))
      
      ;; Erreurs acceptables (interruption, socket fermée ailleurs)
      (usocket:connection-aborted-error () 
        (format t "~&[INFO] Connection aborted.~%"))
      
      ;; Erreurs graves : on logge mais on ne crashe pas la boucle principale
      (error (e)
        (format t "~&[CRITICAL] Error in accept-loop: ~A~%" e)
        (sleep 1))))) ;; Pause pour éviter de spammer les logs en boucle infinie si erreur persistante

(defun start (&key (port 8080) handler 
                   (ssl nil) (ssl-port 8443) 
                   cert-file key-file key-password)
  "Démarre les serveurs HTTP et/ou HTTPS et retourne une liste d'objets LISTENER."
  
  (let ((listeners '())
        ;; Fallback handler si aucun n'est fourni
        (actual-handler (or handler 
                            (lambda (req) 
                              (declare (ignore req))
                              (make-instance 'lumen.core.http:response 
                                             :status 500 :body "No handler defined.")))))
    
    ;; --- A. Démarrage HTTP (Standard) ---
    (handler-case
        (let* ((sock (usocket:socket-listen "0.0.0.0" port 
                                            :reuse-address t 
                                            :element-type '(unsigned-byte 8)))
               (ls (make-instance 'listener 
                                  :port port 
                                  :socket sock
                                  :handler actual-handler
                                  :ssl-p nil)))
          
          ;; On lance le thread d'acceptation
	  
          (setf (listener-thread ls)
                (bt:make-thread 
                 (lambda () 
                   ;; On passe explicitement le handler à la boucle
                   (accept-loop sock actual-handler :ssl nil))
                 :name (format nil "lumen-accept-~A" port)))
          
          (push ls listeners)
          (format t "~&[SERVER] Listening HTTP on port ~A~%" port))
      
      (usocket:address-in-use-error ()
        (format t "~&[FATAL] HTTP Port ~A is already in use. Skipping.~%" port))
      (error (e)
        (format t "~&[FATAL] Failed to bind HTTP port ~A: ~A~%" port e)))

    ;; --- B. Démarrage HTTPS (Optionnel) ---
    (when ssl
      (handler-case
          (progn
            ;; 1. Init du contexte SSL
            (init-ssl-context cert-file key-file key-password)
            
            ;; 2. Création Socket & Listener
            (let* ((sock (usocket:socket-listen "0.0.0.0" ssl-port 
                                                :reuse-address t 
                                                :element-type '(unsigned-byte 8)))
                   (ls (make-instance 'listener 
                                      :port ssl-port 
                                      :socket sock
                                      :handler actual-handler
                                      :ssl-p t)))
              
              ;; 3. Thread TLS
              (setf (listener-thread ls)
                    (bt:make-thread 
                     (lambda () 
                       ;; On passe :ssl t et le handler
                       (accept-loop sock actual-handler :ssl t))
                     :name (format nil "lumen-accept-tls-~A" ssl-port)))
              
              (push ls listeners)
              (format t "~&[SERVER] Listening HTTPS on port ~A~%" ssl-port)))
        
        (usocket:address-in-use-error ()
          (format t "~&[FATAL] HTTPS Port ~A is already in use. Skipping.~%" ssl-port))
        (error (e)
          (format t "~&[FATAL] Failed to bind HTTPS port ~A: ~A~%" ssl-port e))))
    
    ;; Retourne la liste (ex: (#<LISTENER HTTPS> #<LISTENER HTTP>)) ou NIL si tout a échoué
    listeners))

(defun stop (listeners)
  "Arrête une liste de listeners retournée par START."
  (dolist (l listeners)
    (format t "~&[SERVER] Stopping listener on port ~A...~%" (listener-port l))
    
    ;; 1. Fermeture Socket (Coupe les nouvelles connexions)
    (ignore-errors (usocket:socket-close (listener-socket l)))
    
    ;; 2. Arrêt du Thread (Brutal mais nécessaire pour les boucles bloquantes)
    (let ((thr (listener-thread l)))
      (when (and thr (bt:thread-alive-p thr))
        (ignore-errors (bt:destroy-thread thr)))))
  
  ;; Nettoyage SSL global si plus aucun listener ne tourne (optionnel)
  ;; (setf *ssl-context* nil) 
  t)
