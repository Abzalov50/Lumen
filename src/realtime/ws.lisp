(in-package :cl)

(defpackage :lumen.realtime.ws
  (:use :cl :alexandria)
  (:import-from :lumen.core.http
    :request :response :req-headers :req-path :req-query :respond-json)
  (:import-from :lumen.utils :ensure-header :str-prefix-p)
  (:import-from :lumen.core.middleware :defmiddleware)
  (:export
    ;; handshake + I/O
    :respond-ws
    ;; registry / routing
   :defws :register-ws :unregister-ws :ws-route-dispatch :list-ws-endpoints
    :decode-close-payload :safe-close
    ;; Middleware
    :ws-upgrade-middleware
    ))

(in-package :lumen.realtime.ws)

;;; ---------- RFC6455 Handshake ----------

(defparameter +ws-guid+ "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

(defun %b64 (u8) (cl-base64:usb8-array-to-base64-string u8))
(defun %sha1 (u8) (ironclad:digest-sequence :sha1 u8))
(defun %utf8->string (u8) (trivial-utf-8:utf-8-bytes-to-string u8))
(defun %string->utf8 (s) (trivial-utf-8:string-to-utf-8-bytes s))

(defun %header (headers name)
  (cdr (assoc name headers :test #'string-equal)))

(defun %compute-accept (client-key)
  (let* ((bytes (trivial-utf-8:string-to-utf-8-bytes (concatenate 'string client-key +ws-guid+)))
         (sha   (%sha1 bytes)))
    (%b64 sha)))

(defun %tokenize (s)
  "Découpe 'a, b ,c' → (\"a\" \"b\" \"c\") (trim, ignore vides)."
  (when s
    (remove-if #'(lambda (x) (zerop (length x)))
               (mapcar #'string-trim '(#\Space #\Tab) (make-list (length (uiop:split-string s :separator ",")) :initial-element " "))
               :key #'identity)))

(defun %split-protos (s)
  (when s
    (remove-if #'(lambda (x) (zerop (length x)))
               (mapcar #'(lambda (x) (string-trim '(#\Space #\Tab) x))
                       (uiop:split-string s :separator ",")))))

(defun %ws-handshake-ok-p (headers)
  (and (string-equal "websocket" (or (%header headers "upgrade") ""))
       (search "upgrade" (string-downcase (or (%header headers "connection") "")))
       (%header headers "sec-websocket-key")
       (string= (or (%header headers "sec-websocket-version") "13") "13")))

(defun %negotiate-subprotocol (headers server-protos &key require)
  "Retourne (values chosen or NIL). Si REQUIRE et aucun match → :fail."
  (let* ((client-raw (%header headers "sec-websocket-protocol"))
         (client-protos (%split-protos client-raw)))
    (cond
      ((null server-protos) nil)
      ((null client-protos) (when require :fail))
      (t
       (let ((match (find-if
                     (lambda (p) (member p server-protos :test #'string-equal))
                     client-protos)))
         (or match (when require :fail)))))))

;;; ---------- Frames (lecture/écriture) ----------

(defun %read-u8 (s) (read-byte s nil nil))
(defun %write-u8 (s n) (write-byte (logand n #xff) s))
(defun %read-n (s n)
  (let ((buf (make-array n :element-type '(unsigned-byte 8))))
    (let ((got (read-sequence buf s)))
      (unless (= got n) (return-from %read-n nil)))
    buf))

(defun %be->u16 (a b) (+ (ash a 8) b))
(defun %be->u64 (v) (let ((n 0)) (dotimes (i 8 n) (setf n (+ (ash n 8) (aref v i))))))

(defun %apply-mask! (payload mask)
  (let ((len (length payload)))
    (dotimes (i len)
      (setf (aref payload i)
            (logxor (aref payload i)
                    (aref mask (mod i 4)))))   ; ← aref, pas “are”
    payload))

(defun %ws-send-frame (flexi opcode payload-u8)
  ;; FIN=1, RSV=0, server → client: jamais maské
  (%write-u8 flexi (logior #x80 (logand opcode #x0f)))
  (let ((len (length payload-u8)))
    (cond
      ((< len 126)
       (%write-u8 flexi len))
      ((< len 65536)
       (%write-u8 flexi 126)
       (%write-u8 flexi (ldb (byte 8 8) len))
       (%write-u8 flexi (ldb (byte 8 0) len)))
      (t
       (%write-u8 flexi 127)
       (let ((tmp (make-array 8 :element-type '(unsigned-byte 8))))
         (dotimes (i 8)
           (setf (aref tmp (- 7 i)) (ldb (byte 8 (* i 8)) len)))
         (write-sequence tmp flexi))))
  (when (> (length payload-u8) 0)
    (write-sequence payload-u8 flexi))
  (finish-output flexi)))

(defun %ws-recv-frame (flexi)
  "Retourne (values opcode payload-u8 masked-p fin-p) ou (values :eof nil nil nil)."
  (let ((b1 (%read-u8 flexi)))
    (when (null b1) (return-from %ws-recv-frame (values :eof nil nil nil)))
    (let* ((fin   (logtest b1 #x80))
           (op    (logand b1 #x0f))
           (b2    (%read-u8 flexi)))
      (when (null b2) (return-from %ws-recv-frame (values :eof nil nil nil)))
      (let* ((masked (logtest b2 #x80))
             (len    (logand b2 #x7f)))
        (cond
          ((= len 126)
           (let ((b (%read-n flexi 2)))
             (when (null b) (return-from %ws-recv-frame (values :eof nil nil nil)))
             (setf len (%be->u16 (aref b 0) (aref b 1)))))
          ((= len 127)
           (let ((b (%read-n flexi 8)))
             (when (null b) (return-from %ws-recv-frame (values :eof nil nil nil)))
             (setf len (%be->u64 b)))))
        (let* ((mask (when masked (%read-n flexi 4)))
	       (pl   (if (plusp len) (%read-n flexi len)
			 (make-array 0 :element-type '(unsigned-byte 8)))))
	  (when (and masked pl)
	    (%apply-mask! pl mask)) ; ← assure-toi que c’est bien appelé
	  (format t "[ws] op=~x masked=~a len=~d~%" op masked (length pl))
	  (values op pl masked fin))))))

(defun %send (flexi kind data)
  (ecase kind
    (:text   (%ws-send-frame flexi #x1 (%string->utf8 data)))
    (:binary (%ws-send-frame flexi #x2 data))
    (:ping   (%ws-send-frame flexi #x9 data))
    (:pong   (%ws-send-frame flexi #xA data))
    (:close
     (let ((payload
            (cond
              ((null data) #())
              ((integerp data)
               (let ((u (make-array 2 :element-type '(unsigned-byte 8))))
                 (setf (aref u 0) (ldb (byte 8 8) data)
                       (aref u 1) (ldb (byte 8 0) data))
                 u))
              ((stringp data) (%string->utf8 data))
              (t data))))
       (%ws-send-frame flexi #x8 payload)))))

(defun %recv (flexi)
  "Retourne (values type payload). type ∈ (:text :binary :ping :pong :close :eof :unknown).
   Pour :close → payload U8 éventuel (2 octets code + raison UTF-8)."
  (multiple-value-bind (op pl _masked _fin) (%ws-recv-frame flexi)
    (declare (ignore _masked _fin))
    (case op
      ((:eof) (values :eof nil))
      (#x1    (values :text   (%utf8->string pl)))
      (#x2    (values :binary pl))
      (#x8    (values :close  pl))
      (#x9    (values :ping   pl))
      (#xA    (values :pong   pl))
      (t      (values :unknown pl)))))

;;; ---------- Close codes helpers ----------

(defparameter *valid-close-codes*
  '(1000 1001 1002 1003 1005 1006 1007 1008 1009 1010 1011 1012 1013 1014 1015))

(defun decode-close-payload (u8)
  "Retourne (values code reason-string) ou (values nil nil) si payload vide."
  (if (or (null u8) (= (length u8) 0))
      (values nil nil)
      (let* ((code (+ (ash (aref u8 0) 8) (aref u8 1)))
             (reason (if (> (length u8) 2)
                         (%utf8->string (subseq u8 2))
                         "")))
        (values code reason))))

(defun safe-close (send code &optional reason)
  "Envoie un close avec code valide + raison (optionnelle, UTF-8). Retourne le code utilisé."
  (let* ((c (if (member code *valid-close-codes*) code 1000))
         (r-u8 (and reason (trivial-utf-8:string-to-utf-8-bytes reason)))
         (plen (+ 2 (if r-u8 (length r-u8) 0)))
         (payload (make-array plen :element-type '(unsigned-byte 8))))
    ;; code big-endian
    (setf (aref payload 0) (ldb (byte 8 8) c)
          (aref payload 1) (ldb (byte 8 0) c))
    ;; raison
    (when r-u8 (replace payload r-u8 :start1 2))
    (funcall send :close payload)
    c))

(defun %parse-qs-local (qs)
  "Parse basique pour extraire le token sans dépendre du middleware complet."
  (if (stringp qs)
      (loop for pair in (uiop:split-string qs :separator "&")
            collect (let* ((pos (position #\= pair))
                           (k (if pos (subseq pair 0 pos) pair))
                           (v (if pos (subseq pair (1+ pos)) nil)))
                      (cons k v)))
      qs)) ;; Si c'est déjà une liste ou nil

;;; ---------- respond-ws avec sous-protocoles ----------
(defun respond-ws (req handler &key protocols require-protocol)
  "Handshake WS + boucle handler. Négocie éventuellement SEC-WEBSOCKET-PROTOCOL.
   HANDLER reçoit (recv send req chosen-protocol)."
  (let* ((h (lumen.core.http:req-headers req)))
    (if (%ws-handshake-ok-p h)
        (let* ((client-key (%header h "sec-websocket-key"))
               (accept     (%compute-accept client-key))
               (choice     (%negotiate-subprotocol h protocols :require require-protocol)))
          (when (eql choice :fail)
            (return-from respond-ws
              (respond-json '((:error . ((:type . "ws")
                                         (:message . "subprotocol required"))))
                            :status 400)))
          (let ((hdrs `(("Upgrade" . "websocket")
                        ("Connection" . "Upgrade")
                        ("Sec-WebSocket-Accept" . ,accept)
                        ,@(when choice `(("Sec-WebSocket-Protocol" . ,choice))))))
            (make-instance 'lumen.core.http:response
              :status 101
              :headers hdrs
              :body (lambda (flexi)
                    ;; --- AMÉLIORATION ICI : VERROU D'ÉCRITURE ---
                    (let ((w-lock (bordeaux-threads:make-lock "ws-write")))
                      (labels ((send (kind data) 
                                 ;; On protège l'écriture sur la socket
                                 (bordeaux-threads:with-lock-held (w-lock)
                                   (%send flexi kind data)))
                               (recv () (%recv flexi)))
                        (handler-case
                            (funcall handler #'recv #'send req choice)
                          (error (e)
                            (declare (ignore e))
                            (ignore-errors 
                              (bordeaux-threads:with-lock-held (w-lock)
                                (%send flexi #x8 #())))))))))))
        (respond-json '((:error . ((:type . "ws")
				   (:message . "bad websocket handshake"))))
                      :status 400))))

;;; ---------- Dispatcher par endpoint ----------
(defstruct ws-handler fn protocols require-protocol)

(defvar *ws-handlers*
  (if (boundp '*ws-handlers*) *ws-handlers* (make-hash-table :test 'equal)))

(defun register-ws (path fn &key protocols require-protocol)
  (setf (gethash path *ws-handlers*)
        (make-ws-handler :fn fn :protocols protocols :require-protocol require-protocol))
  path)

(defun unregister-ws (path) (remhash path *ws-handlers*))

(defun list-ws-endpoints ()
  (loop for k being the hash-keys of *ws-handlers* collect k))

(defmacro defws (path (recv send req proto) (&key protocols require-protocol) &body body)
  "Déclare un endpoint WS.
   
   SIGNATURE:
     (defws PATH (RECV-VAR SEND-VAR REQ-VAR PROTO-VAR) 
            (&key PROTOCOLS REQUIRE-PROTOCOL) 
            &body BODY)

   EXEMPLE:
     (defws \"/chat\" (recv send req proto)
       (:protocols '(\"v1\") :require-protocol nil)
       (format t \"Nouveau client connecte !\")
       (loop ...))"
  `(register-ws ,path
                (lambda (,recv ,send ,req ,proto) 
                  (declare (ignorable ,recv ,send ,req ,proto))
                  ,@body)
                :protocols ,protocols
                :require-protocol ,require-protocol))

(defun ws-route-dispatch (req)
  "À utiliser depuis une route GET \"/ws/...\" : dispatch selon req-path."
  (let* ((path (lumen.core.http:req-path req))
         (entry (gethash path *ws-handlers*)))
    (if entry
        (respond-ws req (ws-handler-fn entry)
                    :protocols (ws-handler-protocols entry)
                    :require-protocol (ws-handler-require-protocol entry))
        (respond-json '((:error . ((:type . "ws") (:message . "not found"))))
                      :status 404))))

;;; ------------- Diffusion / rooms (simple registry)
(defvar *ws-rooms* (make-hash-table :test 'equal))
(defun ws-room-join (room send)
  (push send (gethash room *ws-rooms*)))
(defun ws-room-leave (room send)
  (setf (gethash room *ws-rooms*)
        (remove send (gethash room *ws-rooms*))))
(defun ws-broadcast (room kind data)
  (dolist (s (copy-list (gethash room *ws-rooms*)))
    (ignore-errors (funcall s kind data))))

;;; middleware
;;; ------------------------------------------------------------
;;; Helpers: détection upgrade / origin / registry / auth (JWT)
;;; ------------------------------------------------------------

(defun %hdr (req name)
  (cdr (assoc name (req-headers req) :test #'string-equal)))

(defun %upgrade-request-p (req)
  (let ((u (%hdr req "upgrade"))
        (c (%hdr req "connection")))
    (and (string-equal u "websocket")
         (and c (search "upgrade" (string-downcase c))))))

(defun %origin-allowed-p (req origins)
  (or (null origins)
      (member (%hdr req "origin") origins :test #'string=)))

(defun %lookup-ws-entry (path)
  (gethash path *ws-handlers*))

(defun %extract-bearer-token (req &key (query-key "access"))
  (let* ((auth (%hdr req "authorization"))
         ;; On parse la query string localement si c'est une chaîne
         (raw-q (lumen.core.http:req-query req))
         (q     (if (stringp raw-q) 
                    (%parse-qs-local raw-q) 
                    raw-q)))
    
    (or (and auth (lumen.utils:str-prefix-p "Bearer " auth) (subseq auth 7))
        (cdr (assoc query-key q :test #'string=)))))

(defun %maybe-auth-jwt! (req require-jwt jwt-secret &key (query-key "access"))
  "Décode JWT si présent; si REQUIRE-JWT et invalide/absent → retourne une réponse 401.
   Sinon retourne NIL et pose ctx[:jwt] si ok."
  (let ((tok (%extract-bearer-token req :query-key query-key)))
    (cond
      ((null tok)
       (when require-jwt
         (respond-json '((:error . ((:type . "ws") (:message . "auth required")))) :status 401)))
      (t
       (multiple-value-bind (payload _)
           (lumen.core.jwt:jwt-decode tok :secret jwt-secret :verify t)
         (declare (ignore _))
         (if payload
             (progn (lumen.core.http:ctx-set! req :jwt payload) nil)
             (when require-jwt
               (respond-json '((:error . ((:type . "ws") (:message . "invalid token")))) :status 401))))))))

;;; ------------------------------------------------------------
;;; Heartbeat + guards (taille, pong)
;;; ------------------------------------------------------------

(defun %start-heartbeat (send last-pong-ref ping-interval)
  "Démarre un thread ping périodique. Retourne (lambda () (stop))."
  (if (or (null ping-interval) (zerop ping-interval))
      (lambda () nil)
      (let* ((alive t)
             (th (bordeaux-threads:make-thread
                  (lambda ()
                    (loop while alive do
                      (ignore-errors (funcall send :ping #()))
                      (sleep ping-interval)
                      (let ((now (get-universal-time)))
                        (when (> (- now (car last-pong-ref)) (* 3 ping-interval))
                          (ignore-errors (funcall send :close 1006))
                          (setf alive nil)))))
                  :name "ws-heartbeat")))
	(declare (ignore th))
        (lambda () (setf alive nil)))))

(defun %make-recv-with-guards (recv send max-bytes last-pong-ref)
  "Wrap RECV: met à jour le pong, limite la taille, ferme (1009) si dépassement."
  (lambda ()
    (multiple-value-bind (typ data) (funcall recv)
      ;; MAJ last-pong
      (when (eq typ :pong)
        (setf (car last-pong-ref) (get-universal-time)))
      ;; Limite de taille
      (when (and max-bytes data
                 (or (and (stringp data)
                          (> (length (trivial-utf-8:string-to-utf-8-bytes data)) max-bytes))
                     (and (typep data '(simple-array (unsigned-byte 8) (*)))
                          (> (length data) max-bytes))))
        (safe-close send 1009 "message too big")
        (return-from %make-recv-with-guards (values :close #())))
      (values typ data))))

;;; ------------------------------------------------------------
;;; Wrapper de handler: heartbeat, guards
;;; ------------------------------------------------------------

(defun %wrap-user-handler (user-fn ping-interval max-bytes)
  (lambda (recv send req proto)
    (let* ((last-pong-ref (cons (get-universal-time) nil))
           (stop-hb       (%start-heartbeat send last-pong-ref ping-interval))
           (recv*         (%make-recv-with-guards recv send max-bytes last-pong-ref)))
      (unwind-protect
           (funcall user-fn recv* send req proto)
        (funcall stop-hb)))))

;;; ------------------------------------------------------------
;;; Middleware : ws-upgrade
;;; ------------------------------------------------------------
#|
(defun ws-upgrade (&key
                     (mount "/ws")
                     (origins-allow nil) ; '("https://localhost:8443") ou NIL
                     (require-jwt nil)
                     (jwt-secret "change-me")
                     (query-key "access")
                     (ping-interval 0)           ; 0 = off
                     (max-bytes (* 1 1024 1024))) ; 1 MiB
  "Intercepte Upgrade WS sous MOUNT et dispatch vers les handlers DEFWS.
Options: ORIGINS-ALLOW, REQUIRE-JWT, JWT-SECRET, QUERY-KEY, PING-INTERVAL, MAX-BYTES."
  (lambda (next)
    (lambda (req)
      (let ((path (req-path req)))
        (if (and (%upgrade-request-p req) (lumen.utils:str-prefix-p mount path))
            (progn
	      (print (%origin-allowed-p req origins-allow))
              ;; Origin policy
              (unless (%origin-allowed-p req origins-allow)
                (return-from ws-upgrade
                  (lambda (_)
                    (declare (ignore _))
                    (respond-json '((:error . ((:type . "ws") (:message . "origin not allowed")))) :status 403))))
              ;; Lookup handler
              (let ((entry (%lookup-ws-entry path)))
	      (print entry)
                (unless entry
                  (return-from ws-upgrade
                    (lambda (_)
                      (declare (ignore _))
                      (respond-json '((:error . ((:type . "ws") (:message . "not found")))) :status 404))))
                ;; JWT (optionnel)
                (let ((auth-res (%maybe-auth-jwt!
				 req require-jwt jwt-secret :query-key query-key)))
		  (print auth-res)
                  (when auth-res (return-from ws-upgrade (lambda (_) (declare (ignore _)) auth-res))))
                ;; Handshake + boucle
                (respond-ws req
			    (%wrap-user-handler (ws-handler-fn entry) ping-interval max-bytes)
			    :protocols (ws-handler-protocols entry)
			    :require-protocol (ws-handler-require-protocol entry))))
            ;; Pas un WS sous mount → continuer la pile
	    (funcall next req))))))
|#

;;; ------------------------------------------------------------
;;; Middleware : ws-upgrade-middleware (Version CLOS)
;;; ------------------------------------------------------------
;;#|
(defmiddleware ws-upgrade-middleware
    ((mount         :initarg :mount          :initform "/ws")
     (origins-allow :initarg :origins-allow  :initform nil)
     (require-jwt   :initarg :require-jwt    :initform nil)
     (jwt-secret    :initarg :jwt-secret     :initform "change-me")
     (query-key     :initarg :query-key      :initform "access")
     (ping-interval :initarg :ping-interval  :initform 0)
     (max-bytes     :initarg :max-bytes      :initform (* 1 1024 1024)))
    (req next)
  (let ((path (lumen.core.http:req-path req))
        (mount-point (slot-value mw 'mount)))

    ;; 1. Est-ce une requête Upgrade WS sous le bon chemin ?
    (if (and (%upgrade-request-p req) 
             (lumen.utils:str-prefix-p mount-point path))
        
        (progn
          ;; 2. Vérification Origin
          (unless (%origin-allowed-p req (slot-value mw 'origins-allow))
            ;; On coupe la chaîne et on retourne 403
            (return-from lumen.core.pipeline:handle
              (respond-json '((:error . ((:type . "ws") (:message . "origin not allowed")))) 
                            :status 403)))

          ;; 3. Recherche du Handler (Route WS interne)
          (let ((entry (%lookup-ws-entry path)))
            (unless entry
              (return-from lumen.core.pipeline:handle
                (respond-json '((:error . ((:type . "ws") (:message . "not found")))) 
                              :status 404)))

            ;; 4. Authentification JWT (Optionnelle ou requise)
            ;; Note: %maybe-auth-jwt! retourne une réponse (401) si échec, ou NIL si succès.
            (let ((auth-res (%maybe-auth-jwt! req 
                                              (slot-value mw 'require-jwt)
                                              (slot-value mw 'jwt-secret) 
                                              :query-key (slot-value mw 'query-key))))
              (when auth-res 
                (return-from lumen.core.pipeline:handle auth-res)))

            ;; 5. Handshake & Hijack
            ;; respond-ws prend le contrôle du socket et bloque le thread jusqu'à la fin de la connexion.
            (respond-ws req
                        (%wrap-user-handler (ws-handler-fn entry) 
                                            (slot-value mw 'ping-interval) 
                                            (slot-value mw 'max-bytes))
                        :protocols (ws-handler-protocols entry)
                        :require-protocol (ws-handler-require-protocol entry))))

        ;; Sinon : Ce n'est pas pour nous, on passe au middleware suivant
        (funcall next req))))
;;|#
