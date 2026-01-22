(in-package :cl)

(defpackage :lumen.core.jwt
  (:use :cl :alexandria)
  (:import-from :lumen.utils :-> :->> :str-prefix-p :ensure-header :parse-http-date
		:format-http-date :hmac-sha256)
  (:import-from :lumen.utils.time
   :+unix->universal-delta+ :->universal-time :universal-time->unix-seconds)
  (:export :jwt-decode :jwt-encode :*jwt-secret* :issue-access :issue-refresh :*refresh-ttl*))

(in-package :lumen.core.jwt)

(defparameter *jwt-secret* "change-me")
(defparameter *access-ttl*  (* 15 60))       ; 15 min
(defparameter *refresh-ttl* (* 14 24 3600))  ; 14 jours

;;; ---------------------------------------------------------------------------
;;; Utils temps
;;; ---------------------------------------------------------------------------

(defun now-unix-seconds ()
  "Temps courant en secondes UNIX (1970)."
  (lumen.utils.time:universal-time->unix-seconds (get-universal-time)))

(defun %parse-int (x)
  (typecase x
    (integer x)
    (string  (ignore-errors (parse-integer x :junk-allowed t)))
    (t       nil)))

(defun claim->unix-seconds (v)
  "Normalise un claim temporel vers UNIX seconds.
   - Si déjà UNIX (≈ 1e9..2e9), retourne tel quel.
   - Si UNIVERSAL-TIME (≈ 3.9e9), convertit vers UNIX.
   - Si string, essaie de parser puis applique la logique ci-dessus."
  (let* ((n (%parse-int v)))
    (cond
      ((null n) nil)
      ;; Heuristique : > 3e9 → probablement Universal Time (depuis 1900)
      ((> n 3000000000) (lumen.utils.time:universal-time->unix-seconds n))
      (t n))))

;;; ---------------------------------------------------------------------------
;;; Comparaison temps constant pour les signatures
;;; ---------------------------------------------------------------------------

(defun secure-bytes-equal? (a b)
  "Compare deux vecteurs d'octets en temps (quasi) constant."
  (when (and (vectorp a) (vectorp b))
    (let ((len-a (length a))
          (len-b (length b)))
      (when (= len-a len-b)
        (let ((acc 0))
          (dotimes (i len-a (zerop acc))
            (setf acc (logior acc (logxor (aref a i) (aref b i))))))))))

;;; ---------------------------------------------------------------------------
;;; Encodage / Décodage
;;; ---------------------------------------------------------------------------
(defun b64url-encode (u8)
  "Encode un vecteur d’octets en Base64URL sans padding."
  (let* ((s (cl-base64:usb8-array-to-base64-string u8)))
    (string-right-trim "="
      (substitute #\_ #\/
        (substitute #\- #\+ s)))))

(defun b64url-decode (s)
  "Decode une chaîne Base64URL sans padding vers un vecteur d’octets."
  (let* ((s* (substitute #\/ #\_ (substitute #\+ #\- s)))
         (m (mod (length s*) 4))
         (padded (if (zerop m) s* (concatenate 'string s* (make-string (- 4 m) :initial-element #\=)))))
    (cl-base64:base64-string-to-usb8-array padded)))

(defun json->bytes (obj)
  (trivial-utf-8:string-to-utf-8-bytes (cl-json:encode-json-to-string obj)))

(defun bytes->json (u8)
  (cl-json:decode-json-from-string (trivial-utf-8:utf-8-bytes-to-string u8)))

(defun now () (truncate (get-universal-time)))

(defun jwt-encode (payload &key (secret *jwt-secret*))
  (let* ((header '((:typ . "JWT") (:alg . "HS256")))
         (h (b64url-encode (json->bytes header)))
         (p (b64url-encode (json->bytes payload)))
         (msg (trivial-utf-8:string-to-utf-8-bytes (format nil "~A.~A" h p)))
         (sig (hmac-sha256 (trivial-utf-8:string-to-utf-8-bytes secret) msg))
         (s (b64url-encode sig)))
    ;;(print h)
    ;;(print p)
    ;;(print s)
    (format nil "~A.~A.~A" h p s)))

(defun jwt-decode (token &key (secret *jwt-secret*) (verify t) (leeway 60) (debug nil))
  "Décode un JWT HS256.
Retourne (values payload t) si OK, sinon (values nil nil).
- Dates (exp/nbf) attendues en UNIX seconds ; compat UT → UNIX incluse.
- LEEWAY en secondes.
- Si VERIFY=T, contrôle signature et exp/nbf."
  (labels
      ((logf (&rest args) (when debug (apply #'format t args)))
       (utf8 (s) (trivial-utf-8:string-to-utf-8-bytes s))
       (alist-get-ci (alist key)
         (or (cdr (assoc key alist))
             (and (symbolp key) (cdr (assoc (string-downcase (symbol-name key)) alist :test #'string=)))
             (and (stringp key) (cdr (assoc (intern (string-upcase key) :keyword) alist))))))
    (handler-case
        (let* ((parts (uiop:split-string token :separator ".")))
          (unless (= (length parts) 3)
            (return-from jwt-decode (values nil nil)))
          (destructuring-bind (h64 p64 s64) parts
            (let* ((hdr-bytes (b64url-decode h64))
                   (pld-bytes (b64url-decode p64))
                   (sig-bytes (b64url-decode s64))
                   (header    (bytes->json hdr-bytes))   ;; alist
                   (payload   (bytes->json pld-bytes)))  ;; alist
              (when verify
                ;; Vérification signature (HS256)
                (let* ((alg (or (alist-get-ci header :alg) (alist-get-ci header "alg")))
                       (msg (utf8 (format nil "~A.~A" h64 p64)))
                       (calc (hmac-sha256 (utf8 secret) msg)))
                  (when (and alg (not (string= alg "HS256")))
                    (logf "~&[jwt] alg non supporté: ~A~%" alg)
                    (return-from jwt-decode (values nil nil)))
                  (unless (secure-bytes-equal? sig-bytes calc)
                    (logf "~&[jwt] signature invalide~%")
                    (return-from jwt-decode (values nil nil))))
                ;; Vérifs exp/nbf (UNIX seconds) + leeway
                (let* ((exp (claim->unix-seconds (alist-get-ci payload :exp)))
                       (nbf (claim->unix-seconds (alist-get-ci payload :nbf)))
                       (now (now-unix-seconds)))
                  (logf "~&[jwt] now=~A exp=~A nbf=~A leeway=~A~%" now exp nbf leeway)
                  (when (and exp (<= exp (- now leeway)))
                    (logf "~&[jwt] token expiré~%")
                    (return-from jwt-decode (values nil nil)))
                  (when (and nbf (> nbf (+ now leeway)))
                    (logf "~&[jwt] token non encore valable (nbf)~%")
                    (return-from jwt-decode (values nil nil)))))
              (values payload t))))
      (error (e)
        (logf "~&[jwt] decode error: ~A~%" e)
        (values nil nil)))))
#|
(defun issue-access (user-id &key role scopes)
  (jwt-encode `((:sub . ,user-id)
                (:role . ,role)
                (:scopes . ,scopes)
                (:iat . ,(now))
                (:exp . ,(+ (now) *access-ttl*)))))
|#

(defun issue-access (user-id &key role scopes tenant claims (ttl *access-ttl*))
  "Émet un access JWT (dates en UNIX seconds).
   CLAIMS (alist) est mergé à la fin pour permettre un override ciblé."
  (let* ((now (now-unix-seconds))
         (exp (+ now (or ttl *access-ttl*)))
         (base `((:sub    . ,user-id)
                 (:typ    . "access")
                 (:role   . ,role)
                 (:scopes . ,(or scopes '()))
                 ,@(when tenant `((:tenant . ,tenant)))
                 (:iat    . ,now)
                 (:exp    . ,exp)))
         (payload (append base (or claims '()))))
    (jwt-encode payload :secret *jwt-secret*)))

(defun issue-refresh (user-id &key role scopes tenant claims (ttl *refresh-ttl*))
  "Émet un refresh JWT (dates en UNIX seconds).
   Peut embarquer :scopes et :tenant pour re-générer un access sans requête DB."
  (let* ((now (now-unix-seconds))
         (exp (+ now (or ttl *refresh-ttl*)))
         (base `((:sub  . ,user-id)
                 (:typ  . "refresh")
                 (:role . ,role)
                 ,@(when scopes `((:scopes . ,scopes)))
                 ,@(when tenant `((:tenant . ,tenant)))
                 (:iat  . ,now)
                 (:exp  . ,exp)))
         (payload (append base (or claims '()))))
    (jwt-encode payload :secret *jwt-secret*)))
