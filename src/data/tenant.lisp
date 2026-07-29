(in-package :cl)

(defpackage :lumen.data.tenant
  (:use :cl)
  (:import-from :lumen.core.http  :ctx-get :ctx-set! :req-headers :req-ctx)
  (:import-from :lumen.core.config :cfg-get :cfg-get-bool :cfg-get-int)
  (:import-from :lumen.data.db    :ensure-connection :query-1a)
  (:import-from :lumen.data.repo.query :select*)
  (:import-from :lumen.core.pipeline :defmiddleware)
  (:export :normalize-host
           :tenant-id-by-code
           :tenant-id-for-host
   :tenant-code-by-id
   :clear-tenant-host-cache
:invalidate-tenant-host-cache
	   :tenant-host-cache-stats
	   :*tenant-host-cache-ttl-seconds*
   :tenant-middleware :tenant-auto))

(in-package :lumen.data.tenant)

;;;; -----------------------------------------------------------------------------
;;;; Config / cache
;;;; -----------------------------------------------------------------------------
(defparameter *tenant-cache-ttl-ms*
  (or (lumen.core.config:cfg-get-int :tenant/cache-ttl-ms :default 30000) ; 30s
      30000))

(defstruct %cache-entry value expires-at)

(defvar *tenant-cache* (make-hash-table :test 'equal))
;; clés possibles :
;;  - ("host" . "api.localhost")   → tenant-id
;;  - ("tid->code" . <uuid>)       → tenant-code
;;  - ("code->tid" . "t-reg-01")   → tenant-id

(defun %now-ms ()
  (round (* 1000.0 (/ (get-internal-real-time) internal-time-units-per-second))))

(defun %cache-get (key)
  (let* ((e (gethash key *tenant-cache*)))
    (when (and e (<= (%now-ms) (%cache-entry-expires-at e)))
      (%cache-entry-value e))))

(defun %cache-put (key val &optional (ttl-ms *tenant-cache-ttl-ms*))
  (setf (gethash key *tenant-cache*)
        (make-%cache-entry :value val :expires-at (+ (%now-ms) (max 1 ttl-ms))))
  val)

(defstruct tenant-host-cache-entry
  tenant-id
  expires-at)

(defparameter *tenant-host-cache*
  (make-hash-table :test #'equal))

(defparameter *tenant-host-cache-lock*
  (bordeaux-threads:make-lock
   "tenant-host-cache"))

(defparameter *tenant-host-cache-ttl-seconds*
  300)

(defvar *tenant-host-cache-hits* 0)
(defvar *tenant-host-cache-misses* 0)

(defun %tenant-cache-now ()
  (/ (get-internal-real-time)
     internal-time-units-per-second))

(defparameter *tenant-host-negative-cache-ttl-seconds*
  30)

(defvar *tenant-host-cache-hits* 0)
(defvar *tenant-host-cache-misses* 0)

(defun %tenant-cache-now ()
  (/ (get-internal-real-time)
     internal-time-units-per-second))

(defun %tenant-host-cache-get (host)
  "Retourne deux valeurs : TENANT-ID et FOUND-P."
  (let ((now (%tenant-cache-now)))

    (bordeaux-threads:with-lock-held
        (*tenant-host-cache-lock*)

      (let ((entry
              (gethash host
                       *tenant-host-cache*)))

        (cond
          ((null entry)
           (incf *tenant-host-cache-misses*)
           (values nil nil))

          ((<=
            (tenant-host-cache-entry-expires-at entry)
            now)

           (remhash host
                    *tenant-host-cache*)

           (incf *tenant-host-cache-misses*)

           (values nil nil))

          (t
           (incf *tenant-host-cache-hits*)

           (values
            (tenant-host-cache-entry-tenant-id entry)
            t)))))))

(defun %tenant-host-cache-put (host tenant-id)
  (let ((entry
          (make-tenant-host-cache-entry
           :tenant-id tenant-id
           :expires-at
           (+ (%tenant-cache-now)
              *tenant-host-cache-ttl-seconds*))))

    (bordeaux-threads:with-lock-held
        (*tenant-host-cache-lock*)

      (setf
       (gethash host *tenant-host-cache*)
       entry)))

  tenant-id)

(defun normalize-tenant-host (host)
  "Normalise un nom d'hôte avant son utilisation comme clé de cache."
  (let* ((raw
           (string-downcase
            (string-trim
             '(#\Space #\Tab #\Newline #\Return)
             (or host ""))))

         ;; Retire le port d'un host IPv4 ou DNS :
         ;; lvh.me:8515 devient lvh.me.
         (without-port
           (cond
             ;; Les adresses IPv6 entre crochets sont conservées.
             ((and (plusp (length raw))
                   (char= (char raw 0) #\[))
              (let ((closing-bracket
                      (position #\] raw)))
                (if closing-bracket
                    (subseq raw
                            0
                            (1+ closing-bracket))
                    raw)))

             (t
              (let ((colon
                      (position #\: raw :from-end t)))
                (if colon
                    (subseq raw 0 colon)
                    raw)))))

         ;; Un FQDN avec un point final doit être équivalent
         ;; à sa forme sans point final.
         (normalized
           (string-right-trim
            '(#\.)
            without-port)))

    normalized))

(defun clear-tenant-host-cache ()
  "Vide entièrement le cache de résolution des tenants."
  (bordeaux-threads:with-lock-held
      (*tenant-host-cache-lock*)

    (clrhash *tenant-host-cache*)

    (setf *tenant-host-cache-hits* 0
          *tenant-host-cache-misses* 0))

  t)

(defun invalidate-tenant-host-cache (host)
  "Supprime du cache l'entrée correspondant à HOST."
  (let ((key
          (normalize-tenant-host host)))

    (bordeaux-threads:with-lock-held
        (*tenant-host-cache-lock*)

      (remhash key *tenant-host-cache*)))

  t)

(defun tenant-host-cache-stats ()
  "Retourne les statistiques courantes du cache."
  (bordeaux-threads:with-lock-held
      (*tenant-host-cache-lock*)

    (list
     :entries
     (hash-table-count *tenant-host-cache*)

     :hits
     *tenant-host-cache-hits*

     :misses
     *tenant-host-cache-misses*)))

(defun %cached-tenant-id (host)
  "Retourne deux valeurs : le tenant et la présence d'une entrée valide."
  (let ((now
          (%tenant-cache-now)))

    (bordeaux-threads:with-lock-held
        (*tenant-host-cache-lock*)

      (let ((entry
              (gethash host
                       *tenant-host-cache*)))

        (cond
          ((null entry)
           (incf *tenant-host-cache-misses*)
           (values nil nil))

          ((<=
            (tenant-host-cache-entry-expires-at entry)
            now)

           (remhash host
                    *tenant-host-cache*)

           (incf *tenant-host-cache-misses*)

           (values nil nil))

          (t
           (incf *tenant-host-cache-hits*)

           ;; TENANT-ID peut être NIL dans le cadre
           ;; d'une entrée négative.
           (values
            (tenant-host-cache-entry-tenant-id entry)
            t)))))))

(defun %cache-tenant-id (host tenant-id)
  (let* ((ttl
           (if tenant-id
               *tenant-host-cache-ttl-seconds*
               *tenant-host-negative-cache-ttl-seconds*))

         (entry
           (make-tenant-host-cache-entry
            :tenant-id tenant-id
            :expires-at
            (+ (%tenant-cache-now)
               ttl))))

    (bordeaux-threads:with-lock-held
        (*tenant-host-cache-lock*)

      (setf
       (gethash host *tenant-host-cache*)
       entry)))

  tenant-id)

(defun resolve-tenant-id-cached (host resolver)
  "Résout HOST en utilisant le cache puis RESOLVER en cas de cache miss.

RESOLVER reçoit le host normalisé et doit retourner le tenant-id ou NIL."
  (let ((normalized-host
          (normalize-tenant-host host)))

    (when (zerop (length normalized-host))
      (return-from resolve-tenant-id-cached nil))

    (multiple-value-bind (tenant-id found-p)
        (%cached-tenant-id normalized-host)

      (if found-p
          tenant-id

          (%cache-tenant-id
           normalized-host
           (funcall resolver
                    normalized-host))))))

;;;; -----------------------------------------------------------------------------
;;;; Normalisation host
;;;; -----------------------------------------------------------------------------
(defun %maybe-real-host-from-proxy (req)
  "Si un mw 'trust-proxy' alimente ctx [:real-host], l’utiliser en priorité."
  (or (ctx-get req :real-host)
      (cdr (assoc "x-forwarded-host" (req-headers req) :test #'string-equal))
      (cdr (assoc "x-real-host"      (req-headers req) :test #'string-equal))))

(defun %strip-port (host)
  "Retire le :port éventuel. Gère IPv6 entre crochets."
  (when host
    (cond
      ;; [::1]:8080  → [::1]
      ((and (plusp (length host))
            (char= (char host 0) #\[))
       (let* ((rb (position #\] host))
              (rest (and rb (subseq host (1+ rb)))))
         (if (and rb rest (plusp (length rest)) (= (char-code (char rest 0)) (char-code #\:)))
             (subseq host 0 (1+ rb))
             host)))
      ;; api.localhost:8080 → api.localhost
      (t (let ((pos (position #\: host)))
           (if pos (subseq host 0 pos) host))))))

(defun normalize-host (host)
  "Lowercase, supprime '.' final inutile et l’espace, enlève le port."
  (when host
    (let* ((h (string-downcase (string-trim '(#\Space #\Tab) host)))
           (h (%strip-port h)))
      (if (and h (plusp (length h)) (char= (char h (1- (length h))) #\.))
          (subseq h 0 (1- (length h)))
          h))))

;;;; -----------------------------------------------------------------------------
;;;; Helpers DB (compatibles avec tes signatures)
;;;; -----------------------------------------------------------------------------
(defun tenant-id-by-code (code)
  "SELECT id FROM tenants WHERE code=$1 LIMIT 1 → UUID string ou NIL."
  (let* ((key (cons "code->tid" code))
         (cached (%cache-get key)))
    (or cached
        (when (and code (plusp (length code)))
          (ensure-connection
            (let ((row (select* :tenants :filters (list '= :code code) :limit 1)))
              (when (and row (first row))
                (%cache-put key (cdr (assoc :id (first row)))))))))))

(defun tenant-code-by-id (tid)
  "SELECT code FROM tenants WHERE id=$1 LIMIT 1 → string ou NIL."
  (let* ((key (cons "tid->code" tid))
         (cached (%cache-get key)))
    (or cached
        (when tid
          (ensure-connection
            (let ((row (query-1a "SELECT code FROM tenants WHERE id=$1" tid)))
              (when row
                (%cache-put key (cdr (assoc :code row))))))))))

#|
(defun tenant-id-for-host (host)
  "SELECT t.id FROM tenant_domains d JOIN tenants t ON t.id=d.tenant_id WHERE d.host=$1 LIMIT 1."
  (let ((h (normalize-host host)))
    (let* ((key (cons "host" (or h "")))
           (cached (%cache-get key)))
      (or cached
          (and h
               (ensure-connection
                 (let ((row (select* :tenant_domains
                                     :filters (list '= :host h)
                                     :select '(:tenant_id)
                                     :limit 1)))
                   (when (and row (first row))
                     (%cache-put key (or (cdr (assoc :tenant_id (first row)))
					 (cdr (assoc :tenant-id (first row)))))))))))))
|#

(defun tenant-id-for-host (host)
  "Retourne le tenant associé à HOST avec un cache global de cinq minutes."
  (let ((normalized-host
          (normalize-host host)))

    (when normalized-host

      (multiple-value-bind (cached-tenant-id found-p)
          (%tenant-host-cache-get normalized-host)

        (if found-p

            (progn
              (format t
                      "~&[TENANT CACHE] HIT host=~A tenant=~A~%"
                      normalized-host
                      cached-tenant-id)

              cached-tenant-id)

            (progn
              (format t
                      "~&[TENANT CACHE] MISS host=~A~%"
                      normalized-host)

              (ensure-connection
                (let* ((rows
                         (select*
                          :tenant_domains
                          :filters
                          (list '= :host normalized-host)
                          :select
                          '(:tenant_id)
                          :limit 1))

                       (row
                         (first rows))

                       (tenant-id
                         (and row
                              (or
                               (cdr
                                (assoc :tenant_id row))

                               (cdr
                                (assoc :tenant-id row))))))

                  (when tenant-id
                    (%tenant-host-cache-put
                     normalized-host
                     tenant-id))

                  tenant-id))))))))

;;;; -----------------------------------------------------------------------------
;;;; TENANT FROM HOST MIDDLEWARE
;;;; -----------------------------------------------------------------------------
(defun %host-header (req)
  (let ((h (lumen.core.http:req-headers req)))
    (or (cdr (assoc "x-forwarded-host" h :test #'string-equal))
        (cdr (assoc "x-real-host"      h :test #'string-equal))
        (cdr (assoc "host"             h :test #'string-equal)))))

(defun %normalize-host (raw)
  (when raw
    (let ((pos (position #\: raw)))
      (if pos (subseq raw 0 pos) raw))))

(defmiddleware tenant-middleware
    ((require-host :initarg :require-host :initform nil)
     (allow-headers :initarg :allow-headers :initform t)
     (resolver-fn :initarg :resolver-fn :initform 'tenant-id-for-host))
    (req next)
  
  (with-slots (require-host allow-headers resolver-fn) mw
    (labels ((fallback-from-headers ()
               (let* ((h (lumen.core.http:req-headers req))
                      (tid (cdr (assoc "x-tenant-id" h :test #'string-equal)))
                      (tcd (cdr (assoc "x-tenant-code" h :test #'string-equal))))
                 (cond
                   (tid (values tid (tenant-code-by-id tid)))
                   (tcd (let ((tid2 (tenant-id-by-code tcd)))
                          (when tid2 (values tid2 tcd))))
                   (t (values nil nil))))))
      
      (let* ((host-raw (%host-header req))
             (host (%normalize-host host-raw))
             ;; On utilise funcall pour permettre l'injection de mock en test
             (tid (and host (handler-case (funcall resolver-fn host) (error () nil))))
             (code (and tid (tenant-code-by-id tid))))
        
        (multiple-value-bind (tid2 code2)
            (if tid
                (values tid code)
                (and allow-headers (fallback-from-headers)))
          (format t "~&[MW TENANT] TENANT ID: ~A~%" tid2)
          (cond
            ;; Cas Succès : Tenant Trouvé
            (tid2
             (lumen.core.http:ctx-set! req :tenant-id tid2)
             (when code2 (lumen.core.http:ctx-set! req :tenant-code code2))
             (funcall next req))
            
            ;; Cas Échec Strict : 404
            (require-host
             (lumen.core.http:respond-json 
              '((:error . ((:type . "tenant") (:message . "Unknown tenant for host")))) 
              :status 404))
            
            ;; Cas Échec Permissif : On continue (Mode "Public" ou "Admin global")
            (t 
             (funcall next req))))))))

;;; Factory Helper (Optionnel)
(defun tenant-auto ()
  "Configure le middleware Tenant via ENV."
  (make-instance 'tenant-middleware
                 :require-host (lumen.core.config:cfg-get-bool :tenant/require-host :default nil)
                 :allow-headers (lumen.core.config:cfg-get-bool :tenant/allow-headers :default t)))
