(in-package :cl)

(defpackage :lumen.data.tenant
  (:use :cl)
  (:import-from :lumen.core.http  :ctx-get :ctx-set! :req-headers :req-ctx)
  (:import-from :lumen.core.config :cfg-get :cfg-get-bool :cfg-get-int)
  (:import-from :lumen.data.db    :ensure-connection :query-1a)
  (:import-from :lumen.data.repo.query :select*)
  (:export :normalize-host
           :tenant-id-by-code
           :tenant-id-for-host
           :tenant-code-by-id))

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
