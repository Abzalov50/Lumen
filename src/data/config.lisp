(in-package :cl)

(defpackage :lumen.data.config
  (:use :cl)
  (:import-from :lumen.utils
    :str-prefix-p)
  (:export :parse-postgres-url :db-config :normalize-sslmode :merge-db-plists))

(in-package :lumen.data.config)

;;; ---------------------------------------------------------------------------
;;; Helpers (pure functions, no external deps required)
;;; ---------------------------------------------------------------------------

(defun %percent-decode (s)
  "Decode %XX sequences in S. Returns S if NIL."
  (when s
    (with-output-to-string (out)
      (loop for i from 0 below (length s) do
            (let ((ch (char s i)))
              (cond
                ((and (char= ch #\%) (<= (+ i 2) (1- (length s))))
                 (let* ((h1 (char s (1+ i)))
                        (h2 (char s (+ i 2)))
                        (code (parse-integer (coerce (list h1 h2) 'string)
                                             :radix 16 :junk-allowed t)))
                   (when code (write-char (code-char code) out))
                   (incf i 2)))
                (t (write-char ch out))))))))

(defun %split-once (s delimiter)
  "Split S at first occurrence of DELIMITER (char). Returns two values: head tail."
  (let ((pos (position delimiter s)))
    (if pos
        (values (subseq s 0 pos) (subseq s (1+ pos)))
        (values s nil))))

(defun %parse-query (query)
  "Return alist of query params from QUERY (k=v&k2=v2). Keys as keywords."
  (let ((pairs (when (and query (> (length query) 0))
                 (uiop:split-string query :separator "&")))
        (acc '()))
    (dolist (p pairs (nreverse acc))
      (multiple-value-bind (k v) (%split-once p #\=)
        (let ((kk (intern (string-upcase (%percent-decode k)) :keyword))
              (vv (%percent-decode v)))
          (push (cons kk vv) acc))))))

(defun normalize-sslmode (mode)
  "→ one of :DISABLE :ALLOW :PREFER :REQUIRE :VERIFY-CA :VERIFY-FULL."
  (let ((m (etypecase mode
             (null :disable)
             (string (string-downcase mode))
             (symbol (string-downcase (symbol-name mode))))))
    (cond
      ((member m '("disable" "off" "0") :test #'string-equal) :disable)
      ((string= m "allow") :allow)
      ((string= m "prefer") :prefer)
      ((or (string= m "require") (string= m "on") (string= m "1")) :require)
      ((member m '("verify-ca" "verify_ca") :test #'string-equal) :verify-ca)
      ((member m '("verify-full" "verify_full") :test #'string-equal) :verify-full)
      (t :disable))))

(defun %int (x &optional (default 0))
  (etypecase x
    (null default)
    (integer x)
    (string (or (ignore-errors (parse-integer x)) default))
    (t default)))

(defun merge-db-plists (&rest plists)
  "Left→right merge; later non-NIL values override earlier."
  (let ((out '()))
    (dolist (pl plists (nreverse out))
      (loop for (k v) on pl by #'cddr do
            (when v (setf (getf out k) v))))))

;;; ---------------------------------------------------------------------------
;;; parse-postgres-url
;;; ---------------------------------------------------------------------------

(defun parse-postgres-url (url-string)
  "Parse postgres:// URL → plist:
   :HOST :PORT :DATABASE :USER :PASSWORD :SSLMODE :APPLICATION-NAME"
  (when (and url-string (> (length url-string) 0))
    (let* ((s (string-trim '(#\Space #\Tab) url-string))
           (prefix "postgres://")
           (s (if (lumen.utils:str-prefix-p prefix (string-downcase s))
                  (subseq s (length prefix)) s))
           head query)
      (multiple-value-setq (head query) (%split-once s #\?))
      (multiple-value-bind (auth hostpart) (%split-once head #\@)
        (unless hostpart (setf hostpart auth auth nil))
        (multiple-value-bind (user pass) (when auth (%split-once auth #\:))
          (multiple-value-bind (hostport dbname) (%split-once hostpart #\/)
            (multiple-value-bind (host port) (%split-once hostport #\:)
              (let* ((qp (%parse-query query))
                     (ssl (normalize-sslmode (or (cdr (assoc :SSLMODE qp)))))
                     (app (%percent-decode (cdr (assoc :APPLICATION_NAME qp)))))
                (list :host (%percent-decode host)
                      :port (%int port 5432)
                      :database (%percent-decode dbname)
                      :user (%percent-decode user)
                      :password (%percent-decode pass)
                      :sslmode ssl
                      :application-name (or app "lumen"))))))))))

;;; ---------------------------------------------------------------------------
;;; db-config (ENV → normalized plist)
;;; ---------------------------------------------------------------------------

(defun db-config ()
  "Assemble config from DATABASE_URL + PG* envs.
Returns plist:
  :URL :POOL-SIZE :POOL-TIMEOUT-MS :SSLMODE :APPLICATION-NAME
  :HOST :PORT :DATABASE :USER :PASSWORD (convenience)"
  (let* ((url (lumen.core.config:cfg-get "DATABASE_URL"))
         (from-url (parse-postgres-url url))
         (from-env (list :host (uiop:getenv "PGHOST")
                         :port (%int (uiop:getenv "PGPORT") 5432)
                         :database (uiop:getenv "PGDATABASE")
                         :user (uiop:getenv "PGUSER")
                         :password (uiop:getenv "PGPASSWORD")
                         :sslmode (normalize-sslmode (or (uiop:getenv "PGSSLMODE")
                                                        (getf from-url :sslmode)))
                         :application-name (or (uiop:getenv "PGAPPNAME")
                                               (getf from-url :application-name))))
         (base (merge-db-plists from-url from-env))
         (pool (list :pool-size (%int (uiop:getenv "DB_POOL_SIZE") 10)
                     :pool-timeout-ms (%int (uiop:getenv "DB_POOL_TIMEOUT_MS") 5000))))
    (merge-db-plists (list :url (or url (format nil "postgres://~a/~a"
                                               (or (getf base :host) "localhost")
                                               (or (getf base :database) "lumen"))))
                     base
                     pool)))
