(in-package :cl)

(defpackage :lumen.data.migrations
  (:use :cl)
  (:import-from :lumen.utils
    :str-prefix-p)
  (:import-from :lumen.data.db
    :with-tx :exec :query-a :query-1a :ensure-connection)
  (:export
    :defmigration
    :migrate-all :migrate-up-to :rollback-one :status :reset!))
(in-package :lumen.data.migrations)

;;;; Registry
(defstruct migration version up-forms down-forms)
(defvar *registry* (make-array 0 :adjustable t :fill-pointer 0))
(defvar *lock* (bt:make-lock "lumen.migrations"))

(defun %to-version-string (v)
  "Normalise VERSION en string. Accepte integer ou string."
  (etypecase v
    (integer (write-to-string v :base 10))
    (string  v)))

(defun %push-migration! (m)
  (bt:with-lock-held (*lock*)
    (vector-push-extend m *registry*)
    ;; garder l’ordre par version croissante dans le registre
    (let* ((lst (loop for i below (length *registry*) collect (aref *registry* i)))
           (sorted (sort lst (lambda (a b)
                               (string< (migration-version a)
                                        (migration-version b))))))
      (setf *registry* (make-array 0 :adjustable t :fill-pointer 0))
      (dolist (x sorted) (vector-push-extend x *registry*)))))

(defmacro defmigration (version &key up down)
  "VERSION = string ou integer horodaté.
UP/DOWN = forms qui seront encapsulées dans une fonction (lambda () ...).
On enregistre préférentiellement des *fonctions objets* (#'), mais on tolère
les lambda-expressions via %ensure-up-fn / %ensure-down-fn."
  (let* ((ver-str (if (integerp version) (write-to-string version) version))
         (up-fn   (when up   `#'(lambda () ,@up)))
         (down-fn (when down `#'(lambda () ,@down))))
    `(%push-migration!
      (make-migration
       :version    ,ver-str
       :up-forms   ,(or up-fn   `(lambda () ,@up))   ; fallback pour compat
       :down-forms ,(or down-fn `(lambda () ,@down))))))

(defun %ensure-up-fn (mig)
  (let ((f (migration-up-forms mig)))
    (cond
      ((functionp f) f)                ; déjà une fonction
      ((consp f)     (eval f))         ; lambda-expression → fonction
      (t (error "Migration ~A has no UP." (migration-version mig))))))

(defun %ensure-down-fn (mig)
  (let ((f (migration-down-forms mig)))
    (cond
      ((null f)     (error "Migration ~A has no DOWN." (migration-version mig)))
      ((functionp f) f)
      ((consp f)     (eval f))
      (t (error "Migration ~A has invalid DOWN." (migration-version mig))))))

;;;; Storage
(defun %ensure-table! ()
  (ensure-connection
      (ignore-errors (postmodern:query "set local client_min_messages = 'error'"))
    (exec
     "CREATE TABLE IF NOT EXISTS schema_migrations (
        version text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )")))

(defun %applied-list ()
  (ensure-connection
    (multiple-value-bind (rows _n)
        (query-a "SELECT version, applied_at FROM schema_migrations ORDER BY version ASC")
      (declare (ignore _n))
      rows)))

(defun %applied-versions ()
  (print (%applied-list))
  (mapcar (lambda (r) (cdr (assoc :version r))) (%applied-list)))

(defun %find-mig (vstr)
  (loop for i from 0 below (length *registry*)
        for m = (aref *registry* i)
        when (string= (migration-version m) vstr) do (return m)))

(defun %max-registered-version ()
  (when (> (length *registry*) 0)
    (migration-version (aref *registry* (1- (length *registry*))))))

;;;; Commands
(defun migrate-all (&key (verbose t))
  (%ensure-table!)
  (let ((target (%max-registered-version)))
    (if target
        (migrate-up-to target :verbose verbose)
        (progn
          (when verbose (format t "~&[migrations] nothing to migrate.~%"))
          (values 0 0)))))

(defun migrate-up-to (target &key (verbose t))
  (%ensure-table!)
  (let* ((tgt (%to-version-string target))
         (applied        (%applied-versions)) ; => liste de strings "YYYY-.."
         (applied-set    (alexandria:alist-hash-table
                          (mapcar (lambda (v) (cons (string-trim " " v) t)) applied)
                          :test #'equal))
         ;; dédupliquer le registre par version
         (all       (loop for i below (length *registry*) collect (aref *registry* i)))
         (all-uniq  (remove-duplicates all :key #'migration-version :test #'equal :from-end t))
         ;; à monter = non appliquées ET ≤ target
         (to-up     (remove-if (lambda (m)
                                 (or (gethash (string-trim " " (migration-version m)) applied-set)
                                     (string< tgt (migration-version m))))
                               all-uniq))
         ;; à descendre = appliquées ET > target
         (to-down   (remove-if-not (lambda (m)
                                     (and (gethash (string-trim " " (migration-version m)) applied-set)
                                          (string< tgt (migration-version m))))
                                   all-uniq))
         (n-up 0)
         (n-down 0))
    ;; -------- UP en ordre croissant --------
    (dolist (m (sort to-up (lambda (a b)
                             (string< (migration-version a) (migration-version b)))))
      (let ((ver (migration-version m)))
        (when verbose (format t "~&[migrations] ↑ applying ~A...~%" ver))
        (with-tx ()
          (let ((up-fn (%ensure-up-fn m)))
            (funcall up-fn))
          (exec "INSERT INTO schema_migrations(version)
                 VALUES ($1)
                 ON CONFLICT (version) DO NOTHING"
                ver))
        (setf (gethash ver applied-set) t)
        (incf n-up)
        (when verbose (format t "[migrations] OK ~A~%" ver))))

    ;; -------- DOWN en ordre décroissant (si jamais on voulait descendre) --------
    (dolist (m (sort to-down (lambda (a b)
                               (string> (migration-version a) (migration-version b)))))
      (let ((ver (migration-version m)))
        (when verbose (format t "~&[migrations] ↓ rolling back ~A...~%" ver))
        (with-tx ()
          (let ((down-fn (%ensure-down-fn m)))
            (funcall down-fn))
          (exec "DELETE FROM schema_migrations WHERE version=$1" ver))
        (remhash ver applied-set)
        (incf n-down)
        (when verbose (format t "[migrations] ROLLBACK OK ~A~%" ver))))
    (values n-up n-down)))

(defun rollback-one (&key (verbose t))
  (%ensure-table!)
  (let ((last (query-1a "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1")))
    (if (null last)
        (progn
          (when verbose (format t "~&[migrations] nothing to rollback.~%"))
          0)
        (let* ((v (cdr (assoc :version last)))
               (m (%find-mig v)))
          (unless m (error "No migration registered for version ~A" v))
          (when verbose (format t "~&[migrations] ↓ rolling back ~A...~%" v))
          (with-tx ()
            (let ((down-fn (%ensure-down-fn m)))
              (funcall down-fn))
            (exec "DELETE FROM schema_migrations WHERE version=$1" v))
          (when verbose (format t "[migrations] ROLLBACK OK ~A~%" v))
          1))))

(defun status ()
  (%ensure-table!)
  (let* ((applied (%applied-list))
         (applied-index (alexandria:alist-hash-table
                         (mapcar (lambda (r) (cons (cdr (assoc :version r)) r)) applied)
                         :test #'equal)))
    (loop for i from 0 below (length *registry*)
          for m = (aref *registry* i)
          for v = (migration-version m)
          for row = (gethash v applied-index)
          collect
          (list (cons :version v)
                (cons :applied-p (and row t))
                (cons :applied-at (and row (cdr (assoc :applied_at row))))))))

(defun reset! ()
  (%ensure-table!)
  (exec "TRUNCATE TABLE schema_migrations"))
