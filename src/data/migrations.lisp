(in-package :cl)

(defpackage :lumen.data.migrations
  (:use :cl)
  (:import-from :lumen.utils
    :str-prefix-p)
  (:import-from :lumen.data.db
    :with-tx :exec :query-a :query-1a :ensure-connection :run-in-transaction)
  (:export
    :defmigration
    :migrate-all :migrate-up-to :rollback-one :status :reset!))
(in-package :lumen.data.migrations)

;;;; Registry (Isolé par Application)
(defstruct migration app-name version up-forms down-forms)

;; *registry* devient une hash-table. 
;; Clé: Nom de l'app (Keyword), Valeur: Tableau de migrations
(defvar *registry* (make-hash-table :test 'eq))
(defvar *lock* (bt:make-lock "lumen.migrations"))

(defun %to-version-string (v)
  "Normalise VERSION en string. Accepte integer ou string."
  (etypecase v
    (integer (write-to-string v :base 10))
    (string  v)))

(defun %app-registry (app-name)
  "Retourne le tableau des migrations pour une app donnée."
  (gethash app-name *registry* (make-array 0 :adjustable t :fill-pointer 0)))

(defun %push-migration! (app-name m)
  (bt:with-lock-held (*lock*)
    (let ((app-reg (%app-registry app-name)))
      ;; On nettoie l'existant si on recharge le fichier (pour éviter les doublons en dev)
      (setf app-reg (delete-if (lambda (x)
                                 (string= (migration-version x)
                                          (migration-version m))) 
                               app-reg))
      (vector-push-extend m app-reg)
      ;; Tri par version
      (let* ((lst (loop for i below (length app-reg) collect (aref app-reg i)))
             (sorted (sort lst (lambda (a b)
                                 (string< (migration-version a)
                                          (migration-version b))))))
        (setf (gethash app-name *registry*)
              (make-array (length sorted)
                          :adjustable t
                          :fill-pointer (length sorted) 
                          :initial-contents sorted))))))

(defun %transform-migration-body (forms)
  (let ((executable-forms
          (loop for f in forms
                collect (if (stringp f)
                            `(lumen.data.db:exec ,f)
                            f))))
    `(cl:progn ,@executable-forms)))

;;; --- ANALYSEUR SYNTAXIQUE INTELLIGENT ---
(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun %analyze-and-transform (form)
    (cond
      ((stringp form) `(lumen.data.db:exec ,form))
      ((null form) '(cl:progn))
      ((and (consp form) (symbolp (car form)) (string-equal (symbol-name (car form)) "PROGN")) form)
      ((and (consp form) (or (stringp (car form)) (consp (car form))))
       `(cl:progn
          ,@(loop for step in form collect
                  (if (stringp step) `(lumen.data.db:exec ,step) step))))
      (t form))))

(defmacro defmigration (app-name version &key up down)
  "Définit une migration rattachée à une application (APP-NAME)."
  (let ((ver-str (if (integerp version) (write-to-string version) version))
        (up-body   (%analyze-and-transform up))
        (down-body (%analyze-and-transform down)))
    
    ;; On s'assure que app-name est un keyword à la compilation
    `(let ((app-kw (intern (string-upcase (string ',app-name)) "KEYWORD")))
       (%push-migration! app-kw
         (make-migration
          :app-name   app-kw
          :version    ,ver-str
          :up-forms   #'(lambda () ,up-body)
          :down-forms #'(lambda () ,down-body))))))

(defun %ensure-up-fn (mig) (migration-up-forms mig))
(defun %ensure-down-fn (mig) (migration-down-forms mig))

;;;; Storage
(defun %ensure-table! ()
  (run-in-transaction
   (lambda ()
      (ignore-errors (postmodern:query "set local client_min_messages = 'error'"))
      (exec
      "CREATE TABLE IF NOT EXISTS schema_migrations (
        version text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )"))))

(defun %applied-list ()
  (ensure-connection
    (multiple-value-bind (rows _n)
        (query-a "SELECT version, applied_at FROM schema_migrations ORDER BY version ASC")
      (declare (ignore _n))
      rows)))

(defun %applied-versions ()
  (mapcar (lambda (r) (cdr (assoc :version r))) (%applied-list)))

(defun %find-mig (app-name vstr)
  (let ((reg (%app-registry app-name)))
    (loop for i from 0 below (length reg)
          for m = (aref reg i)
          when (string= (migration-version m) vstr) do (return m))))

(defun %max-registered-version (app-name)
  (let ((reg (%app-registry app-name)))
    (when (> (length reg) 0)
      (migration-version (aref reg (1- (length reg)))))))

;;;; Commands (Mise à jour : Nécessitent app-name)

(defun migrate-all (app-name &key (verbose t))
  "Applique toutes les migrations non appliquées pour une application spécifique."
  (%ensure-table!)
  (let* ((app-kw (intern (string-upcase (string app-name)) "KEYWORD"))
         (target (%max-registered-version app-kw)))
    (if target
        (migrate-up-to app-kw target :verbose verbose)
        (progn
          (when verbose (format t "~&[migrations:~A] nothing to migrate.~%" app-kw))
          (values 0 0)))))

(defun migrate-up-to (app-name target &key (verbose t))
  (%ensure-table!)
  (let* ((app-kw (intern (string-upcase (string app-name)) "KEYWORD"))
         (tgt (%to-version-string target))
         (applied          (%applied-versions))
         (applied-set      (alexandria:alist-hash-table
                            (mapcar (lambda (v) (cons (string-trim " " v) t)) applied)
                            :test #'equal))
         (reg        (%app-registry app-kw))
         (all        (loop for i below (length reg) collect (aref reg i)))
         (all-uniq   (remove-duplicates all :key #'migration-version :test #'equal :from-end t))
         (to-up      (remove-if (lambda (m)
                                  (or (gethash (string-trim " " (migration-version m)) applied-set)
                                      (string< tgt (migration-version m))))
                                all-uniq))
         (to-down    (remove-if-not (lambda (m)
                                      (and (gethash (string-trim " " (migration-version m)) applied-set)
                                           (string< tgt (migration-version m))))
                                    all-uniq))
         (n-up 0) (n-down 0))
    
    ;; UP
    (dolist (m (sort to-up (lambda (a b) (string< (migration-version a) (migration-version b)))))
      (let ((ver (migration-version m)))
        (when verbose (format t "~&[migrations:~A] ↑ applying ~A...~%" app-kw ver))
        (run-in-transaction
         (lambda ()
           (funcall (%ensure-up-fn m))
           (exec "INSERT INTO schema_migrations(version) VALUES ($1) ON CONFLICT DO NOTHING" ver)))
        (setf (gethash ver applied-set) t)
        (incf n-up)
        (when verbose (format t "[migrations:~A] OK ~A~%" app-kw ver))))
    
    ;; DOWN
    (dolist (m (sort to-down (lambda (a b) (string> (migration-version a) (migration-version b)))))
      (let ((ver (migration-version m)))
        (when verbose (format t "~&[migrations:~A] ↓ rolling back ~A...~%" app-kw ver))
        (run-in-transaction
         (lambda ()
           (funcall (%ensure-down-fn m))
           (exec "DELETE FROM schema_migrations WHERE version=$1" ver)))
        (remhash ver applied-set)
        (incf n-down)
        (when verbose (format t "[migrations:~A] ROLLBACK OK ~A~%" app-kw ver))))
    (values n-up n-down)))

(defun rollback-one (app-name &key (verbose t))
  (%ensure-table!)
  (let* ((app-kw (intern (string-upcase (string app-name)) "KEYWORD"))
         (last (query-1a "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1")))
    (if (null last)
        (progn (when verbose (format t "~&[migrations:~A] nothing to rollback.~%" app-kw)) 0)
        (let* ((v (cdr (assoc :version last)))
               (m (%find-mig app-kw v)))
          (unless m (error "No migration registered for version ~A in app ~A" v app-kw))
          (when verbose (format t "~&[migrations:~A] ↓ rolling back ~A...~%" app-kw v))
          (run-in-transaction
           (lambda ()
             (funcall (%ensure-down-fn m))
             (exec "DELETE FROM schema_migrations WHERE version=$1" v)))
          (when verbose (format t "[migrations:~A] ROLLBACK OK ~A~%" app-kw v))
          1))))

(defun status (app-name)
  (%ensure-table!)
  (let* ((app-kw (intern (string-upcase (string app-name)) "KEYWORD"))
         (applied (%applied-list))
         (applied-index (alexandria:alist-hash-table
                         (mapcar (lambda (r) (cons (cdr (assoc :version r)) r)) applied)
                         :test #'equal))
         (reg (%app-registry app-kw)))
    (loop for i from 0 below (length reg)
          for m = (aref reg i)
          for v = (migration-version m)
          for row = (gethash v applied-index)
          collect
          (list (cons :version v)
                (cons :applied-p (and row t))
                (cons :applied-at (and row (cdr (assoc :applied_at row))))))))

(defun reset! ()
  (%ensure-table!)
  (run-in-transaction
   (lambda () (exec "TRUNCATE TABLE schema_migrations"))))
