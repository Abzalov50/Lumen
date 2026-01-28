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
    ;; On nettoie l'existant si on recharge le fichier (pour éviter les doublons en dev)
    (setf *registry* (delete-if (lambda (x)
				  (string= (migration-version x)
					   (migration-version m))) 
                     *registry*))
    (vector-push-extend m *registry*)
    ;; Tri par version
    (let* ((lst (loop for i below (length *registry*) collect (aref *registry* i)))
           (sorted (sort lst (lambda (a b)
                               (string< (migration-version a)
					(migration-version b))))))
      (setf *registry* (make-array (length sorted)
                                   :adjustable t
                                   :fill-pointer (length sorted) 
                                   :initial-contents sorted)))))

(defun %transform-migration-body (forms)
  "Transforme une liste de formes (strings ou code) en code exécutable.
   - String -> (lumen.data.db:exec string)
   - Code   -> Code
   Retourne le tout enveloppé dans un PROGN."
  (let ((executable-forms
          (loop for f in forms
                collect (if (stringp f)
                            `(lumen.data.db:exec ,f)
                            f))))
    ;; On retourne toujours un PROGN explicite pour garantir la structure (lambda () (progn ...))
    `(cl:progn ,@executable-forms)))

;;; --- ANALYSEUR SYNTAXIQUE INTELLIGENT ---

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun %analyze-and-transform (form)
    "Devine si FORM est une liste implicite (nouvelle syntaxe) ou un bloc de code (ancienne)."
    (cond
      ;; CAS 1 : Chaîne unique -> (exec "...")
      ;; Ex: :up "CREATE TABLE..."
      ((stringp form)
       `(lumen.data.db:exec ,form))

      ;; CAS 2 : Rien -> (progn)
      ((null form) 
       '(cl:progn))

      ;; CAS 3 : Legacy Explicite (Commence par PROGN) -> On garde tel quel
      ;; Ex: :up (progn (exec "A") (exec "B"))
      ((and (consp form) (symbolp (car form)) (string-equal (symbol-name (car form)) "PROGN"))
       form)

      ;; CAS 4 : Nouvelle Syntaxe (Liste implicite)
      ;; Condition : C'est une liste ET (Le premier élément est une String OU une sous-liste)
      ;; Ex: :up ("SQL" "SQL") ou :up ((print "A") "SQL")
      ((and (consp form)
            (or (stringp (car form))
                (consp (car form))))
       `(cl:progn
          ,@(loop for step in form collect
                  (if (stringp step)
                      `(lumen.data.db:exec ,step)
                      step))))

      ;; CAS 5 : Legacy Implicite (Forme unique commençant par un symbole)
      ;; Ex: :up (lumen.data.db:exec "SQL")
      ;; Ici le CAR est 'lumen.data.db:exec (un symbole), donc on ne le traite pas comme une liste de strings
      (t form))))

#|
(defmacro defmigration (version &key up down)
  "Définit une migration.
   VERSION : string ou integer.
   UP/DOWN : Liste de strings SQL ou de code Lisp."
  (let ((ver-str (if (integerp version) (write-to-string version) version))
        ;; On transforme le corps pour gérer le sucre syntaxique (strings -> exec)
        (up-body   (%transform-migration-body up))
        (down-body (%transform-migration-body down)))
    
    `(%push-migration!
      (make-migration
       :version    ,ver-str
       ;; On génère directement des lambdas compilées, plus besoin de eval runtime
       :up-forms   #'(lambda () ,up-body)
       :down-forms #'(lambda () ,down-body)))))
|#

(defmacro defmigration (version &key up down)
  "Définit une migration avec support de la syntaxe implicite (liste de strings) et legacy (progn)."
  (let ((ver-str (if (integerp version) (write-to-string version) version))
        (up-body   (%analyze-and-transform up))
        (down-body (%analyze-and-transform down)))
    
    `(%push-migration!
      (make-migration
       :version    ,ver-str
       :up-forms   #'(lambda () ,up-body)
       :down-forms #'(lambda () ,down-body)))))

(defun %ensure-up-fn (mig)
  (migration-up-forms mig))

(defun %ensure-down-fn (mig)
  (migration-down-forms mig))

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
         (applied         (%applied-versions))
         (applied-set     (alexandria:alist-hash-table
                           (mapcar (lambda (v) (cons (string-trim " " v) t)) applied)
                           :test #'equal))
         (all        (loop for i below (length *registry*) collect (aref *registry* i)))
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
        (when verbose (format t "~&[migrations] ↑ applying ~A...~%" ver))
        (run-in-transaction
         (lambda ()
           (funcall (%ensure-up-fn m))
           (exec "INSERT INTO schema_migrations(version) VALUES ($1) ON CONFLICT DO NOTHING" ver)))
        (setf (gethash ver applied-set) t)
        (incf n-up)
        (when verbose (format t "[migrations] OK ~A~%" ver))))
    
    ;; DOWN
    (dolist (m (sort to-down (lambda (a b) (string> (migration-version a) (migration-version b)))))
      (let ((ver (migration-version m)))
        (when verbose (format t "~&[migrations] ↓ rolling back ~A...~%" ver))
        (run-in-transaction
         (lambda ()
           (funcall (%ensure-down-fn m))
           (exec "DELETE FROM schema_migrations WHERE version=$1" ver)))
        (remhash ver applied-set)
        (incf n-down)
        (when verbose (format t "[migrations] ROLLBACK OK ~A~%" ver))))
    (values n-up n-down)))

(defun rollback-one (&key (verbose t))
  (%ensure-table!)
  (let ((last (query-1a "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1")))
    (if (null last)
        (progn (when verbose (format t "~&[migrations] nothing to rollback.~%")) 0)
        (let* ((v (cdr (assoc :version last)))
               (m (%find-mig v)))
          (unless m (error "No migration registered for version ~A" v))
          (when verbose (format t "~&[migrations] ↓ rolling back ~A...~%" v))
          (run-in-transaction
           (lambda ()
             (funcall (%ensure-down-fn m))
             (exec "DELETE FROM schema_migrations WHERE version=$1" v)))
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
  (run-in-transaction
   (lambda () (exec "TRUNCATE TABLE schema_migrations"))))
