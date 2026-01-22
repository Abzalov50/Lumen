(in-package :cl-user)

(defpackage :lumen.data.errors
  (:use :cl)
  (:export
   ;; conditions Lumen
   :db-error :db-constraint-violation :db-not-found
   :db-deadlock-retryable :db-timeout :db-serialization-retryable
   ;; API
   :map-db-error :translate-db-error :retryable-db-error-p
   :db-error-message :db-error-constraint :db-error-sqlstate))

(in-package :lumen.data.errors)

;;; ----------------------------------------------------------------------------
;;; Conditions Lumen
;;; ----------------------------------------------------------------------------
(define-condition db-error (error)
  ((message   :initarg :message   :reader db-error-message)
   (sqlstate  :initarg :sqlstate  :reader db-error-sqlstate :initform nil))
  (:report (lambda (c s)
             (format s "DB error: ~@[SQLSTATE=~a ~]~a"
                     (db-error-sqlstate c)
                     (db-error-message c)))))

(define-condition db-constraint-violation (db-error)
  ((constraint :initarg :constraint :reader db-error-constraint))
  (:report (lambda (c s)
             (format s "Constraint violation~@[ (~a)~]~@[ SQLSTATE=~a~]: ~a"
                     (db-error-constraint c)
                     (db-error-sqlstate c)
                     (db-error-message c)))))

(define-condition db-not-found (db-error) ())

(define-condition db-deadlock-retryable (db-error) ()
  (:report (lambda (c s)
             (format s "Deadlock / lock contention (retryable)~@[ SQLSTATE=~a~]: ~a"
                     (db-error-sqlstate c)
                     (db-error-message c)))))

(define-condition db-serialization-retryable (db-error) ()
  (:report (lambda (c s)
             (format s "Serialization failure (retryable)~@[ SQLSTATE=~a~]: ~a"
                     (db-error-sqlstate c)
                     (db-error-message c)))))

(define-condition db-timeout (db-error) ()
  (:report (lambda (c s)
             (format s "Timeout / query canceled~@[ SQLSTATE=~a~]: ~a"
                     (db-error-sqlstate c)
                     (db-error-message c)))))

;;; ----------------------------------------------------------------------------
;;; Helpers
;;; ----------------------------------------------------------------------------
(defun %coerce-message (c)
  (with-output-to-string (s) (princ c s)))

(defun %maybe-sqlstate (c)
  "Essaye de récupérer le SQLSTATE si disponible. Si non, NIL.
cl-postgres encode généralement le code dans la classe de condition,
mais on garde ce hook pour extensions."
  (declare (ignore c))
  nil)

;;; ----------------------------------------------------------------------------
;;; Mapping cl-postgres/postmodern → Lumen
;;; ----------------------------------------------------------------------------
(defun map-db-error (condition)
  "Mappe une condition DB en **condition Lumen** ou NIL si ignorable (warning/notice).
Ne **signale** rien — retourne un objet condition ou NIL."
  (labels ((mk (class &key msg constraint sqlstate)
             (make-condition class
                             :message   (or msg (%coerce-message condition))
                             :constraint constraint
                             :sqlstate  sqlstate)))
    (let* ((msg (%coerce-message condition))
           (st  (%maybe-sqlstate condition)))
      (cond
        ;; --- Notices / Warnings : on les ignore (pas d'erreur)
        ((typep condition 'cl-postgres:postgresql-warning) nil)
        ;;((typep condition 'cl-postgres:postgresql-notice)  nil)

        ;; --- Constraint violations (SQLSTATE 23xxx)
        ((typep condition 'cl-postgres-error:unique-violation)
         (mk 'db-constraint-violation :constraint :unique :sqlstate "23505" :msg msg))
        ((typep condition 'cl-postgres-error:not-null-violation)
         (mk 'db-constraint-violation :constraint :not-null :sqlstate "23502" :msg msg))
        ((typep condition 'cl-postgres-error:foreign-key-violation)
         (mk 'db-constraint-violation :constraint :foreign-key :sqlstate "23503" :msg msg))
        ((typep condition 'cl-postgres-error:check-violation)
         (mk 'db-constraint-violation :constraint :check :sqlstate "23514" :msg msg))

        ;; --- Concurrency / retryable
        ((typep condition 'cl-postgres-error:deadlock-detected)
         (mk 'db-deadlock-retryable :sqlstate "40P01" :msg msg))
        ((typep condition 'cl-postgres-error:serialization-failure)
         (mk 'db-serialization-retryable :sqlstate "40001" :msg msg))
        ((typep condition 'cl-postgres-error:lock-not-available)
         (mk 'db-deadlock-retryable :sqlstate "55P03" :msg msg))

        ;; --- Timeouts / cancellations / admin
        ((typep condition 'cl-postgres-error:query-canceled)
         (mk 'db-timeout :sqlstate "57014" :msg msg))
        ((typep condition 'cl-postgres-error:admin-shutdown)
         (mk 'db-timeout :sqlstate "57P01" :msg msg))
        ((typep condition 'postmodern:database-connection-error)
         (mk 'db-timeout :msg msg))  ;; panne réseau

        ;; --- Fallback générique
        (t (make-condition 'db-error :message msg :sqlstate st))))))

(defun translate-db-error (condition)
  "Mappe et **signale** une erreur Lumen seulement si non-ignorable.
WARNING/NOTICE sont loggués puis ignorés."
  (let ((mapped (map-db-error condition)))
    (cond
      ((null mapped)
       ;; Option : log discret au besoin
       (format *trace-output* "~&[DB][IGNORED] ~a~%" condition)
       (values))              ; ↩︎ rien à signaler
      (t
       (error mapped)))))

(defun retryable-db-error-p (thing)
  "Vrai si l'objet (ou la condition runtime) correspond à une erreur retryable."
  (typep (etypecase thing
           (db-error thing)
           (condition (map-db-error thing)))
         '(or db-deadlock-retryable db-serialization-retryable db-timeout)))
