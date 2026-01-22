;;;; tests/util.lisp
(in-package :cl)

(defpackage :lumen.tests.util
  (:use :cl :fiveam)
  (:import-from :lumen.data.config :db-config)
  (:import-from :lumen.data.db :start! :stop! :with-tx :query-a :query-1a :exec)
  (:export :with-test-db :random-name :ensure-schema! :drop-table!
           :*have-db* :skip-when-no-db :with-rollback
   :pg-now))

(in-package :lumen.tests.util)

(defparameter *have-db* t)

(defun env (name &optional (default nil))
  (or (uiop:getenv name) default))

(defun random-name (&optional (prefix "tmp"))
  (format nil "~a_~36R" prefix (random (expt 36 6))))

(defun ensure-schema! ()
  ;; essaie d’ouvrir une connexion via start! utilisant les envs
  (handler-case
      (progn
        (lumen.data.db:start!
         :config (list :database (env "LUMEN_TEST_DB" "db_test_lumen")
                       :user     (env "LUMEN_TEST_USER" "postgres")
                       :password (env "LUMEN_TEST_PASS" "@Hensek999")
                       :host     (env "LUMEN_TEST_HOST" "localhost")
                       :port     (parse-integer (env "LUMEN_TEST_PORT" "5432"))
                       :pool-size 4
                       :application-name "lumen-tests"))
	(format t "~&CONN DB: ~A~%" pomo:*database*))    
    (error (e)
      (setf *have-db* nil)
      (format *error-output* "~&[WARN] No DB for tests: ~a~%" e)))
  *have-db*)

(defun drop-table! (table)
  (ignore-errors
    (exec (format nil "drop table if exists ~a cascade" table))))

(defmacro skip-when-no-db (&body body)
  `(if (not *have-db*)
       (fiveam:skip "No DB configured (set LUMEN_TEST_* env vars).")
       (progn ,@body)))

(defmacro with-test-db (&body body)
  `(progn
     (ensure-schema!)
     (skip-when-no-db
       ,@body)))

(defmacro with-rollback (&body body)
  "Exécute BODY dans UNE connexion Postmodern unique, BEGIN au début, ROLLBACK à la fin.
Toutes les requêtes internes partagent la même *database*."
  `(lumen.data.db:with-conn
     (lumen.data.db:exec "BEGIN")
     (unwind-protect
          (progn ,@body)
       (ignore-errors (lumen.data.db:exec "ROLLBACK")))))

(defun pg-now ()
  (lumen.data.db:query-1a "select now() as now"))
