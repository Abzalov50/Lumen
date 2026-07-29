(in-package :lumen.test)

(def-suite :db-suite)
(in-suite :db-suite)

(test db-network-error-classification
  (dolist
      (message
       '("Connection to database server lost."
         "Database server connection lost."
         "Connection is closed."
         "Connection not open."))

    (is
     (lumen.utils:db-network-error-p
      (make-condition
       'simple-error
       :format-control message)))))

(test db-basic-select
  (with-test-db
    (with-rollback
      (let ((tname (random-name "t_sel")))
        (unwind-protect
             (progn
               (exec (format nil "create table ~a (id serial primary key, v text)" tname))
               (exec (format nil "insert into ~a(v) values($1),($2)" tname) "a" "b")
               (multiple-value-bind (rows n)
                   (query-a (format nil "select * from ~a order by id asc" tname))
                 (is (= n 2))
                 (is (equal (cdr (assoc :v (first rows))) "a"))
                 (is (equal (cdr (assoc :v (second rows))) "b")))
               (let ((row (query-1a (format nil "select * from ~a where v=$1" tname) "b")))
                 (is (equal (cdr (assoc :v row)) "b"))))
          (drop-table! tname))))))

(test db-exec-returning
  (with-test-db
    (with-rollback
      (let ((tname (random-name "t_ins")))
        (unwind-protect
             (progn
               (exec (format nil "create table ~a (id serial primary key, v text)" tname))
               (multiple-value-bind (n ret)
                   (exec (format nil "insert into ~a(v) values($1) returning id, v" tname) "x")
                 (is (= n 1))
                 (is (equal (cdr (assoc :v ret)) "x"))))
          (drop-table! tname))))))

(test db-timeout-and-slowlog
  (with-test-db
    (with-rollback
      (let ((got-slow nil))
        (let ((*on-slow-query* (lambda (sql &key params rows affected)
                                 (declare (ignore sql params rows affected))
                                 (setf got-slow t)))
              (*slow-query-ms* 100.0))
          ;; pg_sleep(0.2) ~200ms > 100 → slow
          (query-a "select pg_sleep($1)" 0.2d0)
          (is (eq got-slow t)))
        ;; timeout 50ms < 100ms sleep → erreur de timeout
        (handler-case
            (query-a "select pg_sleep($1)" 0.1d0 :timeout-ms 50)
          (t (e) (is (typep e 'error))))))))

(test db-discards-a-terminated-pooled-connection
  (with-test-db
    (let* ((config
             lumen.core.context:*current-db-config*)

           (killer
             nil)

           (backend-pid
             nil))

      (unwind-protect
           (progn
             ;; Crée une connexion, puis la rend au pool.
             (lumen.data.db:with-conn ()
               (setf
                backend-pid
                (postmodern:query
                 "SELECT pg_backend_pid()"
                 :single)))

             ;; La connexion de contrôle appartient au même utilisateur et
             ;; termine exclusivement le backend créé par ce test.
             (setf
              killer
              (postmodern:connect
               (getf config :database)
               (getf config :user)
               (getf config :password)
               (or
                (getf config :host)
                "localhost")
               :port
               (or
                (getf config :port)
                5432)
               :use-ssl
               (case
                   (getf config :sslmode)
                 (:require :yes)
                 (t :no))
               :pooled-p nil))

             (let ((postmodern:*database*
                     killer))

               (unless
                   (postmodern:query
                    "SELECT pg_terminate_backend($1)"
                    backend-pid
                    :single)

                 (fiveam:skip
                  "Le rôle PostgreSQL de test ne peut pas terminer sa propre connexion.")))

             ;; L'emprunt suivant doit détecter la socket morte, la jeter,
             ;; créer une connexion saine et exécuter le traitement une seule
             ;; fois.
             (let ((application-call-count 0))

               (is
                (=
                 42

                 (lumen.data.db:with-conn ()
                   (incf application-call-count)

                   (postmodern:query
                    "SELECT 42"
                    :single))))

               (is
                (=
                 1
                 application-call-count))))

        (when killer
          (ignore-errors
            (postmodern:disconnect
             killer)))))))
