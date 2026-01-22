(in-package :lumen.test)

(def-suite :db-suite)
(in-suite :db-suite)

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
