(in-package :lumen.test)

(def-suite :repo-suite)
(in-suite :repo-suite)

(defun seed-users (tname)
  (exec (format nil "create table ~a (id serial primary key, email text unique, name text, created_at timestamptz default now())" tname))
  (loop for i from 1 to 30 do
        (exec (format nil "insert into ~a(email,name,created_at) values($1,$2, now() - ($3||' minutes')::interval)" tname)
              (format nil "u~a@example.com" i)
              (format nil "User ~a" i)
              i)))

(test repo-select-count-order
  (with-test-db
    (with-rollback
      (let ((tname (random-name "users")))
        (unwind-protect
             (progn
               (seed-users tname)
               (let ((n (count* tname :filters '())))
                 (is (= n 30)))
               (let ((rows (select* tname
                                    :filters '((= :name "User 10"))
                                    :order '((:id :asc))
                                    :order-whitelist '(:id :name))))
                 (is (= (length rows) 1))
                 (is (equal (cdr (assoc :email (first rows))) "u10@example.com"))))
	  ;;(sleep 60)
          (drop-table! tname))))))

(test repo-upsert
  (with-test-db
    (with-rollback
      (let ((tname (random-name "users")))
        (unwind-protect
             (progn
               (exec (format nil "create table ~a (id serial primary key, email text unique, name text, updated_at timestamptz default now())" tname))
               ;; insert
               (multiple-value-bind (n ret)
                   (upsert* tname '((:email . "a@example.com") (:name . "Alice"))
                            :conflict-columns '(:email) :returning t)
                 (is (= n 1))
                 (is (equal (cdr (assoc :name ret)) "Alice")))
               ;; update on conflict
               (multiple-value-bind (n ret)
                   (upsert* tname '((:email . "a@example.com") (:name . "Alice2"))
                            :conflict-columns '(:email)
                            :update-columns '(:name)
                            :touch-updated-col :updated_at
                            :returning t)
                 (is (= n 1))
                 (is (equal (cdr (assoc :name ret)) "Alice2"))))
          (drop-table! tname))))))

(test repo-keyset
  (with-test-db
    (with-rollback
      (let ((tname (random-name "users")))
        (unwind-protect
             (progn
               (seed-users tname)
               ;; page descendante
               (let* ((page1 (select-page-keyset* tname
                               :order '((:created_at :desc))
                               :key :created_at
                               :limit 10))
                      (items1 (getf page1 :items))
                      (cursor1 (getf page1 :next-cursor)))
                 (is (= (length items1) 10))
                 (let* ((page2 (select-page-keyset* tname
                                :order '((:created_at :desc))
                                :key :created_at
                                :after cursor1
                                :limit 10))
                        (items2 (getf page2 :items)))
                   (is (= (length items2) 10))
                   ;; la première ligne de page2 doit être < dernière de page1
                   (let ((t1 (cdr (assoc :created-at (car (last items1)))))
                         (t2 (cdr (assoc :created-at (first items2)))))
                     (is (< t2 t1)))))
               ;; page précédente (inverse)
               (let* ((pageA (select-page-keyset* tname
                               :order '((:created_at :desc))
                               :key :created_at :limit 5))
                      (firstA (first (getf pageA :items)))
                      (before (cdr (assoc :created-at firstA)))
                      (pagePrev (select-page-keyset-prev* tname
                                 :order '((:created_at :desc))
                                 :key :created_at
                                 :before before
                                 :limit 5)))
                 (is (<= (length (getf pagePrev :items)) 5))))
          (drop-table! tname))))))
