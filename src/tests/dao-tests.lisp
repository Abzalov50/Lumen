(in-package :lumen.test)

(def-suite :dao-suite)
(in-suite :dao-suite)

(defun debug-class-slots (obj)
  (mapcar #'closer-mop:slot-definition-name
          (closer-mop:class-slots (class-of obj))))

(test dao-crud-timestamps-types-patch-lock
  (with-test-db
    (with-rollback
      (let ((tname (random-name "users")))
        (unwind-protect
             (progn
               (exec (format nil
			     "create table ~a (
  id uuid primary key default gen_random_uuid(),
  email text unique not null,
  name text not null,
  prefs jsonb,
  tags text[],
  lock_version integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
)" tname))
               ;; Déclaration d'entité de test
               (eval
                `(lumen.data.dao:defentity test-user
                   :table ,(intern (string-upcase tname) :keyword)
                   :primary-key :id
                   :timestamps '(:created :created_at :updated :updated_at :db-fn "now()")
                   :lock-version :lock_version
                   :fields
                   ((:col :id          :type :uuid        :label "ID" :readonly? t :hidden? t)
                    (:col :email       :type :text        :required? t :input-type :email)
                    (:col :name        :type :text        :required? t)
                    (:col :prefs       :type :jsonb)
                    (:col :tags        :type (:array :text))
                    (:col :lock_version :type :integer :readonly? t :hidden? t)
                    (:col :created_at  :type :timestamptz :readonly? t :hidden? t)
                    (:col :updated_at  :type :timestamptz :readonly? t :hidden? t)))))
          ;; INSERT (timestamps forcés via now())
          (let* ((u (make-instance 'test-user
                                   :email "a@example.com"
                                   :name "Alice"
                                   :prefs '((:theme . "dark"))
                                   :tags #("x" "y"))))
	    ;;#|
	    (multiple-value-bind (n u1)
		(entity-insert! u :returning t)
	      (declare (ignore n))
	      (flet ((sv (o n) (slot-value o (entity-slot-symbol 'test-user n))))
		;;(format t "~&id=~S~%" (sv u1 'id))
		;;(format t "~&name=~S~%" (sv u1 'name))
		;;(format t "~&created_at=~S~%" (sv u1 'created_at))
		;;(format t "~&updated_at=~S~%" (sv u1 'updated_at))
		;;(format t "~&lock_version=~S~%" (sv u1 'lock_version))
		(is (slot-value u1 'id))
		(is (sv u1 'created_at))
		(is (slot-value u1 'updated_at))
		(is (equal (slot-value u1 'name) "Alice"))
		(is (zerop (length (dirty-slots u1)))))))
            ;; PATCH update (seulement name) + incr lock_version + touch updated_at
          (let* ((row (query-1a (format nil "select * from ~a where email=$1" tname)
				"a@example.com"))
                   (u (row->entity 'test-user row))
                   (ver0 (slot-value u 'lock_version)))
	      ;;(format t "~&ver0=~D~%" ver0)
              (setf (slot-value u 'name) "Alice B.")
	      ;;(format t "~&dirty-slots=~D~%" (length (dirty-slots u)))
              (is (= (length (dirty-slots u)) 1))
              (let ((u2 (entity-update! u :patch t)))
		;;(format t "~&U2 NAME=~S~%" (slot-value u2 'name))
		;;(format t "~&LOCK VERSION=~D~%" (slot-value u2 'lock_version))
		(is (equal (slot-value u2 'name) "Alice B."))
		(is (= (slot-value u2 'lock_version) (+ ver0 1)))))
            ;; Conflit d’optimistic locking
	    ;;#|
            (let* ((row (query-1a (format nil "select * from ~a where email=$1" tname) "a@example.com"))
                   (uA (row->entity 'test-user row))
                   (uB (row->entity 'test-user row)))
	    
              ;; Udpate A OK
              (setf (slot-value uA 'name) "A-1")
              (entity-update! uA :patch t)
              ;; Udpate B avec lock_version périmée → conflit
              (setf (slot-value uB 'name) "B-1")
	   
              (handler-case
                  (entity-update! uB :patch t)
		(concurrent-update-error ()
		  (pass "Conflicting update detected")))
	    
	      )
	    ;;|#
            (drop-table! tname))))))
