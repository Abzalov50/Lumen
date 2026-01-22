(in-package :lumen.test)

(def-suite :form-suite)
(in-suite :form-suite)

(test forms-schema-ui
  (with-test-db
    (with-rollback
      (let ((tname (random-name "users")))
        (unwind-protect
             (progn
               (eval
                `(lumen.data.dao:defentity form-user
                   :table ,(intern (string-upcase tname) :keyword)
                   :fields
                   ((:col :id          :type :uuid :label "ID" :readonly? t :hidden? t :ui-order 0)
                    (:col :email       :type :text :label "Email" :input-type :email :required? t :ui-order 1)
                    (:col :name        :type :text :label "Nom" :required? t :placeholder "ex: Alice" :ui-order 2)
                    (:col :prefs       :type :jsonb :label "Préférences" :ui-order 3)
                    (:col :login_at    :type :timestamptz :label "Dernière connexion" :ui-order 4 :disabled? t)))))
               (let* ((sch (schema 'form-user))
                      (js (getf sch :json-schema))
                      (ui (getf sch :ui))
                      (props (cdr (assoc :properties js))))
		 ;;(print sch)
		 ;;(print props)
		 (print ui)
                 ;; hidden id ne doit pas être required
                 (let ((req (cdr (assoc :required js))))
                   (is (not (find "id" req :test #'string=))))
                 ;; prefs JSON → object + ui:widget textarea par défaut
                 (let* ((p-prefs (lumen.utils:alist-get props "prefs")))
		   ;;(print p-prefs)
                   (is (equal (lumen.utils:alist-get p-prefs :type) "object")))
                   (let* ((prefs-ui (lumen.utils:alist-get ui "prefs")))
		     (print prefs-ui)
                     (is (equal (lumen.utils:alist-get prefs-ui :|ui:widget| :test #'eq) "textarea")))
                 ;; timestamptz → datetime widget
                 (let* ((login-ui (lumen.utils:alist-get ui "login_at")))
                   (is (equal (lumen.utils:alist-get login-ui :|ui:widget| :test #'eq) "datetime"))))
          (drop-table! tname))))))
