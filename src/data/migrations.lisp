(in-package :cl)

(defpackage :lumen.data.migrations
  (:use :cl)

  (:import-from :lumen.data.db
                :exec
                :query-a
                :query-1a
                :run-in-transaction)

  (:export
   :defmigration
   :ensure-storage!
   :migrate-all
   :migrate-up-to
   :rollback-one
   :status
   :reset!))

(in-package :lumen.data.migrations)


;;;; ============================================================
;;;; Configuration du stockage
;;;; ============================================================

(defparameter *migration-table-schema* "public")

(defparameter *migration-table-name* "schema_migrations")


(defun %safe-sql-identifier-p (value)
  "Vérifie qu'une chaîne peut être utilisée comme identifiant SQL simple."
  (and
   (stringp value)
   (plusp (length value))

   (let ((first-char (char value 0)))
     (or (alpha-char-p first-char)
         (char= first-char #\_)))

   (every
    (lambda (char)
      (or (alphanumericp char)
          (char= char #\_)))
    value)))


(defun %migration-table-fqn ()
  "Retourne le nom SQL pleinement qualifié de la table des migrations."
  (unless (%safe-sql-identifier-p *migration-table-schema*)
    (error "Invalid migration schema name: ~S"
           *migration-table-schema*))

  (unless (%safe-sql-identifier-p *migration-table-name*)
    (error "Invalid migration table name: ~S"
           *migration-table-name*))

  (format nil
          "~A.~A"
          *migration-table-schema*
          *migration-table-name*))


(defun %quote-sql-identifier (value)
  "Protège un identifiant SQL provenant du catalogue PostgreSQL."
  (format nil
          "\"~A\""
          (with-output-to-string (out)
            (loop
              for char across value
              do
                 (if (char= char #\")
                     (write-string "\"\"" out)
                     (write-char char out))))))


;;;; ============================================================
;;;; Helpers génériques
;;;; ============================================================

(defun %row-value (row &rest keys)
  "Lit une valeur dans une alist PostgreSQL en tolérant plusieurs
conventions de nommage des clés."
  (loop
    for key in keys
    for entry = (assoc key row)
    when entry
      do (return (cdr entry))))


(defun %nullish-p (value)
  "Reconnaît les principales représentations d'une valeur absente."
  (or
   (null value)

   (and
    (symbolp value)
    (member
     (string-downcase (symbol-name value))
     '("null" "nil")
     :test #'string=))

   (and
    (stringp value)
    (member
     (string-downcase
      (string-trim
       '(#\Space #\Tab #\Newline #\Return)
       value))
     '("" "null" "nil")
     :test #'string=))))


(defun %truthy-p (value)
  "Convertit les formes booléennes usuelles en booléen Lisp."
  (cond
    ((or (eq value t)
         (eq value :true))
     t)

    ((stringp value)
     (member
      (string-downcase
       (string-trim
        '(#\Space #\Tab #\Newline #\Return)
        value))
      '("t" "true" "1" "yes" "on")
      :test #'string=))

    ((numberp value)
     (not (zerop value)))

    (t
     nil)))


(defun %normalize-app-key (app-name)
  "Normalise le nom applicatif sous forme de keyword."
  (etypecase app-name
    (keyword
     app-name)

    (symbol
     (intern
      (string-upcase
       (symbol-name app-name))
      "KEYWORD"))

    (string
     (intern
      (string-upcase app-name)
      "KEYWORD"))))


(defun %app-name-string (app-name)
  "Forme persistée du nom d'application."
  (string-downcase
   (symbol-name
    (%normalize-app-key app-name))))


(defun %digits-only-p (value)
  (and
   (stringp value)
   (plusp (length value))
   (every #'digit-char-p value)))


(defun %version< (left right)
  "Compare deux versions.

Les versions entièrement numériques sont comparées numériquement.
Les autres sont comparées lexicographiquement."
  (if (and (%digits-only-p left)
           (%digits-only-p right))
      (< (parse-integer left)
         (parse-integer right))
      (string< left right)))


(defun %version> (left right)
  (%version< right left))


(defun %to-version-string (value)
  "Normalise VERSION sous forme de chaîne."
  (etypecase value
    (integer
     (write-to-string value :base 10))

    (string
     value)))


;;;; ============================================================
;;;; Registre des migrations
;;;; ============================================================

(defstruct migration
  app-name
  version
  up-forms
  down-forms)


(defvar *registry*
  (make-hash-table :test #'eq)
  "Registre des migrations, isolé par application.")


(defvar *lock*
  (bt:make-lock "lumen.migrations"))


(defun %app-registry (app-name)
  (or
   (gethash
    (%normalize-app-key app-name)
    *registry*)

   (make-array
    0
    :adjustable t
    :fill-pointer 0)))


(defun %push-migration! (app-name migration)
  "Enregistre une migration sans doublon de version."
  (bt:with-lock-held (*lock*)
    (let* ((app-key
             (%normalize-app-key app-name))

           (registry
             (%app-registry app-key))

           (existing
             (loop
               for index below (length registry)
               collect (aref registry index)))

           (without-same-version
             (remove
              (migration-version migration)
              existing
              :key #'migration-version
              :test #'string=))

           (sorted
             (sort
              (cons migration without-same-version)
              (lambda (left right)
                (%version<
                 (migration-version left)
                 (migration-version right))))))

      (setf
       (gethash app-key *registry*)
       (make-array
        (length sorted)
        :adjustable t
        :fill-pointer (length sorted)
        :initial-contents sorted))

      migration)))


(eval-when (:compile-toplevel :load-toplevel :execute)

  (defun %analyze-and-transform (form)
    (cond
      ((stringp form)
       `(lumen.data.db:exec ,form))

      ((null form)
       '(cl:progn))

      ((and
        (consp form)
        (symbolp (car form))
        (string-equal
         (symbol-name (car form))
         "PROGN"))
       form)

      ((and
        (consp form)
        (or
         (stringp (car form))
         (consp (car form))))
       `(cl:progn
          ,@(loop
              for step in form
              collect
              (if (stringp step)
                  `(lumen.data.db:exec ,step)
                  step))))

      (t
       form))))


(defmacro defmigration (app-name version &key up down)
  "Définit une migration rattachée à APP-NAME."
  (let ((version-string
          (if (integerp version)
              (write-to-string version)
              version))

        (up-body
          (%analyze-and-transform up))

        (down-body
          (%analyze-and-transform down)))

    `(let ((app-key
             (%normalize-app-key ',app-name)))

       (%push-migration!
        app-key

        (make-migration
         :app-name app-key
         :version ,version-string
         :up-forms
         (lambda ()
           ,up-body)
         :down-forms
         (lambda ()
           ,down-body))))))


(defun %ensure-up-fn (migration)
  (migration-up-forms migration))


(defun %ensure-down-fn (migration)
  (migration-down-forms migration))


;;;; ============================================================
;;;; Inspection du stockage
;;;; ============================================================

(defun %storage-exists-p ()
  (let* ((table-name
           (%migration-table-fqn))

         (row
           (query-1a
            "SELECT to_regclass($1)::text AS relation_name"
            table-name))

         (relation-name
           (%row-value
            row
            :relation-name
            :relation_name
            :*relation--name*)))

    (not (%nullish-p relation-name))))


(defun %column-exists-p (column-name)
  (let* ((row
           (query-1a
            "SELECT EXISTS (
                 SELECT 1
                   FROM information_schema.columns
                  WHERE table_schema = $1
                    AND table_name = $2
                    AND column_name = $3
             ) AS exists_p"
            *migration-table-schema*
            *migration-table-name*
            column-name))

         (value
           (%row-value
            row
            :exists-p
            :exists_p
            :*exists--p*)))

    (%truthy-p value)))


(defun %composite-primary-key-exists-p ()
  "Vérifie que la clé primaire porte exactement sur (app_name, version)."
  (let* ((table-name (%migration-table-fqn))
         (row
           (query-1a
            "SELECT EXISTS (
                 SELECT 1
                   FROM pg_constraint AS c
                  WHERE c.conrelid = to_regclass($1)
                    AND c.contype = 'p'
                    AND (
                        SELECT array_agg(a.attname::text ORDER BY k.ordinality)
                          FROM unnest(c.conkey)
                               WITH ORDINALITY AS k(attnum, ordinality)
                          JOIN pg_attribute AS a
                            ON a.attrelid = c.conrelid
                           AND a.attnum = k.attnum
                    ) = ARRAY['app_name', 'version']::text[]
             ) AS exists_p"
            table-name))
         (value
           (%row-value row
                       :exists-p
                       :exists_p
                       :*exists--p*)))
    (%truthy-p value)))

(defun %primary-key-name ()
  (let* ((table-name
           (%migration-table-fqn))

         (row
           (query-1a
            "SELECT constraint_info.conname
               FROM pg_constraint AS constraint_info
              WHERE constraint_info.conrelid = to_regclass($1)
                AND constraint_info.contype = 'p'
              LIMIT 1"
            table-name)))

    (%row-value
     row
     :conname
     :*conname*)))


(defun %null-app-name-exists-p ()
  (let* ((table-name
           (%migration-table-fqn))

         (row
           (query-1a
            (format nil
                    "SELECT EXISTS (
                         SELECT 1
                           FROM ~A
                          WHERE app_name IS NULL
                     ) AS exists_p"
                    table-name)))

         (value
           (%row-value
            row
            :exists-p
            :exists_p
            :*exists--p*)))

    (%truthy-p value)))


;;;; ============================================================
;;;; Bootstrap et mise à niveau du stockage
;;;; ============================================================

(defun %create-storage! ()
  (let ((table-name
          (%migration-table-fqn)))

    (exec
     (format nil
             "CREATE TABLE ~A (
                  app_name text NOT NULL,
                  version text NOT NULL,
                  applied_at timestamptz NOT NULL DEFAULT now(),

                  CONSTRAINT schema_migrations_pkey
                      PRIMARY KEY (app_name, version)
              )"
             table-name))

    t))


(defun %upgrade-legacy-storage! (app-name)
  "Transforme l'ancienne table globale version/applied_at en table
isolée par application.

Toutes les anciennes lignes sont attribuées à APP-NAME. Cette reprise
est adaptée aux bases historiques qui n'hébergeaient qu'une seule
application dans la table de migrations."
  (let* ((table-name
           (%migration-table-fqn))

         (app-name-string
           (%app-name-string app-name)))

    (unless (%column-exists-p "version")
      (error
       "La table ~A ne contient pas la colonne version."
       table-name))

    (unless (%column-exists-p "applied_at")
      (exec
       (format nil
               "ALTER TABLE ~A
                    ADD COLUMN applied_at timestamptz
                    NOT NULL DEFAULT now()"
               table-name)))

    (unless (%column-exists-p "app_name")
      (exec
       (format nil
               "ALTER TABLE ~A
                    ADD COLUMN app_name text"
               table-name)))

    ;; Les anciennes versions sont rattachées à l'application
    ;; qui effectue la première mise à niveau.
    (exec
     (format nil
             "UPDATE ~A
                 SET app_name = $1
               WHERE app_name IS NULL"
             table-name)
     app-name-string)

    (exec
     (format nil
             "ALTER TABLE ~A
                  ALTER COLUMN app_name SET NOT NULL"
             table-name))

    ;; L'ancienne clé primaire portait seulement sur version.
    (unless (%composite-primary-key-exists-p)
      (let ((primary-key-name
              (%primary-key-name)))

        (when primary-key-name
          (exec
           (format nil
                   "ALTER TABLE ~A
                        DROP CONSTRAINT ~A"
                   table-name
                   (%quote-sql-identifier
                    primary-key-name))))

        (exec
         (format nil
                 "ALTER TABLE ~A
                      ADD CONSTRAINT schema_migrations_pkey
                      PRIMARY KEY (app_name, version)"
                 table-name))))

    t))


(defun ensure-storage!
    (app-name
     &key
       (bootstrap-p nil)
       (upgrade-legacy-p nil))

  "Vérifie et, lorsque cela est explicitement autorisé, initialise
le stockage des migrations.

BOOTSTRAP-P autorise la création de public.schema_migrations.

UPGRADE-LEGACY-P autorise la conversion de l'ancienne structure
(version, applied_at) vers :
(app_name, version, applied_at).

Par défaut, cette fonction ne modifie jamais la structure de la base."
  (let ((app-key
          (%normalize-app-key app-name)))

    (run-in-transaction
     (lambda ()
       (ignore-errors
         (exec
          "SET LOCAL client_min_messages = 'error'"))

       (cond
         ;; Aucune table : création seulement en mode explicite.
         ((not (%storage-exists-p))
          (unless bootstrap-p
            (error
             "La table ~A est absente. Exécutez les migrations \
avec :bootstrap-p t et un rôle de migration."
             (%migration-table-fqn)))

          (%create-storage!))

         ;; Table historique sans app_name.
         ((not (%column-exists-p "app_name"))
          (unless upgrade-legacy-p
            (error
             "La table ~A utilise encore l'ancien format. \
Exécutez la migration avec :upgrade-legacy-p t."
             (%migration-table-fqn)))

          (%upgrade-legacy-storage! app-key))

         ;; app_name existe, mais la structure peut rester incomplète.
         (t
          (when (%null-app-name-exists-p)
            (unless upgrade-legacy-p
              (error
               "La table ~A contient des lignes sans app_name."
               (%migration-table-fqn)))

            (%upgrade-legacy-storage! app-key))

          (unless (%composite-primary-key-exists-p)
            (unless upgrade-legacy-p
              (error
               "La clé primaire de ~A doit être (app_name, version)."
               (%migration-table-fqn)))

            (%upgrade-legacy-storage! app-key))))

       t))))


;;;; ============================================================
;;;; Consultation des migrations appliquées
;;;; ============================================================

(defun %applied-list (app-name)
  (let ((table-name
          (%migration-table-fqn))

        (app-name-string
          (%app-name-string app-name)))

    (multiple-value-bind (rows count)
        (query-a
         (format nil
                 "SELECT version, applied_at
                    FROM ~A
                   WHERE app_name = $1
                   ORDER BY applied_at ASC, version ASC"
                 table-name)
         app-name-string)

      (declare (ignore count))
      rows)))


(defun %applied-versions (app-name)
  (mapcar
   (lambda (row)
     (%row-value row :version :*version*))
   (%applied-list app-name)))


(defun %find-migration (app-name version-string)
  (let ((registry
          (%app-registry app-name)))

    (loop
      for index below (length registry)
      for migration = (aref registry index)
      when
        (string=
         (migration-version migration)
         version-string)
        do
           (return migration))))


(defun %max-registered-version (app-name)
  (let ((registry
          (%app-registry app-name)))

    (when (plusp (length registry))
      (migration-version
       (aref registry
             (1- (length registry)))))))


(defun %applied-version-table (app-name)
  (let ((table
          (make-hash-table :test #'equal)))

    (dolist (version (%applied-versions app-name))
      (setf
       (gethash
        (string-trim " " version)
        table)
       t))

    table))


;;;; ============================================================
;;;; Exécution interne
;;;; ============================================================

(defun %migrate-up-to-internal
    (app-name target
     &key
       (verbose t))

  (let* ((app-key
           (%normalize-app-key app-name))

         (app-name-string
           (%app-name-string app-key))

         (target-version
           (%to-version-string target))

         (applied-set
           (%applied-version-table app-key))

         (registry
           (%app-registry app-key))

         (all-migrations
           (loop
             for index below (length registry)
             collect (aref registry index)))

         (unique-migrations
           (remove-duplicates
            all-migrations
            :key #'migration-version
            :test #'equal
            :from-end t))

         (to-up
           (remove-if
            (lambda (migration)
              (let ((version
                      (migration-version migration)))

                (or
                 (gethash
                  (string-trim " " version)
                  applied-set)

                 (%version<
                  target-version
                  version))))
            unique-migrations))

         (to-down
           (remove-if-not
            (lambda (migration)
              (let ((version
                      (migration-version migration)))

                (and
                 (gethash
                  (string-trim " " version)
                  applied-set)

                 (%version<
                  target-version
                  version))))
            unique-migrations))

         (up-count 0)
         (down-count 0)

         (table-name
           (%migration-table-fqn)))

    ;; Migrations ascendantes.
    (dolist
        (migration
         (sort
          to-up
          (lambda (left right)
            (%version<
             (migration-version left)
             (migration-version right)))))

      (let ((version
              (migration-version migration)))

        (when verbose
          (format
           t
           "~&[migrations:~A] ↑ applying ~A...~%"
           app-key
           version))

        (run-in-transaction
         (lambda ()
           (funcall
            (%ensure-up-fn migration))

           (exec
            (format nil
                    "INSERT INTO ~A (
                         app_name,
                         version
                     )
                     VALUES ($1, $2)
                     ON CONFLICT (
                         app_name,
                         version
                     )
                     DO NOTHING"
                    table-name)
            app-name-string
            version)))

        (setf
         (gethash version applied-set)
         t)

        (incf up-count)

        (when verbose
          (format
           t
           "[migrations:~A] OK ~A~%"
           app-key
           version))))

    ;; Migrations descendantes.
    (dolist
        (migration
         (sort
          to-down
          (lambda (left right)
            (%version>
             (migration-version left)
             (migration-version right)))))

      (let ((version
              (migration-version migration)))

        (when verbose
          (format
           t
           "~&[migrations:~A] ↓ rolling back ~A...~%"
           app-key
           version))

        (run-in-transaction
         (lambda ()
           (funcall
            (%ensure-down-fn migration))

           (exec
            (format nil
                    "DELETE
                       FROM ~A
                      WHERE app_name = $1
                        AND version = $2"
                    table-name)
            app-name-string
            version)))

        (remhash version applied-set)
        (incf down-count)

        (when verbose
          (format
           t
           "[migrations:~A] ROLLBACK OK ~A~%"
           app-key
           version))))

    (values up-count down-count)))


;;;; ============================================================
;;;; API publique
;;;; ============================================================

(defun migrate-all
    (app-name
     &key
       (verbose t)
       (bootstrap-p nil)
       (upgrade-legacy-p nil))

  "Applique toutes les migrations non appliquées de APP-NAME.

Aucun bootstrap ou changement de structure n'est réalisé sans
option explicite."
  (let* ((app-key
           (%normalize-app-key app-name))

         (target
           (%max-registered-version app-key)))

    (ensure-storage!
     app-key
     :bootstrap-p bootstrap-p
     :upgrade-legacy-p upgrade-legacy-p)

    (if target
        (%migrate-up-to-internal
         app-key
         target
         :verbose verbose)

        (progn
          (when verbose
            (format
             t
             "~&[migrations:~A] nothing to migrate.~%"
             app-key))

          (values 0 0)))))


(defun migrate-up-to
    (app-name target
     &key
       (verbose t)
       (bootstrap-p nil)
       (upgrade-legacy-p nil))

  "Migre APP-NAME jusqu'à TARGET."
  (let ((app-key
          (%normalize-app-key app-name)))

    (ensure-storage!
     app-key
     :bootstrap-p bootstrap-p
     :upgrade-legacy-p upgrade-legacy-p)

    (%migrate-up-to-internal
     app-key
     target
     :verbose verbose)))


(defun rollback-one
    (app-name
     &key
       (verbose t)
       (bootstrap-p nil)
       (upgrade-legacy-p nil))

  "Annule la dernière migration appliquée de APP-NAME."
  (let* ((app-key
           (%normalize-app-key app-name))

         (app-name-string
           (%app-name-string app-key))

         (table-name
           (%migration-table-fqn)))

    (ensure-storage!
     app-key
     :bootstrap-p bootstrap-p
     :upgrade-legacy-p upgrade-legacy-p)

    (let* ((row
             (query-1a
              (format nil
                      "SELECT version
                         FROM ~A
                        WHERE app_name = $1
                        ORDER BY applied_at DESC,
                                 version DESC
                        LIMIT 1"
                      table-name)
              app-name-string))

           (version
             (%row-value
              row
              :version
              :*version*)))

      (if (%nullish-p version)
          (progn
            (when verbose
              (format
               t
               "~&[migrations:~A] nothing to rollback.~%"
               app-key))
            0)

          (let ((migration
                  (%find-migration
                   app-key
                   version)))

            (unless migration
              (error
               "No migration registered for version ~A in app ~A"
               version
               app-key))

            (when verbose
              (format
               t
               "~&[migrations:~A] ↓ rolling back ~A...~%"
               app-key
               version))

            (run-in-transaction
             (lambda ()
               (funcall
                (%ensure-down-fn migration))

               (exec
                (format nil
                        "DELETE
                           FROM ~A
                          WHERE app_name = $1
                            AND version = $2"
                        table-name)
                app-name-string
                version)))

            (when verbose
              (format
               t
               "[migrations:~A] ROLLBACK OK ~A~%"
               app-key
               version))

            1)))))


(defun status
    (app-name
     &key
       (bootstrap-p nil)
       (upgrade-legacy-p nil))

  "Retourne l'état des migrations enregistrées pour APP-NAME."
  (let* ((app-key
           (%normalize-app-key app-name)))

    (ensure-storage!
     app-key
     :bootstrap-p bootstrap-p
     :upgrade-legacy-p upgrade-legacy-p)

    (let* ((applied
             (%applied-list app-key))

           (applied-index
             (make-hash-table :test #'equal))

           (registry
             (%app-registry app-key)))

      (dolist (row applied)
        (setf
         (gethash
          (%row-value row :version :*version*)
          applied-index)
         row))

      (loop
        for index below (length registry)
        for migration = (aref registry index)
        for version = (migration-version migration)
        for row = (gethash version applied-index)

        collect
        (list
         (cons :version version)

         (cons
          :applied-p
          (not (null row)))

         (cons
          :applied-at
          (and
           row
           (%row-value
            row
            :applied-at
            :applied_at
            :*applied--at*))))))))


(defun reset!
    (&optional
       (app-name :default)
     &key
       (all-apps-p nil)
       (bootstrap-p nil)
       (upgrade-legacy-p nil))

  "Supprime l'historique des migrations.

Par défaut, seules les migrations de APP-NAME sont supprimées.

ALL-APPS-P doit être explicitement vrai pour vider toute la table."
  (let* ((app-key
           (%normalize-app-key app-name))

         (app-name-string
           (%app-name-string app-key))

         (table-name
           (%migration-table-fqn)))

    (ensure-storage!
     app-key
     :bootstrap-p bootstrap-p
     :upgrade-legacy-p upgrade-legacy-p)

    (run-in-transaction
     (lambda ()
       (if all-apps-p
           (exec
            (format nil
                    "TRUNCATE TABLE ~A"
                    table-name))

           (exec
            (format nil
                    "DELETE
                       FROM ~A
                      WHERE app_name = $1"
                    table-name)
            app-name-string))))

    t))
