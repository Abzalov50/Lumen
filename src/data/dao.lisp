(in-package :cl)

(defpackage :lumen.data.dao
  (:use :cl)
  (:import-from :lumen.data.db :query-1a :exec)
  (:export
   ;; Macro & API
   :defentity
   :row->entity :entity->row-alist :entity-insert! :entity-update! :entity-delete!
   :validate-entity!
   ;; Hooks (méthodes spécialisables)
   :before-validate :after-validate
   :before-insert :after-insert
   :before-update :after-update
   :before-delete :after-delete
   ;; Introspection
   :entity-metadata :entity-fields :entity-table :entity-primary-key
   ;; Conditions
   :entity-validation-error :concurrent-update-error
   :entity-slot-symbol
   ))

(in-package :lumen.data.dao)

;;; ----------------------------------------------------------------------------
;;; Registry & metadata
;;; ----------------------------------------------------------------------------
(defvar *entity-registry* (make-hash-table :test 'eq))
;; metadata plist par classe :
;;  :table <string>  :primary-key <keyword>  :fields (<field-spec> ...)
;;  :defaults <alist> (colkw . value)
;;
;; field-spec = plist :
;;   :col <keyword> :slot <symbol> :type <symbol|keyword>
;;   :required? <bool> :validator <fn|symbol|nil> :default <val>
;;   ; props UI pour auto-form
;;   :label <string> :input-type <keyword> :placeholder <string> :help <string>
;;   :choices <list of (value . label)> :min <num> :max <num> :step <num>
;;   :pattern <string> :attrs <alist> :readonly? <bool> :disabled? <bool> :hidden? <bool>
;;   :ui-group <string> :ui-order <number>

(defun entity-metadata (class-symbol)
  (or (gethash class-symbol *entity-registry*)
      (error "Aucune entité enregistrée pour ~S" class-symbol)))

(defun entity-fields (class-symbol)
  (getf (entity-metadata class-symbol) :fields))

(defun entity-table (class-symbol)
  (getf (entity-metadata class-symbol) :table))

(defun entity-primary-key (class-symbol)
  (getf (entity-metadata class-symbol) :primary-key))

;;; ----------------------------------------------------------------------------
;;; Timeouts & slow query log
;;; ----------------------------------------------------------------------------
(defvar *default-statement-timeout-ms* nil
  "Timeout par défaut (ms) appliqué aux requêtes si fourni. NIL = pas de timeout.")

(defvar *slow-query-ms* 500.0
  "Seuil (ms) au-delà duquel on loggue la requête comme lente.")

;;; ----------------------------------------------------------------------------
;;; Utilities
;;; ----------------------------------------------------------------------------
(defun %ident-p (x)
  (and (or (symbolp x) (keywordp x) (stringp x))
       (let* ((s (string-downcase (etypecase x
                                    (string x)
                                    (symbol (symbol-name x))))))
         (and (> (length s) 0)
              (char<= #\a (aref s 0) #\z)
              (every (lambda (ch)
                       (or (char<= #\a ch #\z)
                           (char<= #\0 ch #\9)
                           (char= ch #\_)))
                     s)))))

(defun %ident (x)
  (unless (%ident-p x) (error "Identifiant SQL invalide: ~S" x))
  (string-downcase (etypecase x
                     (string x)
                     (symbol (symbol-name x)))))

(defun %kw (x)
  (if (keywordp x) x
      (intern (string-upcase (%ident x)) :keyword)))

(defun %make-param-emitter ()
  (let ((n 0) (ps '()))
    (values
     (lambda (v) (incf n) (push v ps) (format nil "$~D" n))
     (lambda () (nreverse ps)))))

(defun %slot-value (obj slot)
  (slot-value obj slot))

(defun %set-slot (obj slot value)
  (setf (slot-value obj slot) value))

(defun %field-by-col (fields colkw)
  (find colkw fields :key (lambda (f) (getf f :col)) :test #'eq))

(defun %all-field-cols (fields)
  (mapcar (lambda (f) (getf f :col)) fields))

(defun %timestamps-of (metadata)
  "Retourne plist :created <kw> :updated <kw> :db-fn <string>."
  (let ((ts (getf metadata :timestamps)))
    (when ts
      (list :created (or (getf ts :created) :created_at)
            :updated (or (getf ts :updated) :updated_at)
            :db-fn   (or (getf ts :db-fn) "CURRENT_TIMESTAMP")))))

(defun %comma-join (strings)
  (with-output-to-string (s)
    (loop for (a . rest) on strings do
          (princ a s)
          (when rest (princ ", " s)))))

;;; -------- Normalisation de noms de colonnes (insensible casse & _ ↔ -) ---
(defun %norm-col-name (x)
  "UPCASE + remplace _ par - pour comparer des noms de colonnes entre
   nos :fields (:CREATED_AT) et les clés d'alist renvoyées (CREATED-AT)."
  (let ((s (etypecase x
             (symbol (symbol-name x))
             (string x))))
    (substitute #\- #\_ (string-upcase s))))

(defun %alist-find-ci (alist key)
  "Retourne (values v present-p) si une entrée de ALIST correspond à KEY
   en normalisant (UPCASE + _→-). Gère clés symboles ou strings."
  (let ((target (%norm-col-name key)))
    
    (loop for (k . v) in alist
          for ks = (%norm-col-name k)
          when (string= ks target)
            do (return (values v t))
          finally (return (values nil nil)))))

(defun %coerce-in-safe (value ty)
  "COERCE-IN sûre: si la coercition échoue et retourne NIL,
   on garde la valeur d'origine (qui n'était pas NIL)."
  (if (fboundp 'coerce-in)
      (let ((cv (ignore-errors (funcall 'coerce-in value ty))))
        (if (and (null cv) (not (null value))) value cv))
      value))

;;; ---------------- Dirty tracking ----------------
(defun %make-dirty-set () (make-hash-table :test 'eq))
(defun mark-dirty (entity slot) (setf (gethash slot (slot-value entity '%%dirty)) t))
(defun clear-dirty (entity &optional slots)
  (let ((h (slot-value entity '%%dirty)))
    (cond ((null slots) (clrhash h))
          ((listp slots) (dolist (s slots) (remhash s h)))
          (t (remhash slots h))))
  entity)

#|
(defun dirty-slots (entity)
  (let ((h (slot-value entity '%%dirty)) (acc '()))
    (maphash (lambda (k v) (declare (ignore v)) (push k acc)) h)
    (nreverse acc)))
|#

(defun dirty-slots (obj)
  "Retourne les slots modifiés depuis le dernier snapshot."
  (let* ((orig (slot-value obj +original-slot-name+))
         (cls  (class-name (class-of obj)))
         (slots (%entity-field-slots cls)))
    (labels ((orig-val (s)
               (cdr (assoc s orig :test #'eq))))
      (loop for s in slots
            for now = (slot-value obj s)
            for was = (orig-val s)
            unless (equalp now was)
              collect s))))

;;;--------------- Ergonomie : helpers pour patcher
(defun patch! (entity kvs &key (returning t))
  "Applique (liste de (slot . value)) sur ENTITY, marque dirty et fait un PATCH update."
  (dolist (kv kvs)
    (destructuring-bind (slot . value) kv
      (setf (slot-value entity slot) value))) ; les setters marqueront dirty
  (entity-update! entity :returning returning :patch t))

(defun load-clean! (entity row-alist)
  "Affecte les slots à partir d'une row *sans* marquer dirty (utile après SELECT)."
  (let* ((class (class-name (class-of entity)))
         (md (entity-metadata class))
         (fields (getf md :fields)))
    (dolist (f fields)
      (let* ((slot (getf f :slot))
             (col  (getf f :col))
             (ty   (getf f :type))
             (dbv  (cdr (assoc col row-alist))))
        (%set-slot entity slot (coerce-in dbv ty))))
    (clear-dirty entity)
    entity))

;;; ----------------------------------------------------------------------------
;;; Validations
;;; ----------------------------------------------------------------------------
(define-condition entity-validation-error (error)
  ((entity :initarg :entity :reader entity-validation-entity)
   (errors :initarg :errors :reader entity-validation-errors))
  (:report (lambda (c s)
             (format s "Validation failed: ~{~a~^, ~}"
                     (mapcar #'princ-to-string (entity-validation-errors c))))))

(define-condition concurrent-update-error (error)
  ((entity :initarg :entity :reader concurrent-error-entity)
   (where  :initarg :where  :reader concurrent-error-where))
  (:report (lambda (c s)
             (format s "Concurrent update detected; no rows affected. WHERE=~a"
                     (concurrent-error-where c)))))

(defgeneric before-validate (entity) (:method (e) (declare (ignore e)) nil))
(defgeneric after-validate  (entity) (:method (e) (declare (ignore e)) nil))

(defun %run-validator (fun value entity field-spec)
  (cond
    ((null fun) t)
    ((functionp fun) (funcall fun value entity field-spec))
    ((and (symbolp fun) (fboundp fun))
     (funcall (symbol-function fun) value entity field-spec))
    (t (error "Validator invalide: ~S" fun))))

(defun validate-entity! (entity)
  "Vérifie required? et validator ; lève entity-validation-error si échecs."
  (before-validate entity)
  (let* ((class (class-of entity))
         (cname (class-name class))
         (md (entity-metadata cname))
         (fields (getf md :fields))
         (errors '()))
    ;;(print fields)
    (dolist (f fields)
      (let* ((slot (getf f :slot))
             (col  (getf f :col))
             (label (or (getf f :label) (symbol-name slot)))
             (required? (getf f :required?))
             (validator (getf f :validator))
             (val (%slot-value entity slot)))
        (when (and required? (null val))
          (push (format nil "~a (~a) requis" label col) errors))
        (when (and val validator (not (%run-validator validator val entity f)))
          (push (format nil "Validation échouée pour ~a (~a)" label col) errors))))
    (print "****** VALIDATION ENTITY OK")
    (when errors
      (error 'entity-validation-error :entity entity :errors (nreverse errors))))
  (after-validate entity)
  entity)

;;; ----------------------------------------------------------------------------
;;; Hooks CRUD (par défaut no-op)
;;; ----------------------------------------------------------------------------
(defgeneric before-insert (entity)      (:method (e)   (declare (ignore e)) nil))
(defgeneric after-insert  (entity new)  (:method (e n) (declare (ignore e n)) nil))
(defgeneric before-update (entity)      (:method (e)   (declare (ignore e)) nil))
(defgeneric after-update  (entity new)  (:method (e n) (declare (ignore e n)) nil))
(defgeneric before-delete (entity)      (:method (e)   (declare (ignore e)) nil))
(defgeneric after-delete  (entity)      (:method (e)   (declare (ignore e)) nil))

;;; ----------------------------------------------------------------------------
;;; Mapping row -> entity
;;; ----------------------------------------------------------------------------
;; --- row->entity -----------------------------------------------------

(defgeneric row->entity (class row-alist))

(defmethod row->entity ((class symbol) (row-alist list))
  "Construit une instance de CLASS en populant TOUS les slots définis dans :fields.
- Lookup colonnes insensible à la casse (CREATED_AT ≡ created_at ≡ \"created_at\")
- AUCUN filtrage par :readonly?/:hidden?: on remplit les slots s'ils sont présents dans la row
- Coercition sûre (ne transforme pas une valeur en NIL en cas d'échec)"
  (let* ((md (entity-metadata class))
         (fields (getf md :fields))
         (obj (make-instance class)))
    ;;(print fields)
    (dolist (f fields obj)
      (let* ((slot (getf f :slot))
             (col  (getf f :col))
             (ty   (getf f :type)))
        (multiple-value-bind (v present?) (%alist-find-ci row-alist col)
	  ;; On renseigne le slot si la colonne est présente (même si v = NIL / SQL NULL)
          (when present?
            (setf (slot-value obj slot) (%coerce-in-safe v ty))))))
    (snapshot-entity! obj)
    obj))

(defmethod row->entity ((class standard-class) row-alist)
  (row->entity (class-name class) row-alist))

(defun entity-slot-symbol (class slot-name)
  "Retourne le symbole de slot interné dans le package de la classe.
slot-name peut être un symbole ou une string (nom insensible à la casse)."
  (let* ((pkg (symbol-package class))
         (up  (string-upcase (etypecase slot-name
                               (symbol (symbol-name slot-name))
                               (string slot-name)))))
    (or (find-symbol up pkg)
        (error "Slot ~a introuvable dans package ~a pour classe ~a"
               up (package-name pkg) class))))

;; --- entity -> alist -----------------------------------------------------
(defun %col->alist-key (col)
  "Transforme un nom de colonne (souvent keyword avec underscores) en keyword
avec TIRETS (comme le format Postmodern :alists). Exemple:
  :created_at -> :CREATED-AT
  :lock_version -> :LOCK-VERSION"
  (etypecase col
    (keyword
     (let* ((s (symbol-name col))
            (up (string-upcase s))
            (hy (substitute #\- #\_ up)))
       (intern hy :keyword)))
    (symbol
     (%col->alist-key (intern (string-upcase (symbol-name col)) :keyword)))
    (string
     (%col->alist-key (intern (string-upcase col) :keyword)))))

#|
(defun entity->row-alist (entity &key (include-nulls nil))
  "Convertit une instance CLOS d’entité en alist au format « row » (clés colonnes).
Les clés sont normalisées au style Postmodern (:CREATED-AT, :LOCK-VERSION, ...)."
  (let* ((cls   (class-of entity))
         (cname (class-name cls))
         (md    (entity-metadata cname))
         (fields (getf md :fields)))
    (loop for f in fields
          for col  = (getf f :col)
          for slot = (getf f :slot)
          for key  = (%col->alist-key col)
          for have = (slot-boundp entity slot)
          for val  = (and have (slot-value entity slot))
          for out  = (coerce-in val (getf f :type))
          when (or include-nulls (and have (not (null out))))
            collect (cons key out))))
|#

(defun entity->row-alist (entity &key (include-nulls nil))
  "Convertit une instance CLOS d’entité en alist au format « row » (clés colonnes).
   Les clés sont normalisées au style Postmodern (:CREATED-AT, :LOCK-VERSION, ...).
Identité (val = val).
C'est le sérialiseur JSON final (au niveau HTTP) qui fera la transformation Lisp -> JSON String."
  (let* ((cls    (class-of entity))
         (cname  (class-name cls))
         (md     (entity-metadata cname))
         (fields (getf md :fields)))
    (loop for f in fields
          for col  = (getf f :col)
          for slot = (getf f :slot)
          for key  = (%col->alist-key col)
          for have = (slot-boundp entity slot)
          ;; On récupère la valeur brute stockée dans l'objet
          for val  = (and have (slot-value entity slot))
          
          ;; --- MODIFICATION ICI ---
          ;; Anciennement : for out = (coerce-in val (getf f :type))
          ;; Maintenant : on garde la valeur telle quelle.
          ;; Si val est une liste ("uuid" "uuid"), elle reste une liste.
          for out  = val 
          ;; ------------------------
          
          when (or include-nulls (and have (not (null out))))
            collect (cons key out))))

;;; ----------------------------------------------------------------------------
;;; CRUD
;;; ----------------------------------------------------------------------------
(defgeneric entity-insert! (entity &key returning))
(defgeneric entity-update! (entity &key returning))
(defgeneric entity-delete! (entity))

;;; ------------------------------------------------------------
;;; helpers pour INSERT
;;; ------------------------------------------------------------
(defun %normalize-plist-keys->keywords (plist)
  "Convertit les clés d'un plist en :KEYWORDS (UPCASE), conserve les valeurs."
  (labels ((->kw (k)
             (cond
               ((keywordp k) k)
               ((symbolp  k) (intern (string-upcase (symbol-name k)) :keyword))
               ((stringp  k) (intern (string-upcase k) :keyword))
               (t k))))
    (loop for (k v) on plist by #'cddr
          append (list (->kw k) v))))

(defun %maybe-unquote (x)
  (if (and (consp x) (eq (first x) 'quote)) (second x) x))

(defun %timestamp-spec-of (md)
  "Extrait (:created-col :updated-col :db-fn) depuis md[:timestamps].
Accepte :timestamps donné nu, ou pré-quoté ('(...))."
  (let* ((raw (getf md :timestamps))
         (ts  (%maybe-unquote raw)))
    (when ts (setf ts (%normalize-plist-keys->keywords ts)))
    (values (and ts (getf ts :created))
            (and ts (getf ts :updated))
            (or   (and ts (getf ts :db-fn)) "CURRENT_TIMESTAMP"))))

(defun %force-insert-col-p (col created-col updated-col val)
  "Faut-il forcer l'inclusion de la colonne dans INSERT ?
- Pour les timestamps: si la colonne == created/updated et que VAL est NIL, on force avec la DB-FN.
- Sinon: seulement si VAL non-NIL."
  (cond
    ((and created-col (eql col created-col)) t)
    ((and updated-col (eql col updated-col)) t)
    (t (not (null val)))))

(defun %virtual-col-p (field-spec)
  "Vrai si le champ ne correspond pas à une colonne physique dans la table."
  (let ((type (getf field-spec :type)))
    (or (eq type :computed)
        (eq type :one-to-many)
        (eq type :many-to-one)
        (eq type :many-to-many))))

;;; ------------------------------------------------------------
;;; INSERT corrigé (ne pas écraser les DEFAULTs SQL)
;;; ------------------------------------------------------------
(defmethod entity-insert! ((entity standard-object) &key (returning t))
  (let* ((class (class-name (class-of entity)))
         (md    (entity-metadata class))
         (table (%ident (getf md :table)))
         (fields (getf md :fields))
	 ) ; on n'insère pas lv si slot NIL → DEFAULT
    ;; validations (required/validators)
    (validate-entity! entity)

    ;; timestamps
    (multiple-value-bind (created-col updated-col ts-fn) (%timestamp-spec-of md)
      ;; construire colonnes/valeurs
      (multiple-value-bind (emit params-f) (%make-param-emitter)
        (let ((insert-cols '())
              (values-sql  '()))
          (dolist (f fields)
            (let* ((col  (getf f :col))             ; keyword
                   (ty   (getf f :type))
		   ;; On ignore les colonnes virtuelles
                   (is-virtual (%virtual-col-p f)))
	      (unless is-virtual  ;; <--- ICI
                (let* ((slot (getf f :slot))
                       (val  (%slot-value entity slot))
		       ;; ne PAS insérer : PK/lock-version/etc. si VAL = NIL (laisser DEFAULT SQL)
                       (force? (%force-insert-col-p col created-col updated-col val)))
              (when force?
                (push (%ident col) insert-cols)
                (cond
                  ;; timestamps forcés si NIL → littéral DB-FN
                  ((and (eql col created-col) (null val))
                   (push ts-fn values-sql))
                  ((and (eql col updated-col) (null val))
                   (push ts-fn values-sql))
                  ;; sinon paramétré
                  (t
                   (push (funcall emit (coerce-out val ty)) values-sql))))))))
          (setf insert-cols (nreverse insert-cols)
                values-sql (nreverse values-sql))
          ;; si aucune colonne à insérer → on ne peut pas faire INSERT DEFAULT VALUES si on veut RETURNING *
          ;; Postgres supporte `insert into table default values returning *`
          (let* ((insert-list (if insert-cols
                                  (format nil "(~{~a~^, ~})" insert-cols)
                                  ""))
                 (values-list (if insert-cols
                                  (format nil "(~{~a~^, ~})" values-sql)
                                  "default values"))
                 (returning-sql
                   (cond
                     ((eq returning t) " returning *")
                     ((and (listp returning) (every #'keywordp returning))
                      (format nil " returning ~{~a~^, ~}"
                              (mapcar #'%ident returning)))
                     ((null returning) "")
                     (t " returning *")))
                 (sql (format nil "insert into ~a ~a values~a~a"
                              table insert-list values-list returning-sql)))
            (before-insert entity)
	    ;;(print sql)
            (multiple-value-bind (affected ret)
                (apply #'lumen.data.db:exec sql (funcall params-f))
	      (print "ZZZZZZZZZZZZZ")
	      (print ret)
              ;; si RETURNING, ret est la 1ère ligne (alist) d’après exec
              (let* ((row (or (first ret)
                              (and (not (null returning))
                                   (apply #'lumen.data.db:query-1a sql (funcall params-f)))))
		     (x (print "KKKKKKK"))
                     (new (if row (row->entity class row) entity)))
                (clear-dirty new)
		(snapshot-entity! new)
                (after-insert entity new)
                (values affected new)))))))))

#|
(defmethod entity-update! ((entity standard-object) &key (returning t) (patch nil))
  (let* ((class (class-name (class-of entity)))
         (md (entity-metadata class))
         (table (%ident (getf md :table)))
         (pk (or (getf md :primary-key) :id))
         (fields (getf md :fields))
         (lv-col (getf md :lock-version)) ; keyword ou NIL
         (ts (%timestamps-of md))
         (ts-updated (and ts (getf ts :updated)))
         (ts-fn (or (and ts (getf ts :db-fn)) "CURRENT_TIMESTAMP")))
    ;; Validations avant update (toujours)
    (validate-entity! entity)
    (let ((assigns '()) where)
      (multiple-value-bind (emit params-f) (%make-param-emitter)
        ;; Déterminer l’ensemble des slots “à considérer”
        (let* ((candidate-slots
                 (if patch (dirty-slots entity)
                     (mapcar (lambda (f) (getf f :slot)) fields)))
               ;; Récupérer slot/ty de PK & lock-version s'ils existent
               (pk-slot (getf (or (%field-by-col fields pk)
                                  (error "PK ~a introuvable pour ~a" pk class))
                              :slot))
               (pk-ty   (getf (or (%field-by-col fields pk)) :type))
               (lv-slot (and lv-col (getf (or (%field-by-col fields lv-col)) :slot)))
               (lv-ty   (and lv-col (getf (or (%field-by-col fields lv-col)) :type)))
               (lv-val  (and lv-slot (%slot-value entity lv-slot))))
          ;; WHERE: PK obligatoire
          (let ((pk-val (%slot-value entity pk-slot)))
            (unless pk-val (error "Primary key manquante (~a) pour UPDATE" pk))
            (setf where (format nil "~a = ~a" (%ident pk)
                                (funcall emit (coerce-out pk-val pk-ty)))))
          ;; WHERE: lock-version si configuré
          (when lv-col
            (unless lv-val
              (error "Optimistic locking activé mais slot ~a est NIL" lv-col))
            (setf where (format nil "~a AND ~a = ~a"
                                where
                                (%ident lv-col)
                                (funcall emit (coerce-out lv-val lv-ty)))))
          ;; SET: construire la liste d'assignations
          (dolist (f fields)
            (let* ((col (getf f :col))
                   (slot (getf f :slot))
                   (ty   (getf f :type))
                   (val  (%slot-value entity slot))
                   (is-pk (eq col pk))
                   (is-lv (and lv-col (eq col lv-col)))
                   (is-up (and ts-updated (eq col ts-updated)))
                   (selected (member slot candidate-slots :test #'eq))
		   ;; MODIF: On ignore les colonnes virtuelles
                   (is-virtual (%virtual-col-p f)))
	      (unless is-virtual
              (cond
                ;; Ne pas SET la PK
                (is-pk nil)
                ;; lock-version: on force "col = col + 1" (littéral SQL), jamais paramétré
                (is-lv (push (format nil "~a = ~a + 1" (%ident col) (%ident col)) assigns))
                ;; PATCH: ignorer les slots non dirty
                ((and patch (not selected)) nil)
                ;; updated_at: si NIL → forcer ts-fn, sinon paramétrer
                (is-up
                 (if (null val)
                     (push (format nil "~a = ~a" (%ident col) ts-fn) assigns)
                     (push (format nil "~a = ~a" (%ident col)
                                   (funcall emit (coerce-out val ty)))
			   assigns)))
                ;; slot normal
                (t
                 (push (format nil "~a = ~a" (%ident col)
                               (funcall emit (coerce-out val ty)))
		       assigns))))))
          ;; En mode patch, si aucun assign sélectionné, on peut quand même toucher updated_at
          (when (and patch ts-updated (null assigns))
            (push (format nil "~a = ~a" (%ident ts-updated) ts-fn) assigns))
          ;; S'assurer qu'il y a au moins un SET (sinon no-op)
	  (format t "~&ENTITY-UPDATE!:ASSIGNS: ~A~%" assigns)
          (let* ((assigns* (if assigns (nreverse assigns) '("/* no-op */")))
                 (returning-sql
		   (cond
                     ;;((eq returning t) " returning *")
		     ;; Cas 1 : On veut tout retourner -> On liste explicitement les colonnes connues
                     ((eq returning t) 
                      (format nil " RETURNING ~{~a~^, ~}" 
                              (mapcar #'%ident (mapcar (lambda (f) (getf f :col)) fields))))
		     ;; Cas 2 : Liste explicite demandée par l'appelant
                     ((and (listp returning) (every #'keywordp returning))
                      (format nil " returning ~{~a~^, ~}" (mapcar #'%ident returning)))
		     ;; Cas 3 : Rien
                     ((null returning) "")
		     ;; Fallback
                     (t " RETURNING *")))
                 (sql (format nil "update ~a set ~a where ~a~a"
                              table (%comma-join assigns*) where returning-sql)))
            (before-update entity)
	    (format t "~&ENTITY-UPDATE!: BEFORE UPDATE OK~%")
            (multiple-value-bind (affected ret)
                (apply #'lumen.data.db:exec sql (funcall params-f))

              ;; Verrou optimiste: si 0 lignes affectées → conflit
              (when (and lv-col (= (or affected 0) 0))
                (error 'concurrent-update-error :entity entity :where where))
              ;; Si on a RETURNING, mapper la ligne ; sinon renvoyer l'entité modifiée
              (let ((row (or (first ret) (and (not (null returning))
                                      (apply #'query-1a sql (funcall params-f))))))
                (let ((new (if row (row->entity class row) entity)))
                  (clear-dirty new)
		  (snapshot-entity! new)
                  (after-update entity new)
                  (values affected new))))))))))
|#

(defmethod entity-update! ((entity standard-object) &key (returning t) (force-all nil))
  (let* ((class (class-name (class-of entity)))
         (md    (entity-metadata class))
         (table (%ident (getf md :table)))
         (pk    (or (getf md :primary-key) :id))
         (fields (getf md :fields))
         (lv-col (getf md :lock-version)) 
         (ts    (%timestamps-of md))
         (ts-updated (and ts (getf ts :updated)))
         (ts-fn (or (and ts (getf ts :db-fn)) "CURRENT_TIMESTAMP")))

    (print "IN ENTITY-UPDATE!")
    (print md)
    (print table)
    (print ts)
    (print ts-updated)
    
    (validate-entity! entity)
    
    (let ((assigns '()) 
          where
          ;; CHANGEMENT MAJEUR : 
          ;; Si force-all = T, on prend tout.
          ;; Sinon (défaut), on ne prend QUE les champs modifiés (dirty).
          (candidate-slots (if force-all 
                               (mapcar (lambda (f) (getf f :slot)) fields)
                               (dirty-slots entity))))
      
      (multiple-value-bind (emit params-f) (%make-param-emitter)
        ;; 1. Construction du WHERE (PK + Lock Version)
        (let ((pk-slot (getf (or (%field-by-col fields pk)
                                 (error "PK introuvable")) :slot))
              (pk-ty   (getf (or (%field-by-col fields pk)) :type))
              (lv-slot (and lv-col (getf (or (%field-by-col fields lv-col)) :slot)))
              (lv-ty   (and lv-col (getf (or (%field-by-col fields lv-col)) :type))))
          
          (let ((pk-val (%slot-value entity pk-slot))
                (lv-val (and lv-slot (%slot-value entity lv-slot))))
            
            (unless pk-val (error "PK manquante pour UPDATE"))
            (setf where (format nil "~a = ~a" (%ident pk) (funcall emit (coerce-out pk-val pk-ty))))
            
            (when lv-col
              (unless lv-val (error "Optimistic locking: slot ~a est NIL" lv-col))
              (setf where (format nil "~a AND ~a = ~a" where (%ident lv-col) (funcall emit (coerce-out lv-val lv-ty)))))))

        ;; 2. Construction du SET
        (dolist (f fields)
          (let* ((col  (getf f :col))
                 (slot (getf f :slot))
                 (ty   (getf f :type))
                 (val  (%slot-value entity slot))
                 (is-pk (eq col pk))
                 (is-lv (and lv-col (eq col lv-col)))
                 (is-up (and ts-updated (eq col ts-updated)))
                 ;; Est-ce qu'on doit mettre à jour ce champ ?
                 (selected (member slot candidate-slots :test #'eq))
                 (is-virtual (%virtual-col-p f)))
            
            (unless is-virtual
              (cond
                ((or is-pk) nil) ;; On ne touche jamais la PK
                
                ;; Lock Version : Incrément forcé (SQL)
                (is-lv 
                 (push (format nil "~a = ~a + 1" (%ident col) (%ident col)) assigns))
                
                ;; Updated At : Forcé
                (is-up
                 (if (null val)
                     (push (format nil "~a = ~a" (%ident col) ts-fn) assigns)
                     (push (format nil "~a = ~a" (%ident col) (funcall emit (coerce-out val ty))) assigns)))
                
                ;; Champs standards : Uniquement si sélectionnés (Dirty ou Force-All)
                (selected
                 (push (format nil "~a = ~a" (%ident col) (funcall emit (coerce-out val ty))) assigns))))))
        
        ;; Cas spécial : Si rien n'est dirty, mais qu'on a un updated_at, on force le "touch"
        (when (and (null assigns) ts-updated)
             (push (format nil "~a = ~a" (%ident ts-updated) ts-fn) assigns))

        ;; Si toujours rien à faire (pas de dirty, pas de timestamp, pas de force-all), on sort tout de suite
        (unless assigns
          (return-from entity-update! (values 0 entity)))
        
        ;; 3. Exécution SQL
        (let* ((assigns* (nreverse assigns))
               (returning-sql
                (cond
                  ((eq returning t) 
                   (format nil " RETURNING ~{~a~^, ~}" 
                           (mapcar #'%ident (mapcar (lambda (f) (getf f :col)) fields))))
                  ((and (listp returning) returning)
                   (format nil " RETURNING ~{~a~^, ~}" (mapcar #'%ident returning)))
                  (t "")))
               (sql (format nil "update ~a set ~a where ~a~a"
                            table (%comma-join assigns*) where returning-sql)))
          
          (before-update entity)
          
          (multiple-value-bind (affected ret)
              (apply #'lumen.data.db:exec sql (funcall params-f))
            
            ;; Check Optimistic Locking
            (when (and lv-col (= (or affected 0) 0))
              (error 'concurrent-update-error :entity entity :where where))
            
            ;; Hydratation du retour
            (let ((row (or (first ret) 
                           (and returning (apply #'lumen.data.db:query-1a sql (funcall params-f))))))
              (let ((new (if row (row->entity class row) entity)))
                (clear-dirty new)
                (snapshot-entity! new)
                (after-update entity new)
                (values affected new)))))))))

(defmethod entity-delete! ((entity standard-object))
  (let* ((class (class-name (class-of entity)))
         (md (entity-metadata class))
         (table (%ident (getf md :table)))
         (pk (or (getf md :primary-key) :id))
         (fields (getf md :fields))
         (pk-slot (getf (or (%field-by-col fields pk)
                            (error "PK ~a introuvable dans ~a" pk class))
                        :slot))
         (pk-val (%slot-value entity pk-slot)))
    (unless pk-val (error "Impossible de supprimer: PK ~a est NIL" pk))
    (before-delete entity)
    (multiple-value-bind (emit params-f) (%make-param-emitter)
      (let* ((where (format nil "~a = ~a" (%ident pk) (funcall emit pk-val)))
             (sql (format nil "delete from ~a where ~a" table where)))
        (multiple-value-bind (n _ret)
            (apply #'lumen.data.db:exec sql (funcall params-f))
          (declare (ignore _ret))
          (after-delete entity)
          n)))))

;;; ----------------------------------------------------------------------------
;;; Macro defentity
;;; ----------------------------------------------------------------------------
;; slot %%dirty pour suivre les modifications
(defparameter +dirty-slot-name+ '%%dirty)
(defparameter +original-slot-name+ '%%original)

(defun %entity-field-specs (class)
  "Retourne la liste des plists field pour CLASS depuis le registry."
  (let ((md (entity-metadata class)))
    (getf md :fields)))

(defun %entity-field-slots (class)
  "Liste des symboles de slots (pas les colonnes) à partir des fields."
  (mapcar (lambda (f) (getf f :slot))
          (%entity-field-specs class)))

(defun %snapshot-entity (obj)
  "Construit un alist (slot . value) pour l'état courant de OBJ."
  (let* ((cls (class-name (class-of obj)))
         (slots (%entity-field-slots cls)))
    (loop for s in slots
          collect (cons s (slot-value obj s)))))

(defun snapshot-entity! (obj)
  "Enregistre le snapshot courant dans %%ORIGINAL."
  (setf (slot-value obj +original-slot-name+)
        (%snapshot-entity obj))
  obj)

(defmacro defentity (name &key table fields primary-key defaults
                            timestamps lock-version)
  "Déclare une entité CLOS + enregistre metadata pour CRUD/validation/UI.
FIELDS est une liste de formes:
  (:col <kw> :type <sym|kw> [:required? t] [:validator fn]
         [:default v] [:label \"...\"] [:input-type :text|...]
         [:placeholder \"...\"] [:help \"...\"] [:choices ((v . l) ...)]
         [:min n] [:max n] [:step n] [:pattern \"^\"] [:attrs ((k . v) ...)]
         [:readonly? t] [:disabled? t] [:hidden? t]
         [:ui-group \"...\"] [:ui-order n]
         [:ui ((k . v) ...)])" ;; <— ajouté pour documenter

  (let* ((cls name)
         (pkg  (or (symbol-package name) *package*))
         (tbl  (if table
                   (string-downcase (etypecase table
                                      (string table)
                                      (symbol (symbol-name table))))
                   (string-downcase (symbol-name name))))
         (pk   (or primary-key :id))
         (norm-fields
           (mapcar
            (lambda (f)
              (destructuring-bind
                  (&key col type required? validator default
                     label input-type placeholder help choices
                     min max step pattern attrs
                     readonly? disabled? hidden?
                     ui-group ui-order slot
                     ui	references ref-col ref-format
                   &allow-other-keys)
                  f
                (let* ((colkw (%kw (or col (error "Champ sans :col dans ~a" name))))
                       (slot-name (or slot
                                      (intern (string-upcase (symbol-name colkw)) pkg))))
                  (list :col colkw :slot slot-name :type type
                        :required? required? :validator validator :default default
                        :label label :input-type input-type :placeholder placeholder
                        :help help :choices choices :min min :max max :step step
                        :pattern pattern :attrs attrs :readonly? readonly?
                        :disabled? disabled? :hidden? hidden?
                        :ui-group ui-group :ui-order ui-order :ui ui
			:references references :ref-col ref-col :ref-format ref-format))))
            fields))
         ;; Spécification des slots CLOS
         (slots
           (append
            ;; Slot interne pour dirty-tracking (lecteur dans PKG)
            (list
             `(,+dirty-slot-name+
               :initform (%make-dirty-set)
               :reader ,(intern (format nil "~a-%%DIRTY"
                                        (string-downcase (symbol-name name)))
                                pkg))
	     `(,+original-slot-name+
	       :initform nil
	       :accessor ,(intern (format nil "~a-%%ORIGINAL"
					  (string-downcase (symbol-name name)))
				  pkg)))
            ;; Slots utilisateurs
            (mapcar
             (lambda (f)
               (let* ((s       (getf f :slot)) ; déjà interné dans PKG
                      ;; initarg = KEYWORD correspondant au slot (ex. :ID, :EMAIL)
                      (initarg (intern (symbol-name s) :keyword))
                      ;; accessor: nom lisible en minuscules avec préfixe <class>-, dans PKG
                      (acc    (intern (concatenate 'string
                                                   (string-downcase (symbol-name cls))
                                                   "-"
                                                   (string-downcase (symbol-name s)))
                                      pkg)))
                 `(,s :initarg ,initarg
                      :accessor ,acc
                      :initform nil)))
             norm-fields)))
         ;; couples (slot . accessor) pour générer les :after (dirty)
         (accessor-specs
           (mapcar (lambda (f)
                     (let* ((slot (getf f :slot))
                            (acc  (intern (concatenate 'string
                                                       (string-downcase (symbol-name name))
                                                       "-"
                                                       (string-downcase (symbol-name slot)))
                                          pkg)))
                       (list slot acc)))
                   norm-fields))
         ;; Defaults: alist (:col . default)
         (defaults-alist
           (loop for f in norm-fields
                 for col = (getf f :col)
                 for def = (getf f :default)
                 when def collect (cons col def))))
    `(progn
       ;; defclass, registry, after-methods, etc. identiques
       (defclass ,cls () ,(append
                           (list
                            `(,+dirty-slot-name+
                              :initform (%make-dirty-set)
                              :reader ,(intern (format nil "~a-%%DIRTY"
                                                       (string-downcase (symbol-name name)))
                                               pkg))
                            `(,+original-slot-name+
                              :initform nil
                              :accessor ,(intern (format nil "~a-%%ORIGINAL"
                                                         (string-downcase (symbol-name name)))
                                                 pkg)))
                           (mapcar
                            (lambda (f)
                              (let* ((s (getf f :slot))
                                     (initarg (intern (symbol-name s) :keyword))
                                     (acc (intern (concatenate 'string
                                                               (string-downcase (symbol-name cls))
                                                               "-"
                                                               (string-downcase (symbol-name s)))
                                                  pkg)))
                                `(,s :initarg ,initarg :accessor ,acc :initform nil)))
                            norm-fields)))

       (setf (gethash ',cls *entity-registry*)
             (list :table ,tbl
                   :primary-key ,(%kw pk)
                   :fields ',norm-fields
                   :defaults ',(or defaults
                                   (loop for f in norm-fields
                                         for def = (getf f :default)
                                         for col = (getf f :col)
                                         when def collect (cons col def)))
                   :timestamps ,timestamps
                   :lock-version ',(when lock-version (%kw lock-version))))

       ,@(mapcar
          (lambda (f)
            (let* ((slot (getf f :slot))
                   (acc  (intern (concatenate 'string
                                              (string-downcase (symbol-name name))
                                              "-"
                                              (string-downcase (symbol-name slot)))
                                 pkg)))
              `(defmethod (setf ,acc) :after (new (obj ,cls))
                 (declare (ignore new))
                 (mark-dirty obj ',slot))))
          norm-fields)

       ',cls)))

;;; ----------------------------------------------------------------------------
;;; Type codecs registry (encode Lisp -> SQL param, decode DB -> Lisp)
;;; ----------------------------------------------------------------------------
(defvar *type-codecs* (make-hash-table :test 'equal))
(defun def-type-codec (type-key &key encode decode)
  "TYPE-KEY: keyword (ex. :jsonb) ou liste (ex. '(:array :uuid)).
ENCODE/DECODE sont des fonctions 1-arg (val) -> val."
  (setf (gethash type-key *type-codecs*)
        (list :encode (or encode #'identity)
              :decode (or decode #'identity))))

(defun find-type-codec (type-key)
  (or (gethash type-key *type-codecs*)
      ;; fallback: identity
      (list :encode #'identity :decode #'identity)))

;; Helpers pour types composés
(defun array-type-p (ty)
  (and (consp ty) (eql (first ty) :array) (second ty)))
(defun array-elem-type (ty) (second ty))

;;;; ------------------------------------------------------------------
;;;; SQL NULL sentinel + helpers
;;;; ------------------------------------------------------------------

(defparameter *sql-null-sentinel*
  (or
   (ignore-errors (symbol-value (find-symbol "*NULL*" "CL-POSTGRES"))) ; Postgres direct
   (ignore-errors (symbol-value (find-symbol "*NULL*" "POSTMODERN")))  ; Postmodern
   :null)                                                              ; fallback
  "Valeur à passer/recevoir pour NULL côté driver. Adapte si besoin.")

(defun sql-null-p (x)
  (or (null x) (eq x *sql-null-sentinel*)))

(defun ->sql-null (_)
  (declare (ignore _))
  *sql-null-sentinel*)

;;;; ------------------------------------------------------------------
;;;; Codecs: encode (Lisp -> SQL) / decode (SQL -> Lisp)
;;;; ------------------------------------------------------------------
#|
(defun coerce-out (val type)
  "Lisp -> SQL param (encode: pour insert/update). Force NIL vers le sentinelle NULL.
Transforme les structures Lisp en format compatible Driver DB (String, Vector, etc.).
Appelle :encode du codec"
  (cond
    ;; 1) Type inconnu ou non défini → juste convertir NIL en NULL
    ((null type)
     (if (sql-null-p val) (->sql-null val) val))

    ;; 2) TABLEAUX GÉNÉRIQUES (:array :type)
    ((array-type-p type)
     (let* ((elem (array-elem-type type))
            (elem-enc (getf (find-type-codec elem) :encode)))
       (cond
         ((sql-null-p val) (->sql-null val))
         ;; IMPORTANT : Les drivers SQL attendent souvent un VECTEUR pour les arrays.
         ;; On force la conversion (map 'vector ...) même si c'est une liste en entrée.
         ((or (vectorp val) (listp val))
          (map 'vector (lambda (x) 
                         (if (sql-null-p x) (->sql-null x) (funcall elem-enc x))) 
               val))
         ;; Fallback
         (t val))))

    ;; 3) SCALAIRES & TYPES SPÉCIFIQUES (ex: :string-list)
    (t
     (if (sql-null-p val)
         (->sql-null val)
         (let ((enc (getf (find-type-codec type) :encode)))
           (funcall enc val))))))
|#

(defun coerce-out (val type)
  "Lisp -> SQL param (encode: pour insert/update). Force NIL vers le sentinelle NULL.
Transforme les structures Lisp en format compatible Driver DB (String, Vector, etc.).
Appelle :encode du codec"
  (cond
    ;; 1) Type inconnu
    ((null type)
     (if (sql-null-p val) (->sql-null val) val))

    ;; 2) TABLEAUX GÉNÉRIQUES (:array :type) ou TYPES LISTE (:string-list)
    ((or (array-type-p type) (eq type :string-list))
     (let* ((is-string-list (eq type :string-list))
            (elem (if is-string-list :string (array-elem-type type)))
            (elem-enc (getf (find-type-codec elem) :encode)))
       
       (cond
         ;; CAS SPÉCIAL : NIL signifie souvent "Liste vide" en Lisp, pas "NULL SQL" pour un tableau.
         ;; Si vous voulez vraiment NULL, il faudrait un marqueur spécifique, mais par défaut
         ;; une liste vide Lisp doit devenir un tableau vide SQL '{}'.
         ((null val) 
          (if is-string-list #() (vector))) ;; Retourne un vecteur vide
         
         ((sql-null-p val) (->sql-null val))
         
         ((or (vectorp val) (listp val))
          (map 'vector (lambda (x) 
                         (if (sql-null-p x) (->sql-null x) (funcall elem-enc x))) 
               val))
         (t val))))

    ;; 3) SCALAIRES
    (t
     (if (sql-null-p val)
         (->sql-null val)
         (let ((enc (getf (find-type-codec type) :encode)))
           (funcall enc val))))))

(defun coerce-in (val type)
  "DB -> Lisp (decode: pour row->entity). Convertit NULL driver → NIL.
Transforme les retours Driver DB (String, Vector) en structures Lisp.
Appelle :decode du codec."
  (cond
    ;; 1) Type inconnu
    ((null type)
     (if (sql-null-p val) nil val))

    ;; 2) TABLEAUX GÉNÉRIQUES (:array :type)
    ((array-type-p type)
     (let* ((elem (array-elem-type type))
            (elem-dec (getf (find-type-codec elem) :decode)))
       (cond
         ((sql-null-p val) nil)
         ;; On retourne un vecteur pour rester cohérent avec le driver,
         ;; sauf si un type spécifique demande une liste (voir def-type-codec plus bas)
         ((vectorp val) (map 'vector (lambda (x) (if (sql-null-p x) nil (funcall elem-dec x))) val))
         ((listp val)   (mapcar      (lambda (x) (if (sql-null-p x) nil (funcall elem-dec x))) val))
         (t val))))

    ;; 3) SCALAIRES
    (t
     (if (sql-null-p val)
         nil
         (let ((dec (getf (find-type-codec type) :decode)))
           (funcall dec val))))))

;;; ----------------------------------------------------------------------------
;;; Enregistrements des types
;;; ----------------------------------------------------------------------------
;; 1. Types de base (String, Integer, Boolean, Text)
(def-type-codec :string)
(def-type-codec :text)
(def-type-codec :integer)
(def-type-codec :boolean)

;; 2. UUID → laissé en string par défaut (on pourra brancher un parseur UUID plus tard)
(def-type-codec :uuid)

;; 3. NOUVEAU : :string-list
;; Ce type gère une liste de chaines (Lisp List) <-> SQL Array (Vector)
;; C'est ici qu'on gère la conversion spécifique.
(def-type-codec :string-list
  ;; Encode: Liste Lisp -> Vecteur (pour le driver SQL)
  :encode (lambda (v)
            (if (listp v)
                (coerce v 'vector)
                v))
  ;; Decode: Vecteur SQL -> Liste Lisp (car le type s'appelle string-LIST)
  :decode (lambda (v)
            (if (vectorp v)
                (coerce v 'list)
                v)))

;; :jsonb → hooks configurables ; par défaut identity
(defvar *json-encode* #'identity)
(defvar *json-decode* #'identity)
(def-type-codec :jsonb :encode *json-encode* :decode *json-decode*)

;; :timestamptz → identity par défaut (brancher local-time si dispo)
(defvar *ts-encode* #'identity)
(defvar *ts-decode* #'identity)
(def-type-codec :timestamptz :encode *ts-encode* :decode *ts-decode*)

;; Arrays: utiliser la forme de type '(:array :text) ou '(:array :uuid) etc.
;; Rien à enregistrer: coerce-(in|out) gère les types composés via array-type-p.

;;;----------------------------------------------------------
;;; Exemple de branchement “optionnel” avec local-time et yason
;;;-----------------------------------------------------------
;; JSON via CL-JSON (à adapter selon la lib)
#+cl-json
(progn
  (setf *json-encode* (lambda (x) (cl-json:encode-json-to-string x))
        *json-decode* (lambda (s) (cl-json:decode-json-from-string s)))
  (def-type-codec :jsonb :encode *json-encode* :decode *json-decode*))

;; UUID via com.github.sharplispers.uuid
#+uuid
(progn
  (def-type-codec :uuid
    :encode (lambda (x) (etypecase x
                          (string x)
                          (uuid:uuid (uuid:print-bytes nil x))))
    :decode (lambda (x) (etypecase x
                          (string x)
                          (uuid:uuid x))))) ; si le driver renvoie déjà un objet

;; local-time pour timestamptz
#+local-time
(progn
  (setf *ts-encode*
        (lambda (lt) (local-time:format-timestring nil lt :format local-time:+iso-8601-format+))
        *ts-decode*
        (lambda (s) (etypecase s
                      (string (local-time:parse-timestring s))
                      (t s))))
  (def-type-codec :timestamptz :encode *ts-encode* :decode *ts-decode*))
