(in-package :cl)

(defpackage :lumen.data.repo.core
  (:use :cl)
  (:import-from :lumen.core.http
   :ctx-get :ctx-set! :current-jwt :current-role :current-user-id :respond-404
   :ctx-from-req)
  (:import-from :lumen.data.dao
    :entity-metadata :entity-fields :row->entity :entity->row-alist
    :entity-insert! :entity-update! :entity-delete! :validate-entity!
    :entity-slot-symbol)
  (:import-from :lumen.data.repo.query   ;; helpers SQL existants (select*, count*, etc.)
    :select* :select-page-keyset* :select-page-keyset-prev* :count*)
  (:import-from :lumen.utils
    :alist-get :copy-plist)
  (:import-from :lumen.data.db
   :with-tx :ensure-connection :query-1a)
  (:import-from :lumen.data.tenant :tenant-code-by-id :tenant-id-for-host
   :tenant-id-by-code)
  (:export
    ;; API “haut-niveau” (appelées par les contrôleurs, routes CRUD, jobs…)
    :repo-index :repo-show :repo-create :repo-patch :repo-delete
    ;; Hooks extensibles par entité
    :repo-authorize :repo-normalize :repo-validate :repo-before :repo-persist :repo-after
    ;; Utilitaires
    ))

(in-package :lumen.data.repo.core)

(defun %norm-col-name (x)
  "UPCASE + remplace - par _ pour comparer des noms de colonnes entre
   nos :fields (:CREATED_AT) et les clés d'alist renvoyées (CREATED-AT)."
  (let ((s (etypecase x
             (symbol (symbol-name x))
             (string x))))
    (substitute #\_ #\- (string-upcase s))))

;;; ---------------------------------------------------------------------------
;;; API générique (génériques CLOS)
;;; ---------------------------------------------------------------------------
(defgeneric repo-index  (entity ctx &key filters order select page
				      page-size limit offset after before key))
(defgeneric repo-show   (entity ctx id))
(defgeneric repo-create (entity ctx payload))
(defgeneric repo-patch  (entity ctx id payload))
(defgeneric repo-delete (entity ctx id))

;;; Hooks transverses (spécialisables par entité)
(defgeneric repo-authorize (op entity ctx &key id payload))
(defgeneric repo-normalize (op entity ctx payload))
(defgeneric repo-validate (op entity ctx payload))
(defgeneric repo-before   (op entity ctx &key id payload))
(defgeneric repo-persist  (op entity ctx &key id payload))
(defgeneric repo-after    (op entity ctx result &key id payload))

;;; ---------------------------------------------------------------------------
;;; Defaults : autorisations, normalisation, validation, after (audit)…
;;; ---------------------------------------------------------------------------
(defmethod repo-authorize (op (entity symbol) ctx &key id payload)
  (declare (ignore id payload))
  ;; Par défaut : lecture libre ; écriture nécessite scope "<table>:write"
  (let* ((md (entity-metadata entity))
         (table (string-downcase (or (getf md :table) (symbol-name entity))))
         (write? (member op '(:create :patch :delete))))
    (if write?
        (member (format nil "~a:write" table) (getf ctx :scopes) :test #'string=)
        t)))

(defmethod repo-normalize (op (entity symbol) ctx payload)
  (declare (ignore op entity ctx))
  payload)

;;; ---------------------------------------------------------------------------
;;; Garde-fous framework (CLOS :around)
;;; - S'exécutent AVANT tout (CLOS), mais on délègue aux primaires via
;;;   (call-next-method), puis on RÉ-APPLIQUE les invariants.
;;; - Rend le socle plus robuste même si une app oublie call-next-method.
;;; ---------------------------------------------------------------------------
(defmethod repo-normalize :around ((op (eql :create)) (entity symbol) ctx payload)
  "Pipeline final framework pour CREATE :
   1) Laisse les méthodes primaires spécifiques s’exécuter,
   2) puis garantit les invariants Lumen (idempotents)."
  (let* ((out (call-next-method))                ; ← primaires (app + socle)
         (p0 (%normalize-payload-keys out))      ; clés normalisées (kw/strings)
	 
         (p1 (%remove-protected-fields p0))      ; retire :id, :created_at, etc.
	 
         (tid (%resolve-tenant-id ctx p1)))      ; trouve le tenant depuis ctx/payload
    
    (unless tid
      (error 'lumen.data.errors:db-error
             :message "missing or invalid tenant-id"))
    ;; impose/écrase tenant_id (idempotent) + renvoie un payload propre
    (lumen.utils:alist-set p1 :tenant_id tid :test #'eq)))

(defmethod repo-normalize :around ((op (eql :patch)) (entity symbol) ctx payload)
  "Pipeline final framework pour PATCH :
   - Retire les champs protégés
   - Verrouille le tenant (impossible de changer de tenant)."
  (let* ((out (call-next-method op entity ctx payload))
         (p0 (%normalize-payload-keys out))
         (p1 (%remove-protected-fields p0))
         (tid (%resolve-tenant-id ctx p1)))
    (unless tid
      (error 'lumen.data.errors:db-error
             :message "missing or invalid tenant-id"))
    ;; Ne JAMAIS permettre de modifier :tenant_id par PATCH
    ;;(remf p1 :tenant_id)
    ;;(acons :tenant_id tid p1)
    (lumen.utils:alist-set p1 :tenant_id tid :test #'eq)
    ))

(defmethod repo-validate (op (entity symbol) ctx payload)
  (declare (ignore op))
  ;; Validation DSL si l’app fournit (sinon no-op)
  (ignore-errors
   (let* ((ent (row->entity entity payload)))
      (validate-entity! ent)))
  payload)

(defmethod repo-before (op (entity symbol) ctx &key id payload)
  (declare (ignore op entity ctx id payload))
  ;; no-op par défaut
  (values))

(defmethod repo-after (op (entity symbol) ctx result &key id payload)
  (declare (ignore id payload))
  (let* ((req   (getf ctx :req))
         ;; fallbacks : si :actor-id absent, on essaie via req/JWT
         (actor (or (getf ctx :actor-id)
                    (and req (lumen.core.http:current-user-id req))))
         (tenant (getf ctx :tenant-id))
         (audit? (getf ctx :audit?)))
    ;; debug
    (format t "~&[repo-after] actor=~A tenant=~A op=~A ent=~A~%"
            actor tenant (string-downcase (symbol-name op))
            (string-downcase (symbol-name entity)))
    ;; Audit optionnel
    (when audit?
      (ignore-errors
        (lumen.data.db:ensure-connection
          (lumen.data.db:exec
           "insert into audit_log(actor_id, tenant_id, action, resource, payload)
            values($1,$2,$3,$4,$5)"
           actor
           tenant
           (string-downcase (symbol-name op))
           (string-downcase (symbol-name entity))
           (cl-json:encode-json-to-string result))))))
  result)

;;; ---------------------------------------------------------------------------
;;; Implémentations par défaut (primaire) — s’appuient sur metadata defentity
;;; ---------------------------------------------------------------------------

;; INDEX (liste)
(defun %has-tenant-clause-p (filters tcol tid)
  (let ((clauses (%ensure-filter-list filters)))
    (some (lambda (c)
            (and (consp c)
                 (eq (first c) '=)
                 (eql (second c) tcol)
                 (equal (third c) tid)))
          clauses)))

;;; Utilitaires colonne → keyword
(defun %kw-col (x)
  (cond
    ((null x) nil)
    ((keywordp x) x)
    ((symbolp x)  (intern (string-upcase (symbol-name x)) :keyword))
    ((stringp x)  (intern (string-upcase x) :keyword))
    (t nil)))

(defun entity-timestamp-cols (entity)
  "Retourne la liste des colonnes timestamps (:created / :updated) si définies via :timestamps."
  (let* ((md (entity-metadata entity))
         (ts (getf md :timestamps)))
    (when ts
      (remove nil (list (%kw-col (getf ts :created))
                        (%kw-col (getf ts :updated)))))))

(defun entity-field-cols (entity)
  "Colonnes déclarées dans :fields."
  (mapcar (lambda (f) (getf f :col)) (entity-fields entity)))

(defun %entity-tenant-col (entity &optional md)
  "Retourne la colonne tenant-id *si et seulement si* elle est déclarée ET présente dans :fields."
  (let* ((md     (or md (entity-metadata entity)))
         (fields (getf md :fields))
         (decl   (getf md :tenant-id-col)))
    (labels ((has-col-p (kw)
               (and kw (some (lambda (f) (eq (getf f :col) kw)) fields))))
      (cond
        ((and decl (has-col-p decl)) decl)          ; colonne explicitement déclarée et présente
        ((has-col-p :tenant_id) :tenant_id)         ; fallback si :tenant_id existe réellement
        (t nil)))))                                   ; sinon pas de colonne tenant

(defun %maybe-add-tenant-filter (filters tcol tid)
  "Ajoute (= tcol tid) si tcol/tid présents et que le filtre n’est pas déjà là."
  (let ((flt (%ensure-filter-list filters)))
    (if (and tcol tid (not (%has-tenant-clause-p flt tcol tid)))
        (append flt (list (list '= tcol tid)))
        flt)))

(defun %ensure-tenant-on-payload (entity ctx payload &key (op :create) (forbid-change-p t))
  "Pour CREATE, injecte (tcol . tid) si absent.
   Pour PATCH, bloque (ou ignore) toute tentative de changer le tenant."
  (let* ((md   (entity-metadata entity))
         (tcol (%entity-tenant-col entity md))
         (tid  (getf ctx :tenant-id)))
    (cond
      ;; Pas de colonne tenant sur cette entité → payload inchangé
      ((null tcol) payload)

      ;; CREATE : si tenant présent dans ctx et absent dans payload → on l’injecte
      ((eq op :create)
       (cond
         ((null tid) payload) ; rien à injecter sans tenant courant
         ;; déjà au bon format (keyword)
         ((assoc tcol payload)
          (let ((v (cdr (assoc tcol payload))))
            (when (and v tid (string/= (string-downcase (princ-to-string v))
                                       (string-downcase (princ-to-string tid))))
              (error "forbidden: tenant mismatch on create (~A ≠ ~A)" v tid))
            payload))
         ;; présent sous forme string "tenant_id"
         ((assoc (string-downcase (symbol-name tcol)) payload :test #'string=)
          (let ((v (cdr (assoc (string-downcase (symbol-name tcol)) payload :test #'string=))))
            (when (and v tid (string/= (string-downcase (princ-to-string v))
                                       (string-downcase (princ-to-string tid))))
              (error "forbidden: tenant mismatch on create (~A ≠ ~A)" v tid))
            ;; on normalise en keyword
            (acons tcol (or v tid) (remove (string-downcase (symbol-name tcol)) payload
                                           :key #'car :test #'string=))))
         ;; absent → on injecte
         (t (acons tcol tid payload))))

      ;; PATCH : on empêche (par défaut) le changement de tenant
      ((eq op :patch)
       (let* ((kw-pair (assoc tcol payload))
              (str-key (string-downcase (symbol-name tcol)))
              (str-pair (assoc str-key payload :test #'string=)))
         (when (and forbid-change-p (or kw-pair str-pair))
           (let* ((newv (or (cdr kw-pair) (cdr str-pair)))
                  (curv tid))
             (when (and newv curv (string/= (string-downcase (princ-to-string newv))
                                             (string-downcase (princ-to-string curv))))
               (error "forbidden: cannot change tenant on patch"))))
         ;; on retire toute tentative de changer le tenant pour éviter une update inutile
         (remove tcol
                 (if str-pair
                     (remove str-key payload :key #'car :test #'string=)
                     payload)
                 :key #'car :test #'eq)))

      (t payload))))

(defmethod repo-index ((entity symbol) ctx
                       &key filters order select page page-size limit offset
                            after before key)
  (let* ((md      (entity-metadata entity))
         (table   (getf md :table))
         (allowed (entity-allowed-cols entity :include-pk? t :include-tenant? t))
         (tid     (getf ctx :tenant-id))
         (tcol    (%entity-tenant-col entity md))
         (filters* (%maybe-add-tenant-filter filters tcol tid)))
    (lumen.data.db:ensure-connection
      (cond
        ((or after before)
         (select-page-keyset* table
                              :filters filters*
                              :order   order
                              :key     key
                              :after   after
                              :limit   (or limit 20)
                              :order-whitelist allowed))
        (t
         (select* table
                  :filters filters*
                  :order   order
                  :select  (or select :*)
                  :limit   (or (and page-size (* 1 page-size)) limit 20)
                  :offset  (or offset (and page page-size (* (max 0 (1- page)) page-size)))
                  :order-whitelist allowed))))))

;; SHOW (lecture par PK, filtrée tenant si présent)
(defmethod repo-show ((entity symbol) ctx id)
  (let* ((md   (entity-metadata entity))
         (tbl  (getf md :table))
         (pk   (or (getf md :primary-key) :id))
         (base (list '= pk id))
         (tid  (getf ctx :tenant-id))
         (tcol (%entity-tenant-col entity md))
         (filters (if (and tcol tid)
                      (list 'and base (list '= tcol tid))
                      base)))
    (lumen.data.db:ensure-connection
      (let ((rows (select* tbl :filters filters :limit 1)))
        (and rows (first rows))))))

;; CREATE
(defmethod repo-create ((entity symbol) ctx payload)
  (print "IN REPO CREATE")
  (repo-persist :create entity ctx :payload payload))

(defmethod repo-persist ((op (eql :create)) (entity symbol) ctx &key id payload)
  (declare (ignore id))
  (print "IN REPO PERSIST")
  (let* ((payload* (%ensure-tenant-on-payload entity ctx payload :op :create))
         (ent      (row->entity entity payload*)))
    (validate-entity! ent)
    (lumen.data.db:ensure-connection
      (multiple-value-bind (_n ent2) (entity-insert! ent :returning t)
        (declare (ignore _n))
        (entity->row-alist ent2)))))

;; PATCH (partial update)
(defmethod repo-patch ((entity symbol) ctx id payload)
  (repo-persist :patch entity ctx :id id :payload payload))

(defmethod repo-persist ((op (eql :patch)) (entity symbol) ctx &key id payload)
  (let* ((cur (repo-show entity ctx id)))
    (unless cur (error "not_found"))
    (let* ((payload* (%ensure-tenant-on-payload entity ctx payload :op :patch))
           (ent      (row->entity entity cur)))
      (format t "~&REPO-PERSIT:PATCH:PAYLOAD* : ~A~%" payload*)
      ;; appliquer le patch (hors tenant col)
      (dolist (kv payload*)
        (let* ((col (%norm-col-name (car kv)))
               (val  (cdr kv))
               (slot (entity-slot-symbol entity col)))
          (when slot (setf (slot-value ent slot) val))))
      (validate-entity! ent)
      (format t "~&REPO-PERSIT:PATCH:ENTITY VALIDATION OK~%")
      (lumen.data.db:ensure-connection
        (multiple-value-bind (_n ent2) (entity-update! ent :patch t :returning t)
          (declare (ignore _n))
	  (format t "~&REPO-PERSIT:PATCH:ENTITY UPDATE OK~%")
          (entity->row-alist ent2))))))

;; DELETE
(defmethod repo-delete ((entity symbol) ctx id)
  (repo-persist :delete entity ctx :id id))


(defmethod repo-persist ((op (eql :delete)) (entity symbol) ctx &key id payload)
  (declare (ignore payload))
  (lumen.data.db:with-tx ()
    (let ((cur (repo-show entity ctx id)))
      (unless cur (error "not_found"))
      (entity-delete! (row->entity entity cur))
      `((:deleted . t) (:id . ,id)))))

;;; ---------------------------------------------------------------------------
;;; Transactions & hooks orchestrés via :around
;;; ---------------------------------------------------------------------------
(defmacro %with-read-tx (&body body)
  ;; lecture sans obligation de BEGIN distinct (optionnel)
  `(progn ,@body))

(defmacro %with-write-tx (&body body)
  `(with-tx () ,@body))

;; create
(defmethod repo-create :around (entity ctx payload)
  (%with-write-tx
    (let ((op :create))
      (print "IN REPO CREATE AROUND")
      (repo-authorize op entity ctx :payload payload)
      (let ((p1 (repo-normalize op entity ctx payload)))
	(print p1)
        (repo-validate op entity ctx p1)
        (repo-before   op entity ctx :payload p1)
	(print "REPO OK")
        (let ((res (call-next-method entity ctx p1)))
          (repo-after op entity ctx res :payload p1))))))

;; patch
(defmethod repo-patch :around (entity ctx id payload)
  (%with-write-tx
    (let ((op :patch))
      (repo-authorize op entity ctx :id id :payload payload)
      (let ((p1 (repo-normalize op entity ctx payload)))
        (repo-validate op entity ctx p1)
        (repo-before   op entity ctx :id id :payload p1)
        (let ((res (call-next-method entity ctx id p1)))
          (repo-after op entity ctx res :id id :payload p1))))))

;; delete
(defmethod repo-delete :around (entity ctx id)
  (%with-write-tx
    (let ((op :delete))
      (repo-authorize op entity ctx :id id)
      (repo-before   op entity ctx :id id)
      (let ((res (call-next-method entity ctx id)))
        (repo-after op entity ctx res :id id)))))

(defun %ensure-filter-list (f)
  "Normalise :filters en liste de clauses (AND implicite).
   Accepte NIL, une seule clause, une liste de clauses, ou (:AND ...)."
  (cond
    ((null f) '())
    ;; (:AND ...) ou (and ...) → on prend le reste
    ((and (listp f) (member (first f) '(and :AND))) (rest f))
    ;; déjà une liste de clauses ? on suppose oui
    ((and (listp f) (every #'listp f)) f)
    ;; une seule clause
    (t (list f))))

(defun %tenant-filter-clause (tid)
  (when tid (list '= :tenant_id tid)))

(defmethod repo-index :around (entity ctx &rest args)
  (destructuring-bind (&key filters &allow-other-keys) args
    (let* ((tid        (getf ctx :tenant-id))
           (base       (%ensure-filter-list filters))
           (ten-clause (%tenant-filter-clause tid))
           ;; Ajoute le tenant_id à la fin (AND implicite)
           (filters*   (if ten-clause (append base (list ten-clause)) base))
           ;; Remplace :filters dans les args
           (args*      (alexandria:plist-alist (lumen.utils:copy-plist args))))
      (setf (cdr (assoc :filters args*)) filters*)
      (let ((op :index))
        (repo-authorize op entity ctx)
        (let ((res (apply #'call-next-method entity ctx
			  (alexandria:alist-plist args*))))
	  (repo-after op entity ctx res))))))

(defmethod repo-show :around (entity ctx id)
  (let ((op :show))
    (repo-authorize op entity ctx :id id)
    (let* ((row (call-next-method))      ; SELECT primaire
           (tid (getf ctx :tenant-id))
           (rid (and row (cdr (assoc :tenant_id row))))
	   (res 
      (cond
        ((null row) nil)                                 ; rien trouvé
        ((null tid) nil)                                 ; pas de scope tenant → prudence
        ((null rid) row)                                 ; pas de colonne tenant → renvoie
        ((string-equal (string rid) (string tid)) row)   ; même tenant → OK
        (t nil))))                                      ; cross-tenant → masque la fuite
      (when res
	(repo-after op entity ctx res)))))

;;; ------------------------------------------------------------
;;; Helpers génériques
;;; ------------------------------------------------------------
(defun %kw (x)
  (etypecase x
    (keyword x)
    (symbol  (intern (string-upcase (symbol-name x)) :keyword))
    (string  (intern (string-upcase x) :keyword))))

(defun %normalize-payload-keys (payload)
  "Homogénéise les clés (keywords) et enlève les doublons string/keyword."
  (let ((ht (make-hash-table :test 'equal)))
    (dolist (kv payload)
      (setf (gethash (%kw (car kv)) ht) (cdr kv)))
    (let (out)
      (maphash (lambda (k v) (push (cons k v) out)) ht)
      (nreverse out))))

(defun %remove-protected-fields (payload &key (extra '()))
  (let* ((protected '(:id :created_at :updated_at :lock_version))
         (drop (append protected extra)))
    (remove-if (lambda (kv) (member (car kv) drop))
               payload)))

(defun %maybe-uuid-p (s)
  (and (stringp s)
       (cl-ppcre:scan "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
                      (string-downcase s))))

;;; ------------------------------------------------------------
;;; Sanitation tenant_id
;;;   - ctx :tenant-id prioritaire (UUID attendu)
;;;   - sinon payload :tenant_id (UUID) ou code fonctionnel → résolu via app
;;; ------------------------------------------------------------
(defun %resolve-tenant-id (ctx payload)
  (let* ((ctx-tid (or (getf ctx :tenant-id) (getf ctx :tenant_id)))
         (p-tid   (or (lumen.utils:alist-get payload :tenant_id)
		      (lumen.utils:alist-get payload :tenant-id)))
         (tid
          (cond
            ((and ctx-tid (%maybe-uuid-p ctx-tid)) ctx-tid)
            ((and p-tid (%maybe-uuid-p p-tid))     p-tid)
            (p-tid                                  ;; code fonctionnel → UUID
             (handler-case
                     (funcall (or (symbol-function 'tenant-id-by-code)
                                  (lambda (_code) (declare (ignore _code)) nil))
                              p-tid)
                   (undefined-function () nil)))
            (t nil))))
    tid))
