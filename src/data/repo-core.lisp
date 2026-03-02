(in-package :cl)

(defpackage :lumen.data.repo.core
  (:use :cl)
  (:import-from :lumen.core.http
   :ctx-get :ctx-set! :current-jwt :current-role :current-user-id :respond-404 :ctx-from-req)
  (:import-from :lumen.data.dao
   :entity-metadata :entity-fields :row->entity :entity->row-alist :entity-table
		:entity-primary-key :entity-insert! :entity-update! :entity-delete!
		:entity-slot-symbol :validate-entity-payload)
  (:import-from :lumen.data.repo.query
    :select* :select-page-keyset* :select-page-keyset-prev* :count*)
  (:import-from :lumen.utils :alist-get :copy-plist :alist-set :to-snake-case :col-get :secure-uuid-equal)
  (:import-from :lumen.data.db :with-tx :ensure-connection :exec :run-in-transaction)
  (:import-from :lumen.data.tenant :tenant-code-by-id :tenant-id-for-host :tenant-id-by-code)
  (:import-from :lumen.core.error 
   :application-error :application-error-message :application-error-code)
  (:export
    :repo-index :repo-show :repo-create :repo-patch :repo-delete :repo-after
   :repo-authorize :repo-normalize :repo-validate :repo-before :repo-persist    
   ))

(in-package :lumen.data.repo.core)

(defun %norm-col-name (x)
  (let ((s (etypecase x (symbol (symbol-name x)) (string x))))
    (substitute #\_ #\- (string-upcase s))))

(defun %has-tenant-clause-p (filters tcol tid)
  (let ((clauses (%ensure-filter-list filters)))
    (some (lambda (c)
            (and (consp c)
                 (eq (first c) '=)
                 (eql (second c) tcol)
                 (equal (third c) tid)))
          clauses)))

(defun %maybe-add-tenant-filter (filters tcol tid)
  "Ajoute (= tcol tid) si tcol/tid présents et que le filtre n’est pas déjà là."
  (let ((flt (%ensure-filter-list filters)))
    (if (and tcol tid (not (%has-tenant-clause-p flt tcol tid)))
        (append flt (list (list '= tcol tid)))
        flt)))

(defun %kw (x)
  (etypecase x
    (keyword x)
    (symbol  (intern (string-upcase (symbol-name x)) :keyword))
    (string  (intern (string-upcase x) :keyword))))

(defun %kw-col (x)
  (cond
    ((null x) nil)
    ((keywordp x) x)
    ((symbolp x)  (intern (string-upcase (symbol-name x)) :keyword))
    ((stringp x)  (intern (string-upcase x) :keyword))
    (t nil)))

(defun %normalize-payload-keys (payload)
  (let ((ht (make-hash-table :test 'equal)))
    (dolist (kv payload) (setf (gethash (%kw (car kv)) ht) (cdr kv)))
    (let (out) (maphash (lambda (k v) (push (cons k v) out)) ht) (nreverse out))))

(defun %remove-protected-fields (payload &key (extra '()))
  (let* ((protected '(:id :created_at :updated_at :lock_version))
         (drop (append protected extra)))
    (remove-if (lambda (kv) (member (car kv) drop))
               payload)))

(defun %maybe-uuid-p (s)
  (and (stringp s)
       (cl-ppcre:scan "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
                      (string-downcase s))))

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

(defun %make-label-key (col-key)
  "Transforme :priority en :priority-label"
  (intern (format nil "~A-LABEL" (lumen.utils:to-kebab-case (symbol-name col-key))) :keyword))

(defun %resolve-choice-label (val choices)
  "Trouve le libellé dans une alist de choix ((1 . 'Label') ...)"
  (let ((match (assoc val choices :test (lambda (v c) 
                                          ;; Comparaison robuste (string vs int vs symbol)
                                          (string-equal (princ-to-string v) 
                                                        (princ-to-string c))))))
    (if match (cdr match) val)))

(defun %resolve-reference-label (val ref-table ref-col tid)
  "Exécute une requête SQL légère pour récupérer le label de la FK."
  (when val
    ;; Attention : Ceci génère une requête par champ (N+1).
    ;; Pour des listes massives, il faudrait idéalement faire des JOINS, 
    ;; mais pour un usage standard, c'est acceptable et très flexible.
    (let ((sql (format nil "SELECT ~A FROM ~A WHERE id = $1 AND tenant_id = $2 LIMIT 1" 
                       (lumen.utils:to-snake-case (string ref-col))
                       ref-table)))
      (lumen.data.db:ensure-connection
        (pomo:query sql val tid :single)))))

(defun enrich-row-metadata (entity-sym row tid)
  "Prend une alist (row) et ajoute les champs -LABEL basés sur la déf de l'entité."
  (let ((fields (lumen.data.dao:entity-fields entity-sym)))
    (dolist (f fields)
      (let* ((col-key (getf f :col))          ;; :priority
             (val     (lumen.utils:lookup row col-key)) ;; 2
             (choices (getf f :choices))      ;; ((1 . "Haute")...)
             (ref     (getf f :references))   ;; "users"
	     (hidden? (getf f :hidden?))
             (ref-col (getf f :ref-col)))     ;; "pseudo"
        
        ;; CAS 1 : CHOICES (Enums)
        (when (and (not hidden?) choices val)
          (setf row (acons (%make-label-key col-key)
                           (%resolve-choice-label val choices)
                           row)))
        
        ;; CAS 2 : REFERENCES (FK)
        ;; On ne fait la requête que si on a une valeur, une table ref et une colonne cible
        (when (and (not hidden?) ref ref-col val)
          (let ((label (%resolve-reference-label val ref ref-col tid)))
            (when label
              (setf row (acons (%make-label-key col-key)
                               label
                               row)))))))
    row))

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

      (t payload))))                                ; sinon pas de colonne tenant

(defun entity-field-cols (entity)
  "Colonnes déclarées dans :fields."
  (mapcar (lambda (f) (getf f :col)) (entity-fields entity)))

(defun entity-allowed-cols (entity &key include-pk? include-tenant? include-lock?)
  "Union des colonnes :fields + timestamps + (optionnel) pk/tenant/lock_version.
N’ajoute la colonne tenant que si elle existe effectivement sur l’entité."
  (let* ((md        (entity-metadata entity))
         (fields    (getf md :fields))
         (pk        (and include-pk?     (%kw-col (or (getf md :primary-key) :id))))
         (tcol      (%entity-tenant-col entity md))
         (tenantcol (and include-tenant? tcol))
         ;; lock_version seulement si déclaré ET présent côté fields
         (lock-decl (getf md :lock-version))
         (lockcol   (and include-lock?
                         lock-decl
                         (some (lambda (f) (eq (getf f :col) lock-decl)) fields)
                         (%kw-col lock-decl))))
    (remove-duplicates
     (remove nil
             (append (entity-field-cols entity)
                     (entity-timestamp-cols entity)  ; created_at/updated_at si timestamps présents
                     (list pk tenantcol lockcol)))
     :test #'eq)))

(defun entity-timestamp-cols (entity)
  "Retourne la liste des colonnes timestamps (:created / :updated) si définies via :timestamps."
  (let* ((md (entity-metadata entity))
         (ts (getf md :timestamps)))
    (when ts
      (remove nil (list (%kw-col (getf ts :created))
                        (%kw-col (getf ts :updated)))))))

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

;; Helper pour traiter le résultat de run-in-transaction
(defun %handle-tx-result (result)
  (format t "~&In %handle-tx-result: ~A~%" result)
  (if (and (consp result) (keywordp (car result)))
      (case (car result)
        ;; On déstructure la liste (:business-error "msg" 422)
        (:business-error 
         (destructuring-bind (_ msg code) result
           (declare (ignore _))
	   (format t "~&[REPO DEBUG:%HANDLE-TX-RESULT] Error Response Code : ~A" code)
           ;; On recrée une application-error propre pour Mount-CRUD!
           (error 'lumen.core.error:application-error 
                  :message msg 
                  :code (or code 400))))
        
        (:fatal-error
         (destructuring-bind (_ msg) result
           (declare (ignore _))
           (error "Database Error: ~A" msg)))
        
        (t result))      
      result))

(defun %rows->serialized-entities (entity rows)
  "Transforme des résultats SQL en Alist API, en préservant les colonnes jointes."
  (let* ((serialized
	   (mapcar (lambda (row)
		     (let* (;; 1. On hydrate l'objet pour avoir le formatage standard (dates, etc.)
			    (obj (lumen.data.dao:row->entity entity row))
			    (standard-data (lumen.data.dao:entity->row-alist obj)))
		       ;; 2. On réinjecte les données "orphelines" (celles présentes dans le SQL 
		       ;;    mais absentes de l'entité, comme :project-id-resolved)
		       (loop for (k . v) in row
			     ;; Si la clé n'est pas déjà dans les données standard...
			     unless (assoc k standard-data :test #'eq)
			       ;; ... on l'ajoute à la liste finale
			       do (push (cons k v) standard-data))
              
		       standard-data))
		   rows)))

    (format t "~&ROWS SERIALIZED-ENTITIES: ~A~%" serialized)
    serialized ))

;;; ---------------------------------------------------------------------------
;;; API générique (génériques CLOS)
;;; ---------------------------------------------------------------------------
(defgeneric repo-index  (entity ctx &key filters order select page page-size limit offset after before key))
(defgeneric repo-show   (entity ctx id))
(defgeneric repo-create (entity ctx payload))
(defgeneric repo-patch  (entity ctx id payload))
(defgeneric repo-delete (entity ctx id))

;;; Hooks transverses (spécialisables par entité)
(defgeneric repo-authorize (op entity ctx &key id payload))
(defgeneric repo-normalize (op entity ctx payload))
(defgeneric repo-validate  (op entity ctx payload))
(defgeneric repo-before    (op entity ctx &key id payload))
(defgeneric repo-persist   (op entity ctx &key id payload))
(defgeneric repo-after     (op entity ctx result &key id payload))
;;; ---------------------------------------------------------------------------

;; Hooks par défaut
(defmethod repo-authorize (op (entity symbol) ctx &key id payload)  
  (declare (ignore id payload))
  ;;(format t "~&[Lumen] In REPO AUTHORIZE (Ent: ~A | Op: ~A)~%" entity op)
  (let* ((md (entity-metadata entity))
         (table (string-downcase (or (getf md :table) (symbol-name entity))))
         (write? (member op '(:create :patch :delete))))
    (if write?
        (member (format nil "~a:write" table) (getf ctx :scopes) :test #'string=)
        t)))

(defmethod repo-normalize (op (entity symbol) ctx payload)  
  (declare (ignore op entity ctx))
  ;;(format t "~&[Lumen] In REPO NORMALIZE (Ent: ~A | Op: ~A)~%" entity op)
  payload)

(defmethod repo-validate (op (entity symbol) ctx payload)
  (declare (ignore op ctx))
  ;; APPEL AU NOUVEAU SYSTÈME
  ;; On valide le payload brut. Si ça échoue, on lève l'exception classique.
  ;; Cela permet au code interne (non-HTTP) d'être protégé aussi.
  (let ((errors (lumen.data.dao:validate-entity-payload entity payload)))
    (when errors
      (error 'lumen.data.dao:entity-validation-error :entity entity :errors errors)))
  ;; Si OK, on retourne le payload
  payload)

(defmethod repo-before (op (entity symbol) ctx &key id payload)  
  (declare (ignore op entity ctx id payload))
  ;;(format t "~&[Lumen] In REPO BEFORE (Ent: ~A | Op: ~A)~%" entity op)
  (values))

(defmethod repo-after (op (entity symbol) ctx result &key id payload)
  (declare (ignore id payload))
  (format t "~&[Lumen] In REPO AFTER (~A)~%" entity)
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

;; Implémentations CRUD
(defmethod repo-index ((entity symbol) ctx
                       &key filters order select page page-size limit offset
                          after before key joins)
  
  (let* ((md        (entity-metadata entity))
         (table     (getf md :table))
         (allowed   (entity-allowed-cols entity :include-pk? t :include-tenant? t))
         (tid       (getf ctx :tenant-id))
         (tcol      (%entity-tenant-col entity md))
         (tcol-qualified (when tcol 
                           (format nil "~A.~A" 
                                   (lumen.data.repo.query::%ident table) 
                                   (lumen.data.repo.query::%ident tcol))))
         
         ;; --- Préparation FTS (Identique à votre code) ---
         (fields     (entity-fields entity))
         (table-name (lumen.data.repo.query::%ident table))
         (search-val (cdr (assoc "q" filters :test #'string=)))
         (raw-list   (remove "q" filters :test #'string= :key #'car))
         ;; --- LOGIQUE FILTRES (Identique à votre code) ---
         (processed-filters
           (let ((acc-hash (make-hash-table :test 'equal))
                 (preserved-clauses '())
		 (resulted-clauses nil))
             (loop for item in raw-list do
	       (let ((val (cdr item)))
		 ;; --- CORRECTION : ON FILTRE LES VALEURS VIDES ---
		 (when (and val (not (and (stringp val) (= 0 (length val)))))
		   (cond
                     ((and (consp item) (not (listp (cdr item))) (not (and (> (length (car item)) 2) (string= (subseq (car item) 0 2) "q["))))
                      (push (cdr item) (gethash (car item) acc-hash)))
                     ((listp item)
                      (let* ((col-sym (if (consp (cdr item)) (second item) nil))
                             (col-str (string-downcase (format nil "~A" col-sym))))
			(unless (or (search "tenant_id" col-str) (search "tenant-id" col-str))
			  (push item preserved-clauses))))))
		 (let ((generated-clauses
			 (loop for k being the hash-keys of acc-hash using (hash-value vals)
                               for clean-k = (string-downcase k)
                               for v-list = (nreverse vals)
                               unless (or (search "tenant_id" clean-k) (search "tenant-id" clean-k))
				 collect 
				 (cond
				   ((and (> (length clean-k) 4) (string= (subseq clean-k (- (length clean-k) 4)) "_gte"))
				    (let ((col-name (subseq clean-k 0 (- (length clean-k) 4))))
				      `(>= ,(intern (string-upcase col-name) :keyword) ,(first v-list))))
				   ((and (> (length clean-k) 4) (string= (subseq clean-k (- (length clean-k) 4)) "_lte"))
				    (let ((col-name (subseq clean-k 0 (- (length clean-k) 4))))
				      `(<= ,(intern (string-upcase col-name) :keyword) ,(first v-list))))
				   (t
				    (let ((col-kw (intern (string-upcase clean-k) :keyword)))
				      (print col-kw)
				      (print (first v-list))
				      (if (> (length v-list) 1) `(in ,col-kw ,v-list) `(= ,col-kw ,(first v-list)))))))))
		   (setf resulted-clauses (append preserved-clauses generated-clauses)))))
		   resulted-clauses))
         (processed-filters (remove nil processed-filters))
         (filters-with-tenant 
          (if (and tcol-qualified tid)
              (cons `(= ,(intern (string-upcase tcol-qualified) :keyword) ,tid) processed-filters)
              processed-filters))
         (final-filters 
           (if (and search-val (> (length search-val) 0))
               (cons `(search :q ,search-val) filters-with-tenant)
               filters-with-tenant))

         (text-cols  (loop for f in fields
                           when (member (getf f :type) '(:string :text :email))
                           collect (format nil "~A.~A" table-name (lumen.data.repo.query::%ident (getf f :col)))))
         
         (fts-config (list :fields text-cols)))

    ;; --- EXECUTION PRINCIPALE ---
    (format t "~&[REPO INDEX] FINAL FILTERS: ~A~%" final-filters)
    
    (cond
      ;; MODE KEYSET (si after/before/key présent)
      ((or after before key)
       (let ((res (select-page-keyset* table
                                       :filters final-filters
                                       :order   order
                                       :key     key
                                       :after   after
                                       :limit   (or limit 20)
                                       :joins   joins
                                       :order-whitelist allowed
                                       :fts-config fts-config)))
         ;; Sérialisation des items
         (setf (getf res :items) 
               (mapcar (lambda (row) (enrich-row-metadata entity row tid))
                       (%rows->serialized-entities entity (getf res :items))))
         res))

      ;; MODE OFFSET / PAGE (Défaut)
      (t
       (let* ((p-size (or page-size limit 20))
              (curr-page (or page 1))
              (calc-offset (or offset (* (max 0 (1- curr-page)) p-size)))
              
              ;; 1. COUNT (Pour la pagination)
              (total-count (count* table 
                                   :filters final-filters 
                                   :joins joins 
                                   :fts-config fts-config))
              
              ;; 2. DATA
              (rows (select* table
                             :filters final-filters
                             :order   order
                             :select  (or select :*)
                             :joins   joins
                             :limit   p-size
                             :offset  calc-offset
                             :order-whitelist allowed
                             :fts-config fts-config))
              
              ;; 3. SERIALIZATION
              (items (mapcar (lambda (row) (enrich-row-metadata entity row tid))
                             (%rows->serialized-entities entity rows))))
         
         ;; RETOUR FORMATÉ POUR HTMX VIEW
         (list :items items
               :count total-count
               :per-page p-size
               :current-page curr-page
               :last-page (ceiling total-count p-size)))))))
#|
(defmethod repo-index ((entity symbol) ctx
                       &key filters order select page page-size limit offset
                         after before key joins)
  
  (let* ((md       (entity-metadata entity))
         (table    (getf md :table))
         (allowed  (entity-allowed-cols entity :include-pk? t :include-tenant? t))
         (tid      (getf ctx :tenant-id))
         (tcol     (%entity-tenant-col entity md))
         (tcol-qualified (when tcol 
                           (format nil "~A.~A" 
                                   (lumen.data.repo.query::%ident table) 
                                   (lumen.data.repo.query::%ident tcol))))
         
         ;; --- CORRECTION 2A : Préparation de la Recherche (FTS) ---
         ;; On détecte automatiquement les colonnes texte pour la recherche "q"
         (fields     (entity-fields entity))
         ;; On récupère le nom brut de la table (ex: "tasks")
         (table-name (lumen.data.repo.query::%ident table))
	 
         ;; Extraction et Transformation de "q" ---
         ;; On sépare "q" des autres filtres
	 ;; A. Extraction de "q" (Recherche globale)
         (search-val (cdr (assoc "q" filters :test #'string=)))
         (raw-list (remove "q" filters :test #'string= :key #'car))
         
         ;; B. LOGIQUE D'AGRÉGATION
         (processed-filters
          (let ((acc-hash (make-hash-table :test 'equal))
                (preserved-clauses '()))
            
            ;; 2. TRI INTELLIGENT (Fix du bug (= = ...))
            (loop for item in raw-list do
              (cond
                ;; CAS A : PAIRE CLÉ/VALEUR ("status" . "todo")
                ;; On vérifie que le CDR n'est PAS une liste (donc c'est une paire pointée)
                ((and (consp item) (not (listp (cdr item))) (not (and (> (length (car item)) 2) (string= (subseq (car item) 0 2) "q["))))
                 (push (cdr item) (gethash (car item) acc-hash)))
                
                ;; CAS B : CLAUSE SXQL EXISTANTE (= :col val)
                ((listp item)
                 ;; On garde la clause SAUF si elle concerne le tenant_id (car on le gère manuellement après)
                 ;; On inspecte le 2ème élément de la liste (la colonne)
                 (let* ((col-sym (if (consp (cdr item)) (second item) nil))
                        (col-str (string-downcase (format nil "~A" col-sym))))
                   (unless (or (search "tenant_id" col-str) (search "tenant-id" col-str))
                     (push item preserved-clauses))))))
            
            ;; 3. GÉNÉRATION DES CLAUSES (Depuis la Hash Table)
            (let ((generated-clauses
                   (loop for k being the hash-keys of acc-hash using (hash-value vals)
                         for clean-k = (string-downcase k)
                         for v-list = (nreverse vals)
                         
                         ;; On ignore tenant_id ici aussi (clés string)
                         unless (or (search "tenant_id" clean-k) (search "tenant-id" clean-k))
                         collect 
                         (cond
                           ;; Intervalle MIN (>=)
                           ((and (> (length clean-k) 4) (string= (subseq clean-k (- (length clean-k) 4)) "_gte"))
                            (let ((col-name (subseq clean-k 0 (- (length clean-k) 4))))
                              `(>= ,(intern (string-upcase col-name) :keyword) ,(first v-list))))

                           ;; Intervalle MAX (<=)
                           ((and (> (length clean-k) 4) (string= (subseq clean-k (- (length clean-k) 4)) "_lte"))
                            (let ((col-name (subseq clean-k 0 (- (length clean-k) 4))))
                              `(<= ,(intern (string-upcase col-name) :keyword) ,(first v-list))))
                           
                           ;; Standard (IN ou =)
                           (t
                            (let ((col-kw (intern (string-upcase clean-k) :keyword)))
                              (if (> (length v-list) 1)
                                  `(in ,col-kw ,v-list)
                                  `(= ,col-kw ,(first v-list)))))))))
              
              ;; FUSION (Clauses préservées + Clauses générées)
              (append preserved-clauses generated-clauses))))
         
         ;; On retire les nils (tenant_id traités)
         (processed-filters (remove nil processed-filters))
         
         ;; --- 3. AJOUT MANUEL DU TENANT (Fix du bug :=) ---
         (filters-with-tenant 
          (if (and tcol-qualified tid)
              ;; On utilise le symbole '=' standard, et on qualifie la colonne
              (cons `(= ,(intern (string-upcase tcol-qualified) :keyword) ,tid) 
                    processed-filters)
              processed-filters))
         
         ;; On réintègre la Recherche Globale (Q)
         (final-filters 
           (if (and search-val (> (length search-val) 0))
               (cons `(search :q ,search-val) filters-with-tenant)
               filters-with-tenant))

	 ;; --- CONFIG FTS ---
	 (text-cols  (loop for f in fields
                           when (member (getf f :type) '(:string :text :email))
                             ;; CORRECTION : On construit "tasks.title" au lieu de juste :title
                             collect (format nil "~A.~A" 
                                             table-name
                                             (lumen.data.repo.query::%ident (getf f :col)))))
         
         (fts-config (list :fields text-cols))) ;; On crée la config pour select*
    (format t "~&[REPO INDEX] FINAL FILTERS: ~A~%" final-filters)
    (let ((rows 
            (cond
              ((or after before)
               ;; Note: select-page-keyset* devra aussi être mis à jour pour accepter fts-config 
               ;; si vous utilisez la pagination curseur avec recherche.
               (select-page-keyset* table
                                    :filters final-filters
                                    :order   order
                                    :key     key
                                    :after   after
                                    :limit   (or limit 20)
                                    :joins   joins
                                    :order-whitelist allowed))
              (t
               (select* table
			:filters final-filters
			:order   order
			:select  (or select :*)
			:joins   joins
			:limit   (or (and page-size (* 1 page-size)) limit)
			:offset  (or offset (and page page-size (* (max 0 (1- page)) page-size)))
			:order-whitelist allowed
			;; --- IMPORTANT : On passe la config FTS ---
			:fts-config fts-config)))))
      (format t "~&[REPO INDEX] SQL RESULT: ~A~%" rows)
      (if (and (listp rows) (keywordp (car rows)) (getf rows :items))
          (let ((items (getf rows :items)))
            (setf (getf rows :items) (let ((base (%rows->serialized-entities entity items)))
				       (mapcar #'(lambda (row)
			(enrich-row-metadata entity row tid))
		    base))))
            rows)
          (let ((base (%rows->serialized-entities entity rows)))
	    (mapcar #'(lambda (row)
			(enrich-row-metadata entity row tid))
		    base)))))
|#
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
    
    ;; Plus de ensure-connection ici non plus
    (let ((row (first (select* tbl :filters filters :limit 1))))
      (when row
        ;; 1. Conversion Objet -> Alist de base
        (let ((base-alist (entity->row-alist (row->entity entity row))))
	  ;; 2. Enrichissement avec les Labels
            (enrich-row-metadata entity base-alist tid))))))

(defmethod repo-create ((entity symbol) ctx payload)
  (format t "~&[Lumen] In REPO CREATE (~A)~%" entity)
  (repo-persist :create entity ctx :payload payload))

(defmethod repo-patch ((entity symbol) ctx id payload)
  (format t "~&[Lumen] In REPO PATCH (~A)~%" entity)
  (repo-persist :patch entity ctx :id id :payload payload))

(defmethod repo-delete ((entity symbol) ctx id)
  (format t "~&[Lumen] In REPO DELETE (~A)~%" entity)
  (repo-persist :delete entity ctx :id id))

(defmethod repo-persist ((op (eql :create)) (entity symbol) ctx &key id payload)
  (declare (ignore id))
  (let* ((payload* (%ensure-tenant-on-payload entity ctx payload :op :create))
         (ent      (row->entity entity payload*)))
    ;;(validate-entity! ent)
    (validate-entity-payload entity payload*)
      (multiple-value-bind (_n ent2) (entity-insert! ent :returning t)
        (declare (ignore _n))
        (entity->row-alist ent2))))

(defmethod repo-persist ((op (eql :patch)) (entity symbol) ctx &key id payload)
  (let* ((cur (repo-show entity ctx id)))
    (unless cur (error 'lumen.data.errors:not-found-error))
    
    (let* ((payload* (%ensure-tenant-on-payload entity ctx payload :op :patch))
           (ent       (row->entity entity cur)))
      
      ;; 1. Application des changements (Lisp)
      (dolist (kv payload*)
        (let* ((col (%norm-col-name (car kv)))
               (val  (cdr kv))
               (slot (entity-slot-symbol entity col)))
          (when slot 
             ;; Ceci marque automatiquement le slot comme DIRTY via le (setf slot-value) :after
             (setf (slot-value ent slot) val))))
      
      ;;(validate-entity! ent)
      (validate-entity-payload entity payload*)
      
      ;; 2. Persistance
      ;; On ne met pas :force-all t, car c'est un patch.
      (multiple-value-bind (_n ent2) (entity-update! ent :returning t)
        (declare (ignore _n))
        (entity->row-alist ent2)))))

(defmethod repo-persist ((op (eql :delete)) (entity symbol) ctx &key id payload)
  (declare (ignore payload))
  
  ;; On essaie de récupérer l'entité
  (let ((cur (repo-show entity ctx id)))
    (cond
      ;; CAS 1 : L'entité existe -> On supprime
      (cur
       (entity-delete! (row->entity entity cur))
       `((:deleted . t) (:id . ,id)))
      
      ;; CAS 2 : L'entité n'existe pas (Déjà supprimée ?) -> On renvoie un succès muet
      ;; C'est ce qui absorbe le retry automatique "fantôme" et évite le crash.
      (t
       ;; Log pour info (optionnel)
       (format t "~&[REPO] Delete idempotent: ~A ~A not found (already deleted?)~%" entity id)
       `((:deleted . t) (:id . ,id) (:warning . "idempotent"))))))

;; Transactions :around
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
  (format t "~&[REPO NORM PATCH] ENTERING PAYLOAD: ~A~%" payload)
  (let* ((out (call-next-method op entity ctx payload))
	 (x (format t "~&[REPO NORM PATCH] OUT NEXT METHOD: ~A~%" out))
         (p0 (%normalize-payload-keys out))
         (p1 (%remove-protected-fields p0))
         (tid (%resolve-tenant-id ctx p1)))
    (format t "~&[REPO NORM PATCH] PAYLOAD: ~A~%" p1)
    (unless tid
      (error 'lumen.data.errors:db-error
             :message "missing or invalid tenant-id"))
    ;; Ne JAMAIS permettre de modifier :tenant_id par PATCH
    ;;(remf p1 :tenant_id)
    ;;(acons :tenant_id tid p1)
    (lumen.utils:alist-set p1 :tenant_id tid :test #'eq)
    ))

(defmethod repo-index :around (entity ctx &rest args)
  (let ((op :index))
    ;; 1. Encapsulation transactionnelle
    (let ((res 
           (run-in-transaction
            (lambda ()
              ;; 2. Préparation des filtres (Logique Tenant)
              (destructuring-bind (&key filters &allow-other-keys) args
                (let* ((tid         (getf ctx :tenant-id))
                       (base        (%ensure-filter-list filters))
                       (ten-clause  (%tenant-filter-clause tid))
                       ;; Ajout de la clause tenant si nécessaire
                       (filters* (if ten-clause (append base (list ten-clause)) base))
                       ;; Copie et modification des arguments
                       (args-alist  (alexandria:plist-alist (lumen.utils:copy-plist args))))
                  ;; Mise à jour des filtres dans la liste d'arguments
                  (setf args-alist (lumen.utils:alist-set args-alist :filters filters*))
                  
                  ;; 3. Autorisation
                  (repo-authorize op entity ctx)
                  
                  ;; 4. Exécution (Next Method) et Hook After
                  (let ((result (apply #'call-next-method entity ctx
                                       (alexandria:alist-plist args-alist))))
		    (repo-after op entity ctx result)
		    )))))))
      
      ;; 5. Gestion standard du résultat de transaction
      (%handle-tx-result res))))
 
(defmethod repo-show :around (entity ctx id)
  (let ((op :show))
    ;; 1. Encapsulation transactionnelle
    (let ((res 
           (run-in-transaction
            (lambda ()
              ;; 2. Autorisation
              (repo-authorize op entity ctx :id id)
              
              ;; 3. Récupération de la donnée brute
              (let* ((row (call-next-method))
                     (tid (getf ctx :tenant-id))
                     (rid (and row (or (lumen.utils:col-get row :tenant_id)
				       (lumen.utils:col-get row :tenant-id))))
                     ;; 4. Filtrage de sécurité (Cross-Tenant check)
                     (secure-row 
                      (cond
                        ((null row) nil)                               ; Pas trouvé
                        ((null tid) nil)                               ; Pas de contexte tenant -> Sécurité par défaut
                        ((null rid) row)                               ; Pas de colonne tenant -> Ressource partagée ?
                        ((secure-uuid-equal rid tid) row)              ; Match -> OK
                        (t nil))))                                     ; Mismatch -> Masquage
                
                ;; 5. Hook After (seulement si on a un résultat valide)
                (when secure-row
                  (repo-after op entity ctx secure-row)))))))
      
      ;; 6. Gestion standard du résultat de transaction
      (%handle-tx-result res))))

(defmethod repo-create :around (entity ctx payload)
  (let ((op :create))
    ;; On passe une LAMBDA pure à la DB
    (let ((res 
           (run-in-transaction 
            (lambda ()
              (repo-authorize op entity ctx :payload payload)
	      (print "AUTHORIZE OK")
              (let ((p1 (repo-normalize op entity ctx payload)))
		(print "NORMALISATION OK")
                (repo-validate op entity ctx p1)
		(print "VALIDATE OK")
                (repo-before    op entity ctx :payload p1)
		(print "BEFORE OK")
                (let ((r (call-next-method entity ctx p1)))
                  (let ((r (repo-after op entity ctx r :payload p1)))
		  r)))))))
      ;; On traite le résultat (throw ou return)
      (%handle-tx-result res))))

(defmethod repo-patch :around (entity ctx id payload)
  (let ((op :patch))
    (let ((res
           (run-in-transaction
            (lambda ()
              (repo-authorize op entity ctx :id id :payload payload)
              (let ((p1 (repo-normalize op entity ctx payload)))
		(print "NORMALISATION OK")
                (repo-validate op entity ctx p1)
		(print "VALIDATE OK")
                (repo-before    op entity ctx :id id :payload p1)
		(print "BEFORE OK")
                ;; call-next-method est capturé par la closure (lexical scope)
                ;; C'est safe car run-in-transaction ne fait pas de magie macro.
                (let ((r (call-next-method entity ctx id p1)))
		  (format t "~&[REPO PATCH AROUND] PAYLOAD: ~A~%" p1)
                  (repo-after op entity ctx r :id id :payload p1)))))))
      
      (%handle-tx-result res))))

(defmethod repo-delete :around (entity ctx id)
  (let ((op :delete))
    (let ((res
           (run-in-transaction
            (lambda ()
              (repo-authorize op entity ctx :id id)
              (repo-before    op entity ctx :id id)
              (let ((r (call-next-method entity ctx id)))
                (repo-after op entity ctx r :id id))))))
      
      (%handle-tx-result res))))


