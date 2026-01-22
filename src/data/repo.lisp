(in-package :cl)

(defpackage :lumen.data.repo.query
  (:use :cl)
  (:import-from :lumen.utils :alist-get :copy-plist :alist-set :to-snake-case :col-get)
  (:import-from :lumen.data.db :query-1a :query-a :exec)
  (:import-from :lumen.data.dao
    :entity-metadata :entity-fields :row->entity :entity->row-alist :entity-table :entity-primary-key
    :entity-insert! :entity-update! :entity-delete! :validate-entity! :entity-slot-symbol)
  (:export
   :select* :count* :select-page*
   ;; Constructeurs sûrs (n'entrent pas en conflit avec CL)
   := :in* :and* :or* :search* :gt :lt :gte :lte :ne
   ;; Defaults
   :*default-order-whitelist*
   ;; Fonctions
   :upsert* :select-page-keyset* :select-page-keyset-prev*
   :find-one :find-by :find-all :count-all :delete-by-id :delete-by))

(in-package :lumen.data.repo.query)

;;; ---------------------------------------------------------------------------
;;; Defaults
;;; ---------------------------------------------------------------------------
(defparameter *default-order-whitelist*
  '(:id :created_at :updated_at :name :email))

;;; ---------------------------------------------------------------------------
;;; Constructeurs sûrs (retournent les formes normalisées)
;;; ---------------------------------------------------------------------------
(defun := (field value)           (list '= field value))
(defun in* (field values)         (list 'in field values))
(defun and* (&rest clauses)       (cons 'and clauses))
(defun or*  (&rest clauses)       (cons 'or clauses))
(defun search* (&key q)           (list 'search :q q))
(defun gt (field x)               (list '> field x))
(defun lt (field x)               (list '< field x))
(defun gte (field x)              (list '>= field x))
(defun lte (field x)              (list '<= field x))
(defun ne (field x)               (list '!= field x))

;;; ---------------------------------------------------------------------------
;;; Validation / helpers SQL
;;; ---------------------------------------------------------------------------
(defun %kw-to-col (k)
  "Convertit :my-field en string \"my_field\" pour le SQL."
  (lumen.utils:to-snake-case (string k)))

(defun %get-pk-col (class-sym)
  "Récupère le nom de la colonne PK pour une entité."
  (%kw-to-col (lumen.data.dao:entity-primary-key class-sym)))

(defun %get-table (class-sym)
  "Récupère le nom de la table."
  (lumen.data.dao:entity-table class-sym))

(defun %ident-p (x)
  "Autorise symbol/keyword/string convertibles en ident SQL simple (lowercase, _)."
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
  (unless (%ident-p x)
    (error "Identifiant SQL invalide: ~S" x))
  (string-downcase (etypecase x
                     (string x)
                     (symbol (symbol-name x)))))

(defun %table-sql (table) (%ident table))
(defun %column-sql (col)  (%ident col))

(defun %comma-join (strings)
  (with-output-to-string (s)
    (loop for (a . rest) on strings do
          (princ a s)
          (when rest (princ ", " s)))))

(defun %kw (x)
  (if (keywordp x) x
      (intern (string-upcase (%ident x)) :keyword)))

;;; ---------------------------------------------------------------------------
;;; Param builder ($1, $2, …)
;;; ---------------------------------------------------------------------------
(defun %make-param-emitter ()
  "Retourne deux closures: (emit value) -> \"$N\" et (params) -> liste."
  (let ((n 0)
        (ps '()))
    (values
     (lambda (value)
       (incf n)
       (push value ps)
       (format nil "$~D" n))
     (lambda () (nreverse ps)))))

;;; ---------------------------------------------------------------------------
;;; WHERE builder (auto-contenu)
;;; ---------------------------------------------------------------------------
(defun %build-where (filters fts-config)
  "Construit la clause WHERE SQL. Supporte q[col], q[col][op] et in_col."
  
  (labels ((split-csv (str)
             (loop for start = 0 then (1+ end)
                   for end = (position #\, str :start start)
                   collect (subseq str start end)
                   while end))
           
           (%filter-form-p (f) (and (consp f) (symbolp (car f))))

           ;; Helper pour convertir les opérateurs string (URL) en symboles Lisp
           (parse-operator (op-str)
             (cond
               ((string= op-str ">") '>)
               ((string= op-str "<") '<)
               ((string= op-str ">=") '>=)
               ((string= op-str "<=") '<=)
               ((or (string= op-str "!=") (string= op-str "<>")) '!=) ;; Gère != et <>
               ((string-equal op-str "like") 'like)
               ((string-equal op-str "ilike") 'ilike)
               (t '=)))) 

    (multiple-value-bind (emit-f params-f) (%make-param-emitter)
      (labels
          ((rec (f)
             (cond
               ((null f) "")

               ;; 2. Liste de CLAUSES (AND ...)
               ((and (consp f)
                     (not (symbolp (car f)))
                     (not (stringp (car f)))
                     (every #'%filter-form-p f))
                (rec (cons 'and f)))

               ;; 3. Liste d'ALIST
               ((and (consp f) (consp (car f)) (stringp (caar f)))
                (rec (cons 'and f)))

               ;; 4. Forme OP standard
               ((and (consp f) (symbolp (car f)))
                (let ((op (car f)))
                  (ecase op
                    ((= > < >= <= !=)
                     (destructuring-bind (field value) (cdr f)
                       (let ((lhs (%column-sql field))
                             (ph  (funcall emit-f value)))
                         (ecase op
                           (=  (format nil "~a = ~a" lhs ph))
                           (>  (format nil "~a > ~a" lhs ph))
                           (<  (format nil "~a < ~a" lhs ph))
                           (>= (format nil "~a >= ~a" lhs ph))
                           (<= (format nil "~a <= ~a" lhs ph))
                           (!= (format nil "~a <> ~a" lhs ph)))))) ;; Force SQL <>
                    ((like ilike)
                     (destructuring-bind (field value) (cdr f)
                       (let ((lhs (%column-sql field))
                             (ph  (funcall emit-f value))
                             (sql-op (if (eq op 'like) "LIKE" "ILIKE")))
                         (format nil "~a ~a ~a" lhs sql-op ph))))
                    (in
                     (destructuring-bind (field values) (cdr f)
                       (let ((lhs (%column-sql field)))
                         (if (or (null values) (endp values))
                             "false"
                             (let* ((placeholders (mapcar (lambda (v) (funcall emit-f v)) values))
                                    (phs (%comma-join placeholders)))
                               (format nil "~a in (~a)" lhs phs))))))
                    (and
                     (let* ((parts (mapcar #'rec (cdr f)))
                            (nn (remove-if (lambda (x) (or (null x) (string= x ""))) parts)))
                       (cond ((null nn) "")
                             ((= 1 (length nn)) (first nn))
                             (t (format nil "(~{~a~^ AND ~})" nn)))))
                    (or
                     (let* ((parts (mapcar #'rec (cdr f)))
                            (nn (remove-if (lambda (x) (or (null x) (string= x ""))) parts)))
                       (cond ((null nn) "")
                             ((= 1 (length nn)) (first nn))
                             (t (format nil "(~{~a~^ OR ~})" nn)))))
                    (search
                     (destructuring-bind (&key q)
                         (if (and (consp (cdr f)) (keywordp (cadr f))) (cdr f) (list :q (second f)))
                       (let ((query q))
                         (cond
                           ((and fts-config (getf fts-config :tsvector))
                            (let* ((lang (or (getf fts-config :language) "simple"))
                                   (tscol (%column-sql (getf fts-config :tsvector)))
                                   (ph (funcall emit-f query)))
                              (format nil "to_tsvector(~a, coalesce(~a,'')) @@ plainto_tsquery(~a, ~a)"
                                      (prin1-to-string lang) tscol (prin1-to-string lang) ph)))
                           (t
                            (let* ((fields (or (getf fts-config :fields) '()))
                                   (like (lambda (col)
                                           (let ((ph (funcall emit-f (format nil "%~a%" query))))
                                             (format nil "~a ILIKE ~a" (%column-sql col) ph))))
                                   (parts (mapcar like fields)))
                              (cond
                                ((null parts) "false")
                                ((= 1 (length parts)) (first parts))
                                (t (format nil "(~{~a~^ OR ~})" parts))))))))))))

               ;; 5. Forme ALIST ("key" . "val") -> CORRECTION MAJEURE ICI
               ((and (consp f) (stringp (car f)))
                (let* ((raw-key (car f))
                       (raw-val (cdr f)))
                  (cond
                    ;; A. Paramètres système ignorés
                    ((or (search "order[" raw-key) (search "limit" raw-key)
                         (search "page" raw-key) (search "page_size" raw-key))
                     "")
                    
                    ;; B. Gestion "in_xxx"
                    ((and (> (length raw-key) 3) (string= (subseq raw-key 0 3) "in_"))
                     (let* ((real-col (subseq raw-key 3))
                            (keyword  (intern (string-upcase real-col) :keyword))
                            (all-vals (split-csv raw-val))
                            (clean-vals (remove-if (lambda (x) (or (string= x "") (string-equal x "null") (string-equal x "nil"))) all-vals)))
                       (if clean-vals (rec (list 'in keyword clean-vals)) "false")))

                    ;; C. Parsing q[col][op] ou q[col]
                    (t
                     (let ((col-name nil)
                           (operator '=))
                       
                       (if (and (> (length raw-key) 2) (string= (subseq raw-key 0 2) "q["))
                           ;; C'est du q[...]
                           (let ((idx-close-1 (position #\] raw-key)))
                             (if idx-close-1
                                 (progn
                                   ;; 1. Nom de colonne : entre index 2 et le premier ]
                                   (setf col-name (subseq raw-key 2 idx-close-1))
                                   
                                   ;; 2. Opérateur : s'il y a un [ après le premier ]
                                   (let* ((idx-open-2 (position #\[ raw-key :start idx-close-1))
                                          (idx-close-2 (and idx-open-2 (position #\] raw-key :start idx-open-2))))
                                     (when (and idx-open-2 idx-close-2)
                                       (setf operator (parse-operator (subseq raw-key (1+ idx-open-2) idx-close-2))))))
                                 ;; Fallback si malformé
                                 (setf col-name (subseq raw-key 2))))
                           
                           ;; Pas de q[...]
                           (setf col-name raw-key))

                       ;; Génération
                       (rec (list operator 
                                  (intern (string-upcase col-name) :keyword) 
                                  raw-val)))))))

               (t (error "Clause de filtre invalide: ~S" f)))))
        (let* ((sql (rec filters)))
          (values sql (funcall params-f)))))))

;;; ---------------------------------------------------------------------------
;;; ORDER builder (avec whitelist)
;;; ---------------------------------------------------------------------------
(defun %build-order (order whitelist)
  (let ((wl (or whitelist '())))
    (labels
        ((allowed? (sym)
           (member (intern (string-upcase (%ident sym)) :keyword) wl :test #'eql)))
      (let ((parts
              (loop for spec in (or order '())
                    for (col dir) = spec
                    do (unless (allowed? col)
                         (error "ORDER BY interdit pour la colonne ~S (hors whitelist)" col))
                    collect (format nil "~a ~a"
                                    (%column-sql col)
                                    (string-upcase (if (eq dir :desc) "DESC" "ASC"))))))
        (if (null parts) "" (%comma-join parts))))))

;;; ---------------------------------------------------------------------------
;;; SELECT list
;;; ---------------------------------------------------------------------------
(defun %build-select (select)
  (cond
    ((or (null select) (eq select :*)) "*")
    (t (let ((cols (mapcar #'%column-sql select)))
         (%comma-join cols)))))

;;; ---------------------------------------------------------------------------
;;; API
;;; ---------------------------------------------------------------------------
(defun %join-non-nil (items &key (sep ", "))
  "Concatène les éléments non-NIL de ITEMS avec SEP."
  (let ((xs (remove nil items)))
    (cond
      ((null xs) "")
      (t (with-output-to-string (out)
           (loop for i from 0
                 for x in xs
                 do (progn
                      (when (> i 0) (princ sep out))
                      (princ x out))))))))

(defun %norm-col-name (x)
  "UPCASE + remplace _ par - pour comparer noms de colonnes
   (DB underscore) ↔ (alist Postmodern hyphen)."
  (let ((s (etypecase x
             (symbol (symbol-name x))
             (string x))))
    (substitute #\- #\_ (string-upcase s))))

(defun %alist-val-ci (alist key)
  "Récupère la valeur associée à KEY (insensible casse & _↔-)."
  (let ((target (%norm-col-name key)))
    (loop for (k . v) in alist
          for ks = (%norm-col-name k)
          when (string= ks target)
            do (return v)
          finally (return nil))))

(defun select* (table &key filters order select limit offset
                      (order-whitelist '()) (fts-config nil))
  "Retourne (list d'alists)."
  (multiple-value-bind (where-sql where-params) (%build-where filters fts-config)
    (let* ((select-sql (%build-select select))
           (order-sql  (%build-order order (or order-whitelist *default-order-whitelist*)))
           (sql (with-output-to-string (s)
                  (format s "select ~a from ~a" select-sql (%table-sql table))
                  (when (and where-sql (> (length where-sql) 0))
                    (format s " where ~a" where-sql))
                  (when (and order-sql (> (length order-sql) 0))
                    (format s " order by ~a" order-sql))
                  (when limit  (format s " limit ~d" (max 0 (truncate limit))))
                  (when offset (format s " offset ~d" (max 0 (truncate offset)))))))
      (format t "~&### IN SELECT* | SQL: ~A~%WHERE-PARAMS: ~A~%##########~%"
	      sql where-params)
      (multiple-value-bind (rows _n)
          (apply #'query-a sql where-params)
        (declare (ignore _n))
        rows))))

(defun count* (table &key filters (fts-config nil))
  "Retourne le nombre total correspondant aux filtres."
  (multiple-value-bind (where-sql where-params) (%build-where filters fts-config)
    (let* ((sql (with-output-to-string (s)
                  (format s "select count(*) as cnt from ~a" (%table-sql table))
                  (when (and where-sql (> (length where-sql) 0))
                    (format s " where ~a" where-sql)))))
      (multiple-value-bind (rows _n)
          (apply #'query-a sql where-params)
        (declare (ignore _n))
        (let ((row (first rows)))
          (or (and row (parse-integer (princ-to-string (cdr (assoc :cnt row))))) 0))))))

(defun select-page* (table &key filters order select page page-size
                           (order-whitelist '()) (fts-config nil))
  "Retourne plist :items :total :page :page-size."
  (let* ((p (max 1 (or page 1)))
         (ps (max 1 (or page-size 20)))
         (off (* (1- p) ps))
         (items (select* table :filters filters :order order :select select
                         :limit ps :offset off
                         :order-whitelist order-whitelist :fts-config fts-config))
         (total (count* table :filters filters :fts-config fts-config)))
    (list :items items :total total :page p :page-size ps)))

(defun %alist-keys-preserving-order (alist)
  (mapcar #'car alist))

(defun %alist-values-preserving-order (alist)
  (mapcar #'cdr alist))

(defun %maybe-updated-at-set (ts-config)
  "Retourne la clause SQL d’update du timestamp si configurée, sinon NIL."
  (when (and ts-config (getf ts-config :updated))
    (let ((col (%column-sql (getf ts-config :updated)))
          (fn  (or (getf ts-config :db-fn) "CURRENT_TIMESTAMP")))
      (format nil "~a = ~a" col fn))))

(defun %kw-set (lst)
  (mapcar (lambda (x) (intern (string-upcase (%ident x)) :keyword)) lst))

(defun upsert* (table row-alist
               &key conflict-columns update-columns returning
                    touch-updated-col (touch-db-fn "CURRENT_TIMESTAMP"))
  "INSERT ... ON CONFLICT (...) DO UPDATE ...
Arguments:
  TABLE              — ident SQL sûr.
  ROW-ALIST          — alist (:col . value) → colonnes à insérer.
  CONFLICT-COLUMNS   — liste de colonnes (obligatoire).
  UPDATE-COLUMNS     — NIL=auto (ROW-ALIST \\ CONFLICT-COLUMNS) | :none | liste.
  RETURNING          — NIL | T (=*) | liste de colonnes.
  TOUCH-UPDATED-COL  — si non NIL, ajoute `col = <touch-db-fn>` si absent d'UPDATE.
  TOUCH-DB-FN        — littéral SQL (par défaut CURRENT_TIMESTAMP)."
  (unless (and conflict-columns (listp conflict-columns) (plusp (length conflict-columns)))
    (error "upsert*: :conflict-columns requis et non vide"))

  (let* ((table-sql (%ident table))
         (cols-raw  (%alist-keys-preserving-order row-alist))
         (vals-raw  (%alist-values-preserving-order row-alist))
         (cols      (mapcar #'%ident cols-raw))
         (conflict-cols (mapcar #'%ident conflict-columns))
         (conflict-set (%kw-set conflict-columns))
         (update-list
           (cond
             ((eq update-columns :none) :none)
             ((null update-columns)
              ;; toutes les colonnes insérées hors colonnes de conflit
              (remove-if (lambda (ckw) (member ckw conflict-set :test #'eq))
                         (%kw-set cols-raw)))
             (t
              (%kw-set update-columns))))
         (do-update-p (not (eq update-list :none))))

    (multiple-value-bind (emit params-f) (%make-param-emitter)
      (let* ((placeholders (mapcar (lambda (v) (funcall emit v)) vals-raw))
             (insert-cols-sql (%comma-join cols))
             (values-sql     (%comma-join placeholders))
             (conflict-sql   (format nil "(~{~a~^, ~})" conflict-cols))
             ;; contruire SET parts sans NIL
             (set-parts
               (when do-update-p
                 (let* ((upd-cols-sql
                          (mapcar (lambda (ckw)
                                    (let ((c (%ident ckw)))
                                      (format nil "~a = EXCLUDED.~a" c c)))
                                  update-list))
                        (touch-part
                          (when (and touch-updated-col
                                     (not (member (%kw touch-updated-col) update-list :test #'eq)))
                            (format nil "~a = ~a" (%ident touch-updated-col) touch-db-fn))))
                   (remove "" (append upd-cols-sql (list touch-part)) :test #'string=))))
             (on-conflict-sql
               (if do-update-p
                   (let ((set-sql (%join-non-nil set-parts)))
                     (when (string= set-sql "")
                       (error "upsert*: aucune colonne à mettre à jour dans DO UPDATE"))
                     (format nil "on conflict ~a do update set ~a" conflict-sql set-sql))
                   (format nil "on conflict ~a do nothing" conflict-sql)))
             (returning-sql
               (cond
                 ((eq returning t) " returning *")
                 ((and (listp returning) (every #'identity returning))
                  (format nil " returning ~{~a~^, ~}"
                          (mapcar #'%ident returning)))
                 ((null returning) "")
                 (t " returning *")))
             (sql (format nil
                          "insert into ~a (~a) values (~a) ~a~a"
                          table-sql insert-cols-sql values-sql on-conflict-sql returning-sql)))
        (apply #'lumen.data.db:exec sql (funcall params-f))))))

;;; ---------------------------------------------------------------------------
;;; Keyset pagination
;;; ---------------------------------------------------------------------------
(defun %normalize-key (key)
  (cond
    ((null key) (error "select-page-keyset*: :key est requis"))
    ((listp key) (mapcar #'%ident key))
    (t (list (%ident key)))))

(defun %order-columns-and-dir (order)
  "Retourne (values cols dir). Si plusieurs colonnes sont fournies,
vérifie que toutes les directions sont identiques (ASC/DESC).
Ex: '((:created_at :desc)) -> (\"created_at\") :desc
    '((:created_at :desc) (:id :desc)) -> (\"created_at\" \"id\") :desc"
  (let* ((pairs (or order '()))
         (cols (mapcar (lambda (p) (%ident (first p))) pairs))
         (dirs (remove-duplicates (mapcar (lambda (p) (or (second p) :asc)) pairs) :test #'eq)))
    (cond
      ((null cols) (error "select-page-keyset*: :order est requis et ne peut pas être vide"))
      ((> (length dirs) 1)
       (error "select-page-keyset*: toutes les directions d'ORDER doivent être identiques pour la keyset pagination"))
      (t (values cols (first dirs))))))

(defun %universal-time-plausible-p (v)
  "Heuristique: V entier de type universal-time SBCL (>> epoch Unix)."
  (and (integerp v) (> v 3000000000))) ; ~2065 Unix, << universal-time (1900 epoch)

(defun %ut->iso8601-utc (ut)
  "Universal-time (secondes depuis 1900-01-01) -> string ISO8601 UTC \"YYYY-MM-DDTHH:MM:SSZ\"."
  (multiple-value-bind (sec min hour day mon year dow dst tz)
      (decode-universal-time ut 0) ; 0 = UTC
    (declare (ignore dow dst tz))
    (format nil "~4,'0D-~2,'0D-~2,'0DT~2,'0D:~2,'0D:~2,'0DZ"
            year mon day hour min sec)))

(defun %normalize-keyset-value (v)
  "Si V ressemble à un universal-time, renvoie une ISO8601 UTC, sinon V inchangé."
  (if (%universal-time-plausible-p v)
      (%ut->iso8601-utc v)
      v))

(defun %keyset-where (key-cols dir after emit-f)
  "WHERE pour la page suivante (AFTER).
- ASC  : tuple(cols)  >  ROW(vals)
- DESC : tuple(cols)  <  ROW(vals)
Mono-colonne: même logique sans tuple."
  (when after
    (let* ((vals-raw (if (listp after) after (list after)))
           (vals (mapcar #'%normalize-keyset-value vals-raw)))
      (unless (= (length vals) (length key-cols))
        (error "select-page-keyset*: :after cardinalité ~D ≠ :key ~D"
               (length vals) (length key-cols)))
      (let ((op (if (eq dir :desc) "<" ">")))
        (if (= 1 (length key-cols))
            (let* ((col (first key-cols))
                   (ph  (funcall emit-f (first vals))))
              (values (format nil "~a ~a ~a" (%ident col) op ph) t))
            (let* ((phs (mapcar (lambda (v) (funcall emit-f v)) vals))
                   (cols-sql (format nil "(~{~a~^, ~})" (mapcar #'%ident key-cols)))
                   (row-sql  (format nil "ROW(~{~a~^, ~})" phs)))
              (values (format nil "~a ~a ~a" cols-sql op row-sql) t)))))))

(defun select-page-keyset* (table
                            &key filters order key after limit
                                 (order-whitelist '()) (fts-config nil))
  "Keyset pagination: retourne plist :items :next-cursor :limit."
  (let* ((lim (max 1 (or limit 20)))
         (key-cols (%normalize-key key)))
    (multiple-value-bind (order-cols dir) (%order-columns-and-dir order)
      (unless (equal key-cols order-cols)
        (error "select-page-keyset*: :key (~{~a~^, ~}) doit correspondre exactement aux colonnes d':order (~{~a~^, ~})"
               key-cols order-cols))
      (multiple-value-bind (emit-f params-f) (%make-param-emitter)
        (multiple-value-bind (where-sql-base where-params)
            (%build-where filters fts-config)
          (dolist (p where-params) (funcall emit-f p))
          (multiple-value-bind (ks-where pushed?) (%keyset-where key-cols dir after emit-f)
            (declare (ignore pushed?))
            (let* ((select-sql (%build-select :*))
                   (order-sql  (%build-order order (or order-whitelist *default-order-whitelist*)))
                   (sql (with-output-to-string (s)
                          (format s "select ~a from ~a" select-sql (%table-sql table))
                          (cond
                            ((and where-sql-base ks-where (plusp (length where-sql-base)) (plusp (length ks-where)))
                             (format s " where (~a) and (~a)" where-sql-base ks-where))
                            ((and where-sql-base (plusp (length where-sql-base)))
                             (format s " where ~a" where-sql-base))
                            ((and ks-where (plusp (length ks-where)))
                             (format s " where ~a" ks-where)))
                          (when (and order-sql (plusp (length order-sql)))
                            (format s " order by ~a" order-sql))
                          (format s " limit ~d" lim))))
              (multiple-value-bind (rows _n)
                  (apply #'query-a sql (funcall params-f))
                (declare (ignore _n))
                (let* ((last (car (last rows)))
                       ;; extraction robuste du curseur (hyphen/underscore)
                       (next-cursor (when last
                                      (let ((vals (mapcar (lambda (c)
                                                            (%alist-val-ci last c))
                                                          key-cols)))
                                        (if (= 1 (length vals))
                                            (first vals)
                                            vals)))))
                  (list :items rows :next-cursor next-cursor :limit lim))))))))))

;;; ---------------------------------------------------------------------------
;;; Keyset pagination (précédent)
;;; ---------------------------------------------------------------------------
(defun %keyset-where-before (key-cols dir before emit-f)
  "WHERE pour la page précédente (BEFORE).
- ASC  : COL < $cursor
- DESC : COL > $cursor
Composite : (c1,c2,...) OP ROW($n1,$n2,...)"
  (when before
    (let* ((vals-raw (if (listp before) before (list before)))
           (vals (mapcar #'%normalize-keyset-value vals-raw))
           (op (if (eq dir :desc) ">" "<")))
      (unless (= (length vals) (length key-cols))
        (error "select-page-keyset-prev*: :before cardinalité ~D ≠ :key ~D"
               (length vals) (length key-cols)))
      (if (= 1 (length key-cols))
          (let* ((col (first key-cols))
                 (ph  (funcall emit-f (first vals))))
            (values (format nil "~a ~a ~a" (%ident col) op ph) t))
          (let* ((phs (mapcar (lambda (v) (funcall emit-f v)) vals))
                 (cols-sql (format nil "(~{~a~^, ~})" (mapcar #'%ident key-cols)))
                 (row-sql  (format nil "ROW(~{~a~^, ~})" phs)))
            (values (format nil "~a ~a ~a" cols-sql op row-sql) t))))))

(defun select-page-keyset-prev* (table
                                 &key filters order key before limit
                                      (order-whitelist '()) (fts-config nil))
  "Keyset pagination (page précédente) : retourne plist :items :prev-cursor :limit."
  (let* ((lim (max 1 (or limit 20)))
         (key-cols (%normalize-key key)))
    (multiple-value-bind (order-cols dir) (%order-columns-and-dir order)
      (unless (equal key-cols order-cols)
        (error "select-page-keyset-prev*: :key (~{~a~^, ~}) doit correspondre à :order (~{~a~^, ~})"
               key-cols order-cols))
      (multiple-value-bind (emit-f params-f) (%make-param-emitter)
        (multiple-value-bind (where-sql-base where-params)
            (%build-where filters fts-config)
          (dolist (p where-params) (funcall emit-f p))
          (multiple-value-bind (ks-where pushed?) (%keyset-where-before key-cols dir before emit-f)
            (declare (ignore pushed?))
            (let* ((select-sql (%build-select :*))
                   (order-sql  (%build-order order (or order-whitelist *default-order-whitelist*)))
                   (sql (with-output-to-string (s)
                          (format s "select ~a from ~a" select-sql (%table-sql table))
                          (cond
                            ((and where-sql-base ks-where (plusp (length where-sql-base)) (plusp (length ks-where)))
                             (format s " where (~a) and (~a)" where-sql-base ks-where))
                            ((and where-sql-base (plusp (length where-sql-base)))
                             (format s " where ~a" where-sql-base))
                            ((and ks-where (plusp (length ks-where)))
                             (format s " where ~a" ks-where)))
                          (when (and order-sql (plusp (length order-sql)))
                            (format s " order by ~a" order-sql))
                          (format s " limit ~d" lim))))
              (multiple-value-bind (rows _n)
                  (apply #'query-a sql (funcall params-f))
                (declare (ignore _n))
                (let* ((first (first rows))
                       ;; extraction robuste
                       (prev-cursor (when first
                                      (let ((vals (mapcar (lambda (c)
                                                            (%alist-val-ci first c))
                                                          key-cols)))
                                        (if (= 1 (length vals)) (first vals) vals)))))
                  (list :items rows :prev-cursor prev-cursor :limit lim))))))))))


;; ------------------------
;; ---       ORM        ---
;; ------------------------

;; --- READERS (LECTURE) ---
(defun find-one (entity-class id &key (cols "*"))
  "Récupère une entité par sa clé primaire.
   Exemple: (find-one 'audit \"uuid-123\")"
  (let* ((table (%get-table entity-class))
         (pk    (%get-pk-col entity-class))
         (sql   (format nil "SELECT ~A FROM ~A WHERE ~A = $1 LIMIT 1" 
                        cols table pk))
         (row   (lumen.data.db:query-1a sql id)))
    (when row
      ;; Hydratation via votre DAO existant
      (lumen.data.dao:row->entity entity-class row))))

(defun find-by (entity-class field value &key (cols "*"))
  "Récupère la PREMIÈRE entité correspondant au critère.
   Exemple: (find-by 'user :email \"bob@mail.com\")"
  (let* ((table (%get-table entity-class))
         (col   (%kw-to-col field))
         (sql   (format nil "SELECT ~A FROM ~A WHERE ~A = $1 LIMIT 1" 
                        cols table col))
         (row   (lumen.data.db:query-1a sql value)))
    (when row
      (lumen.data.dao:row->entity entity-class row))))

(defun find-all (entity-class &key (where "true") (params nil) (limit 100) (offset 0) (order-by "created_at DESC"))
  "Récupère une liste d'entités avec filtrage SQL brut et pagination.
   Exemple: (find-all 'audit :where \"status = $1\" :params '(\"done\"))"
  (let* ((table (%get-table entity-class))
         (sql   (format nil "SELECT * FROM ~A WHERE ~A ORDER BY ~A LIMIT ~D OFFSET ~D"
                        table where order-by limit offset))
         (rows  (apply #'lumen.data.db:query-a sql params)))
    ;; On mappe le constructeur d'entité sur la liste de résultats
    (mapcar (lambda (row) (lumen.data.dao:row->entity entity-class row))
            rows)))

(defun count-all (entity-class &key (where "true") (params nil))
  "Compte les entités."
  (let* ((table (%get-table entity-class))
         (sql   (format nil "SELECT count(*) as c FROM ~A WHERE ~A" table where))
         (res   (apply #'lumen.data.db:query-1a sql params)))
    ;; Gestion robuste du retour (alist ou valeur)
    (if (listp res)
        (cdr (assoc :c res))
        res)))

;; --- WRITERS (ÉCRITURE) ---
(defun delete-by-id (entity-class id)
  "Supprime une entité par sa PK. Retourne le nombre de lignes affectées (0 ou 1)."
  (let* ((table (%get-table entity-class))
         (pk    (%get-pk-col entity-class))
         (sql   (format nil "DELETE FROM ~A WHERE ~A = $1" table pk)))
    (lumen.data.db:exec sql id)))

(defun delete-by (entity-class field value)
  "Supprime des entités par un champ spécifique (ex: audit_id pour des findings)."
  (let* ((table (%get-table entity-class))
         (col   (%kw-to-col field))
         (sql   (format nil "DELETE FROM ~A WHERE ~A = $1" table col)))
    (lumen.data.db:exec sql value)))
