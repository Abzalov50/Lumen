;;;; Lumen / http / crud.lisp
(in-package :cl)

(defpackage :lumen.http.crud
  (:use :cl)
  (:import-from :lumen.utils   :alist-get)
  (:import-from :lumen.utils.json :parse)
  ;; HTTP helpers (adapte selon tes modules actuels)
  (:import-from :lumen.core.http
   :respond-text :respond-json :resp-status :req-path :req-headers :req-query
   :req-method :req-cookies :req-params :respond-404 :respond-422 :respond-500
   :respond-201 :respond-204 :respond-redirect :respond-sse
   :parse-cookie-header :format-set-cookie :add-set-cookie
		:ctx-get :ctx-set! :set-last-modified!)
  (:import-from :lumen.core.error 
   :application-error :application-error-message :application-error-code)
  (:import-from :lumen.core.router  :with-params  :param :defroute)
  (:import-from :lumen.core.middleware :auth-middleware)
  ;; DAO/Repo
  (:import-from :lumen.data.dao  :validate-entity! :row->entity :entity-insert!
		:entity-update! :entity-delete! :entity-metadata  :entity-fields
		:entity-slot-symbol :entity->row-alist :entity-table)
  (:import-from :lumen.data.repo.query :select* :count* :select-page*
		:select-page-keyset* :select-page-keyset-prev*)
  ;; Util
  (:import-from :alexandria      :when-let :alist-plist)
  (:export :mount-crud! :mount-discovery! :make-entity-crud-guard
   :register-custom-route))

(in-package :lumen.http.crud)

;; --- Transition propre : si MOUNT-CRUD! est une fonction ordinaire, on la délie avant de créer le generic
(eval-when (:compile-toplevel :load-toplevel :execute)
  (when (and (fboundp 'mount-crud!)
             (not (typep (symbol-function 'mount-crud!) 'standard-generic-function)))
    (fmakunbound 'mount-crud!)))

;;; ----------------------------------------------------------------------------
;;; Utils: parsing query → filters/order/pagination (sécurisé)
;;; ----------------------------------------------------------------------------
(defun %http-date (ut)
  "universal-time → RFC1123 string (UTC)."
  (multiple-value-bind (sec min hr day mon yr dow dst tz)
      (decode-universal-time ut 0)
    (declare (ignore dst tz))
    (format nil "~a, ~2,'0D ~a ~4,'0D ~2,'0D:~2,'0D:~2,'0D GMT"
            (nth dow '("Sun" "Mon" "Tue" "Wed" "Thu" "Fri" "Sat"))
            day (nth (1- mon) '("Jan" "Feb" "Mar" "Apr" "May" "Jun"
                                 "Jul" "Aug" "Sep" "Oct" "Nov" "Dec"))
            yr hr min sec)))

(defun %strong-etag-from-row (row &key lock-version-col)
  "ETag \"W/\\\"...\\\"\" ou \"\\\"...\\\"\" (on utilise strong ETag ici).
Si lock-version-col est dispo, on l’encode directement ; sinon hash JSON du row."
  (let ((ver (and lock-version-col (cdr (assoc lock-version-col row)))))
    (if ver
        (format nil "\"v~a\"" ver)
        (let* ((s (prin1-to-string row))
               (sum (sxhash s)))
          (format nil "\"h~X\"" sum)))))

(defun %row-timestamp (row &key updated-col created-col)
  "Retourne un universal-time pour Last-Modified en lisant d’abord updated, sinon created."
  (or (cdr (assoc updated-col row))
      (cdr (assoc created-col row))))

(defun %kw (x) (etypecase x (symbol (if (keywordp x) x (intern (string-upcase (symbol-name x)) :keyword)))
                         (string (intern (string-upcase x) :keyword))))

(defun %split (s ch)
  (loop with acc = '()
        for start = 0 then (1+ pos)
        for pos = (position ch s :start start)
        do (push (subseq s start (or pos (length s))) acc)
        while pos
        finally (return (nreverse acc))))

(defun %parse-order (s)
  "order=field:asc,created_at:desc → '((:field :asc) (:created_at :desc))"
  (when s
    (loop for item in (%split s #\,)
          for pair = (%split item #\:)
          for col = (first pair)
          for dir = (or (second pair) "asc")
          collect (list (%kw col) (if (string-equal dir "desc") :desc :asc)))))

(defun %parse-select (s)
  "select=*,id,name → :* ou '(:id :name)"
  (when s
    (if (string= s "*") :* (mapcar #'%kw (%split s #\,)))))

(defun %maybe-number (s)
  (handler-case
      (parse-integer s)
    (error () s)))

(defun %parse-key (s)
  "key=col1,col2"
  (when s (mapcar #'%kw (%split s #\,))))

(defun %parse-after/before (s)
  "after/before: soit valeur simple, soit JSON '[\"v1\",123]' (simple: parse int si possible)."
  (when s
    (cond
      ((and (plusp (length s)) (char= (char s 0) #\[))
       ;; JSON array → parse via lecteur json déjà présent dans Lumen
       (let ((v (lumen.utils.json:parse s))) ; adapte selon ton package JSON
         v))
      (t (%maybe-number s)))))

(defun %ensure-order-whitelist (order whitelist)
  "Filtre ORDER sur WHITELIST."
  (remove-if (lambda (pair) (not (member (first pair) whitelist :test #'eq))) order))

(defun %derive-default-order (entity)
  "Par défaut: si timestamps: (:created_at desc, :id asc) sinon (:id asc)."
  (let* ((md (entity-metadata entity))
         (ts (getf md :timestamps))
         (pk (or (getf md :primary-key) :id)))
    (if (and ts (getf ts :created))
        (list (list (getf ts :created) :desc)
              (list pk :asc))
        (list (list pk :asc)))))

(defun %ensure-key-matches-order (key order)
  (let ((order-cols (mapcar #'first order)))
    (unless (and key (equal key order-cols))
      (error "Keyset: :key (~{~a~^, ~}) doit correspondre exactement à :order (~{~a~^, ~})"
             key order-cols))))

(defun %precondition-check! (req row &key lock-version-col updated-col created-col)
  "Vérifie If-Match (fort) en priorité ; sinon If-Unmodified-Since. Signal erreur 412 si non satisfait."
  (let* ((hdrs (lumen.core.http:req-headers req)) ; adapte si besoin
         (if-match (gethash "If-Match" hdrs))
         (ius     (gethash "If-Unmodified-Since" hdrs))
         (etag    (%strong-etag-from-row row :lock-version-col lock-version-col))
         (lm-ut   (%row-timestamp row :updated-col updated-col :created-col created-col)))
    (cond
      ;; If-Match présent → il doit matcher l’ETag courant
      (if-match
       (unless (and etag (string= etag if-match))
         (error "precondition-failed")))
      ;; Sinon on peut tolérer If-Unmodified-Since si LM disponible (faible)
      (ius
       (when lm-ut
         ;; parse RFC1123 → universal-time : à implémenter si nécessaire ; sinon on ignore
         ;; ici, simple no-op (ou compare si tu as un parseur)
         nil))
      (t
       ;; pas de précondition : si l’entité a un lock_version, on **exige** If-Match
       (when (cdr (assoc lock-version-col row))
         (error "missing-if-match"))))))

;;; ----------------------------------------------------------------------------
;;; Guard Factory
;;; ----------------------------------------------------------------------------
(defun %crud-scope-for (entity op)
  "Mappe l’opération CRUD vers un scope logique table:perm."
  (let* ((md   (entity-metadata entity))
         (seg  (or (string-downcase (symbol-name entity))
		   (getf md :table)))
         (tbl  (string-downcase (etypecase seg
                                  (string seg)
                                  (symbol (symbol-name seg))))))
    (ecase op
      (:index   (list (format nil "read:~a"   tbl)))
      (:show    (list (format nil "read:~a"   tbl)))
      (:create  (list (format nil "write:~a"  tbl)))
      (:patch   (list (format nil "write:~a"  tbl)))
      (:delete  (list (format nil "delete:~a" tbl))))))

;;; ---------------------------------------------------------------------------
;;; Helper: Exécuter un Middleware comme un Guard (Prédicat)
;;; ---------------------------------------------------------------------------

(defun %run-mw-as-guard (mw-instance req)
  "Exécute un middleware objet. 
   - Si le middleware appelle 'next', on retourne T.
   - Si le middleware coupe la chaîne (ex: 401/403), on retourne sa réponse."
  (let* ((dummy-next (lambda (r) (declare (ignore r)) :ok))
         (result (lumen.core.pipeline:handle mw-instance req dummy-next)))
    (if (eql result :ok)
        t
        result)))

;;; ---------------------------------------------------------------------------
;;; 1. JWT Required
;;; ---------------------------------------------------------------------------

(defun %jwt-required-or-response (req)
  "Retourne T si un JWT valide est présent, sinon la réponse 401."
  (let ((mw (make-instance 'lumen.core.middleware:auth-middleware 
                           :required-p t
                           ;; On laisse le secret se résoudre via la config globale si nil
                           :secret lumen.core.jwt:*jwt-secret*)))
    (%run-mw-as-guard mw req)))

;;; ---------------------------------------------------------------------------
;;; 2. Guard Factory (Générique)
;;; ---------------------------------------------------------------------------

(defun make-jwt-guard (&key (required-p t) roles scopes scopes-mode
                            (allow-query-token-p t) 
                            ;; Note: qs-keys n'est pas supporté par le auth-middleware standard actuel
                            ;; (il utilise '("access_token" "token") en dur).
                            (qs-keys nil)) 
  (declare (ignore qs-keys)) 
  "Fabrique une closure (req) ⇒ T | Response."
  
  ;; On instancie le middleware UNE SEULE FOIS à la création du guard (Performance)
  (let ((mw (make-instance 'lumen.core.middleware:auth-middleware
                           :required-p required-p
                           :roles-allow roles
                           :scopes-allow scopes
                           :scopes-mode (or scopes-mode :any)
                           :allow-query allow-query-token-p
                           :secret lumen.core.jwt:*jwt-secret*)))
    (lambda (req)
      (%run-mw-as-guard mw req))))

;;; ---------------------------------------------------------------------------
;;; 3. Entity CRUD Guard
;;; ---------------------------------------------------------------------------

(defun make-entity-crud-guard (entity &key (required-p nil))
  "Retourne un guard dynamique (req &key op) ⇒ T | Response.
   Calcule les scopes requis à la volée selon l'opération."
  (lambda (req &key op &allow-other-keys)
    (cond
      ;; Pas d'opération spécifiée ? On vérifie juste la présence du token
      ((null op) 
       (%jwt-required-or-response req))
      
      ;; Opération spécifiée -> Scope précis requis
      (t
       ;; Supposons que %crud-scope-for existe (ex: "users:read")
       (let* ((required-scopes (%crud-scope-for entity op))
              (mw (make-instance 'lumen.core.middleware:auth-middleware
                                 :required-p required-p
                                 :scopes-allow required-scopes
                                 :scopes-mode :all ;; On veut ce scope précis
                                 :secret lumen.core.jwt:*jwt-secret*)))
         (%run-mw-as-guard mw req))))))

;;; ----------------------------------------------------------------------------
;;; Montage CRUD
;;; ----------------------------------------------------------------------------
;; Déclare le generic (on garde les &key pour compat et on accepte d’autres clés)
(defmethod mount-crud! ((entity symbol)
                        &key (base "/api") (name nil)
                          (order-whitelist '())
                          (auth-guard nil)
                          (host nil)
                          (actions '(:index :show :create :patch :delete)))
  "Monte des routes CRUD pour ENTITY.
   :actions permet de limiter les routes (ex: '(:index :show)).

EXEMPLES DE CONSTRUCTION D'URL VALIDES (QUERY PARAMS) :

1. Filtrage Simple (Paramètres à la racine)
   Tout paramètre inconnu est traité comme un filtre d'égalité SQL.
   - URL : /api/users?role=admin&is_active=true
   - SQL : WHERE role = 'admin' AND is_active = true

2. Filtrage Explicite (Namespace 'q[...]')
   Méthode robuste (recommandée) pour éviter les collisions avec les clés système.
   - URL : /api/users?q[unit_id]=123&q[lastname]=Doe
   - SQL : WHERE unit_id = '123' AND lastname = 'Doe'

3. Pagination
   Gère le découpage des résultats.
   - URL : /api/users?page=2&page_size=20
   - Note : Le paramètre 'limit' est un alias accepté pour 'page_size'.

4. Tri (Sorting)
   Format attendu : 'colonne' (ascendant par défaut) ou 'colonne:desc'.
   - URL : /api/users?order=created_at:desc
   - URL : /api/users?order=lastname:asc

5. Sélection de Colonnes (Projection)
   Permet de ne récupérer que certains champs pour alléger le JSON.
   - URL : /api/users?select=id,firstname,lastname,email

6. Recherche Textuelle Globale
   Le paramètre 'q' utilisé sans crochets déclenche une recherche (ILIKE ou FTS).
   - URL : /api/documents?q=rapport
   - SQL : (col1 ILIKE '%rapport%' OR col2 ILIKE '%rapport%')

7. Combinaison Complexe
   Tous les paramètres peuvent être combinés.
   - URL : /api/documents?target_type=folder&q=fip&page=1&order=created_at:desc
   - Action : Récupère les documents de type 'folder' contenant le mot 'fip',
              affiche la 1ère page, triée par date de création décroissante.

ARGUMENTS :
  :actions - Liste des opérations à monter (ex: '(:index :show)).
  :base    - Préfixe de l'URL (défaut: \"/api\").
  :name    - Nom de la ressource dans l'URL (défaut: table de l'entité).
  :host    - Virtual host optionnel (string ou liste).
  :auth-guard - Fonction (lambda (req &key op)) retournant T ou une réponse HTTP."  
  (let* ((md (lumen.data.dao:entity-metadata entity))
         (seg (or name (getf md :table)))
         (base-list (format nil "~a/~a" base seg))
         (base-item (format nil "~a/~a/:id" base seg)))
    
    (labels
        ((ctx-from-req* (req) (lumen.core.http:ctx-from-req req))

         (guard! (req op)
           (when auth-guard
             (let ((gret (funcall auth-guard req :op op)))
               (unless (eq gret t)
                 (return-from guard! gret)))))

         (route-spec (path)
           (cond
             ((null host) path)
             ((stringp host) (list :host host :path path))
             ((listp host)   (list :hosts host :path path))
             (t path)))

         ;; --- NOUVEAU HELPER DE GESTION D'ERREUR ---
         (handle-app-error (c)
           "Transforme une erreur métier en réponse JSON 400 propre."
           (let ((msg (lumen.core.error:application-error-message c)) ;; Via l'accesseur
                 (code (lumen.core.error:application-error-code c)))  ;; Via l'accesseur
             ;; Log optionnel pour le debug
             (format t "~&[CRUD] Business Error: ~A~%" msg)
	     (format t "~&[CRUD] Erreur reçue avec Code: ~A~%" code)
             (lumen.core.http:respond-json 
              `((:status . "error")
                (:code . "BUSINESS_ERROR")
                (:message . ,msg))
              :status code))))

      ;; INDEX
      (when (member :index actions)
        (lumen.core.router:defroute GET (route-spec base-list) (req)
          (handler-case ;; <--- PROTECTION
              (progn
                (guard! req :index)
                (let* ((ctx (ctx-from-req* req))
                       (qp  (lumen.core.http:req-query req))
                       (reserved-keys '("q" "select" "order" "key" "after" "before" "page" "page_size" "limit"))
                       (filters-q (or (lumen.utils:alist-get qp :q) '()))
                       (filters-root (remove-if (lambda (pair) 
                                                  (member (car pair) reserved-keys :test #'string-equal)) 
                                                qp))
                       (filters (append filters-q filters-root))
                       (select    (%parse-select (lumen.utils:alist-get qp :select)))
                       (order     (or (%ensure-order-whitelist
                                       (%parse-order (lumen.utils:alist-get qp :order))
                                       (or order-whitelist
                                           (mapcar (lambda (f) (getf f :col))
                                                   (lumen.data.dao:entity-fields entity))))
                                      (%derive-default-order entity)))
                       (key       (%parse-key (lumen.utils:alist-get qp :key)))
                       (after     (%parse-after/before (lumen.utils:alist-get qp :after)))
                       (before    (%parse-after/before (lumen.utils:alist-get qp :before)))
                       (page      (%maybe-number (lumen.utils:alist-get qp :page)))
                       (psize     (%maybe-number (lumen.utils:alist-get qp :page_size)))
                       (limit     (%maybe-number (lumen.utils:alist-get qp :limit))))
                  
                  (lumen.core.http:respond-json
                   (lumen.data.repo.core:repo-index entity ctx
                                                    :filters filters 
                                                    :order order :select select
                                                    :page page :page-size psize :limit limit
                                                    :after after :before before :key key))))
            ;; CATCH
            (lumen.core.error:application-error (c) (handle-app-error c)))))

      ;; SHOW
      (when (member :show actions)
        (lumen.core.router:defroute GET (route-spec base-item) (req)
          (handler-case ;; <--- PROTECTION
              (progn
                (guard! req :show)
                (lumen.core.router:with-params (req id)
                  (let* ((ctx (ctx-from-req* req))
                         (row (lumen.data.repo.core:repo-show entity ctx id)))
                    (if row
                        (lumen.core.http:respond-json row)
                        (lumen.core.http:respond-404)))))
            ;; CATCH
            (lumen.core.error:application-error (c) (handle-app-error c)))))

      ;; CREATE
      (when (member :create actions)
        (lumen.core.router:defroute POST (route-spec base-list) (req)
          (handler-case ;; <--- PROTECTION
              (progn
                (guard! req :create)
                (let* ((ctx (ctx-from-req* req))
                       (payload (lumen.core.http:ctx-get req :json))
                       (res (lumen.data.repo.core:repo-create entity ctx payload)))
                  (lumen.core.http:respond-json res :status 201)))
            ;; CATCH
            (lumen.core.error:application-error (c) (handle-app-error c)))))

      ;; PATCH
      (when (member :patch actions)
        (lumen.core.router:defroute PATCH (route-spec base-item) (req)
          (handler-case ;; <--- PROTECTION
              (progn
                (guard! req :patch)
                (lumen.core.router:with-params (req id)
                  (let* ((ctx (ctx-from-req* req))
                         (payload (lumen.core.http:ctx-get req :json))
                         (res (lumen.data.repo.core:repo-patch entity ctx id payload)))
                    (lumen.core.http:respond-json res))))
            ;; CATCH
            (lumen.core.error:application-error (c) (handle-app-error c)))))

      ;; DELETE
      (when (member :delete actions)
        (lumen.core.router:defroute DELETE (route-spec base-item) (req)
          (handler-case ;; <--- PROTECTION
              (progn
                (guard! req :delete)
                (lumen.core.router:with-params (req id)
                  (let* ((ctx (ctx-from-req* req))
                         (res (lumen.data.repo.core:repo-delete entity ctx id)))
                    (lumen.core.http:respond-json res))))
            ;; CATCH
            (lumen.core.error:application-error (c) (handle-app-error c)))))

      ;; Retourne les patterns
      (list base-list base-item))))
