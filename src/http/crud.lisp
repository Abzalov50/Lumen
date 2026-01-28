(in-package :cl)

(defpackage :lumen.http.crud
  (:use :cl)
  (:import-from :lumen.core.http :req-query :ctx-from-req :respond-json :respond-404)
  (:import-from :lumen.utils :alist-get)
  (:import-from :lumen.data.dao  :validate-entity! :row->entity :entity-insert!
		:entity-update! :entity-delete! :entity-metadata  :entity-fields
		:entity-slot-symbol :entity->row-alist :entity-table)
  (:import-from :lumen.core.http
   :respond-text :respond-json :resp-status :req-path :req-headers :req-query
   :req-method :req-cookies :req-params :respond-404 :respond-422 :respond-500
   :respond-201 :respond-204 :respond-redirect :respond-sse
   :parse-cookie-header :format-set-cookie :add-set-cookie
		:ctx-get :ctx-set! :set-last-modified!)
  (:import-from :lumen.core.router  :with-params  :param :defroute)
  (:import-from :lumen.core.error 
   :application-error :application-error-message :application-error-code)
  (:export :mount-crud!
           :make-entity-crud-guard
           ;; Helpers appelés par le code généré
           :%handle-crud-index 
           :%handle-crud-show 
           :%handle-crud-create 
           :%handle-crud-patch 
           :%handle-crud-delete))

(in-package :lumen.http.crud)

;;; ===========================================================================
;;; 1. HELPERS RUNTIME (La logique métier pure)
;;; ===========================================================================
;;; Ces fonctions sont appelées à l'exécution de la requête.
(defun %kw (x) (etypecase x (symbol (if (keywordp x) x (intern (string-upcase (symbol-name x)) :keyword)))
                          (string (intern (string-upcase x) :keyword))))

(defun %split (s ch)
  (loop with acc = '()
        for start = 0 then (1+ pos)
        for pos = (position ch s :start start)
        do (push (subseq s start (or pos (length s))) acc)
        while pos
        finally (return (nreverse acc))))

(defun %derive-default-order (entity)
  "Par défaut: si timestamps: (:created_at desc, :id asc) sinon (:id asc)."
  (let* ((md (lumen.data.dao:entity-metadata entity))
         (ts (getf md :timestamps))
         (pk (or (getf md :primary-key) :id)))
    (if (and ts (getf ts :created))
        (list (list (getf ts :created) :desc)
              (list pk :asc))
        (list (list pk :asc)))))

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

(defun %ensure-key-matches-order (key order)
  (let ((order-cols (mapcar #'first order)))
    (unless (and key (equal key order-cols))
      (error "Keyset: :key (~{~a~^, ~}) doit correspondre exactement à :order (~{~a~^, ~})"
             key order-cols))))

(defun %handle-app-error (c)
  "Gère les erreurs métier proprement (JSON)."
  (let ((msg (lumen.core.error:application-error-message c))
        (code (lumen.core.error:application-error-code c)))
    (lumen.core.http:respond-json 
     `((:status . "error") (:code . "BUSINESS_ERROR") (:message . ,msg))
     :status code)))

(defmacro with-crud-error-handling (&body body)
  `(handler-case 
       (progn ,@body)
     (lumen.core.error:application-error (c) (%handle-app-error c))))

;; --- PARSING UTILS ---
(defun %parse-pagination-params (qp)
  (values 
   (%maybe-number (alist-get qp :page))
   (%maybe-number (alist-get qp :page_size))
   (%maybe-number (alist-get qp :limit))))

(defun %parse-filter-params (qp)
  (let* ((reserved '("q" "select" "order" "key" "after" "before" "page" "page_size" "limit"))
         (q-filters (or (alist-get qp :q) '()))
         (root-filters (remove-if (lambda (p) (member (car p) reserved :test #'string-equal)) qp)))
    (append q-filters root-filters)))

;; --- HANDLERS D'ACTIONS ---

(defun %handle-crud-index (req entity-sym auth-guard order-whitelist)
  (with-crud-error-handling
    ;; 1. Garde
    (when auth-guard (funcall auth-guard req :op :index))
    
    ;; 2. Parsing & Query
    (let* ((ctx (ctx-from-req req))
           (qp  (req-query req))
           (filters (%parse-filter-params qp)))
      
      (multiple-value-bind (page psize limit) (%parse-pagination-params qp)
        (let ((result (lumen.data.repo.core:repo-index 
                       entity-sym ctx
                       :filters filters
                       :select (%parse-select (alist-get qp :select))
                       :order  (or (%ensure-order-whitelist
                                       (%parse-order (lumen.utils:alist-get qp :order))
                                       (or order-whitelist
                                           (mapcar (lambda (f) (getf f :col))
                                                   (lumen.data.dao:entity-fields entity-sym))))
                                      (%derive-default-order entity-sym))
                       :key    (%parse-key (alist-get qp :key))
                       :after  (%parse-after/before (alist-get qp :after))
                       :before (%parse-after/before (alist-get qp :before))
                       :page page :page-size psize :limit limit)))
          (respond-json result))))))

(defun %handle-crud-show (req entity-sym auth-guard id)
  (with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :show))
    (let* ((ctx (ctx-from-req req))
           (row (lumen.data.repo.core:repo-show entity-sym ctx id)))
      (if row
          (respond-json row)
          (respond-404)))))

(defun %handle-crud-create (req entity-sym auth-guard)
  (with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :create))
    (let* ((ctx (ctx-from-req req))
           (payload (lumen.core.http:ctx-get req :json))
           (res (lumen.data.repo.core:repo-create entity-sym ctx payload)))
      (respond-json res :status 201))))

(defun %handle-crud-patch (req entity-sym auth-guard id)
  (with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :patch))
    (let* ((ctx (ctx-from-req req))
           (payload (lumen.core.http:ctx-get req :json))
           (res (lumen.data.repo.core:repo-patch entity-sym ctx id payload)))
      (respond-json res))))

(defun %handle-crud-delete (req entity-sym auth-guard id)
  (with-crud-error-handling
    (when auth-guard (funcall auth-guard req :op :delete))
    (let* ((ctx (ctx-from-req req))
           (res (lumen.data.repo.core:repo-delete entity-sym ctx id)))
      (respond-json res))))


(defun %crud-scope-for (entity op)
  "Mappe l’opération CRUD vers un scope logique table:perm."
  (let* ((md   (lumen.data.dao:entity-metadata entity))
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

(defun %run-mw-as-guard (mw-instance req)
  "Exécute un middleware objet. 
   - Si le middleware appelle 'next', on retourne T.
   - Si le middleware coupe la chaîne (ex: 401/403), on retourne sa réponse."
  (let* ((dummy-next (lambda (r) (declare (ignore r)) :ok))
         (result (lumen.core.pipeline:handle mw-instance req dummy-next)))
    (if (eql result :ok)
        t
      result)))

(defun %jwt-required-or-response (req)
  "Retourne T si un JWT valide est présent, sinon la réponse 401."
  (let ((mw (make-instance 'lumen.core.middleware:auth-middleware 
                           :required-p t
                           ;; On laisse le secret se résoudre via la config globale si nil
                           :secret lumen.core.jwt:*jwt-secret*)))
    (%run-mw-as-guard mw req)))

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

;;; ===========================================================================
;;; 2. GÉNÉRATEUR DE ROUTES (Compile-Time)
;;; ===========================================================================
;;; Cette fonction est appelée par defmodule pour générer le code.

(defun mount-crud! (entity-sym &key (base "/api") (name nil) 
                                    (order-whitelist '())
                                    (auth-guard nil)
                                    (host nil)
                                    (actions '(:index :show :create :patch :delete)))
  "Génère une LISTE de formulaires (construct-route ...) pour le CRUD."
  
  ;; 1. Récupération des métadonnées (Pré-enregistrées par defmodule)
  (let* ((md (lumen.data.dao:entity-metadata entity-sym))
         ;; Safety check : si md est nil, le pré-enregistrement a échoué
         (_ (unless md (error "Métadonnées introuvables pour ~A dans mount-crud!" entity-sym)))
         
         ;; Si md est une Plist (cas compile-time), getf fonctionne.
         (tbl (getf md :table))
         (seg (or name tbl (string-downcase (symbol-name entity-sym))))
         (base-list (format nil "~a/~a" base seg))
         (base-item (format nil "~a/~a/:id" base seg))
         (forms '()))

    ;; Helper pour gérer le host optionnel
    (flet ((route-spec (p) (if host `(:host ,host :path ,p) p)))

      ;; INDEX
      (when (member :index actions)
        (push `(lumen.core.router:construct-route :GET ,(route-spec base-list) (req)
                 (%handle-crud-index req ',entity-sym ,auth-guard ',order-whitelist))
              forms))

      ;; SHOW
      (when (member :show actions)
        (push `(lumen.core.router:construct-route :GET ,(route-spec base-item) (req)
                 (lumen.core.router:with-params (req id)
                   (%handle-crud-show req ',entity-sym ,auth-guard id)))
              forms))

      ;; CREATE
      (when (member :create actions)
        (push `(lumen.core.router:construct-route :POST ,(route-spec base-list) (req)
                 (%handle-crud-create req ',entity-sym ,auth-guard))
              forms))

      ;; PATCH
      (when (member :patch actions)
        (push `(lumen.core.router:construct-route :PATCH ,(route-spec base-item) (req)
                 (lumen.core.router:with-params (req id)
                   (%handle-crud-patch req ',entity-sym ,auth-guard id)))
              forms))

      ;; DELETE
      (when (member :delete actions)
        (push `(lumen.core.router:construct-route :DELETE ,(route-spec base-item) (req)
                 (lumen.core.router:with-params (req id)
                   (%handle-crud-delete req ',entity-sym ,auth-guard id)))
              forms)))

    ;; IMPORTANT : On retourne la liste dans l'ordre
    (nreverse forms)))
