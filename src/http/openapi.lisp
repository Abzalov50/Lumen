;;;; --------------------------------------------------------------------------
;;;; Auto-discovery: /_routes & /openapi.json
;;;; --------------------------------------------------------------------------

(in-package :lumen.http.crud)

;; Registry des CRUD montés
(defvar *crud-registry* (make-hash-table :test 'equal))

;; Registre pour les routes manuelles (non-CRUD)
;; Structure : liste de plists (:method :path :summary :params :tags ...)
(defvar *custom-route-registry* nil)

(defun register-custom-route (method path &key summary params tag security)
  "Enregistre une route manuelle pour la documentation."
  ;; On normalise path et method
  (let ((entry (list :method (string-upcase (string method))
                     :path   path
                     :summary (or summary "Route personnalisée")
                     :params params
                     :tag (or tag "Custom")
                     :security security)))
    ;; On évite les doublons (même method + même path)
    (setf *custom-route-registry*
          (delete-if (lambda (x) 
                       (and (string= (getf x :method) (getf entry :method))
                            (string= (getf x :path)   (getf entry :path))))
                     *custom-route-registry*))
    (push entry *custom-route-registry*)))

;;; Mappers de type (schéma vs paramètres)
(defun %oapi-type+format-for-schema (ftype)
  "Types pour components.schemas (modèle de données)."
  (cond
    ((and (consp ftype) (eq (first ftype) :array)) (cons "array" nil))
    ((member ftype '(:integer :bigint))            (cons "integer" nil))
    ((eq ftype :boolean)                           (cons "boolean" nil))
    ((member ftype '(:timestamptz :timestamp))     (cons "string" "date-time"))
    ((eq ftype :date)                              (cons "string" "date"))
    ((eq ftype :time)                              (cons "string" "time"))
    ((eq ftype :uuid)                              (cons "string" "uuid"))
    ((eq ftype :jsonb)                             (cons "object" nil)) ; <— important: objet coté schéma
    (t                                             (cons "string" nil))))

(defun %oapi-type+format-for-param (ftype)
  "Types pour query params (filtrage). On passe souvent du texte."
  (cond
    ((and (consp ftype) (eq (first ftype) :array)) (cons "string" nil)) ; CSV
    ((member ftype '(:integer :bigint))            (cons "integer" nil))
    ((eq ftype :boolean)                           (cons "boolean" nil))
    ((member ftype '(:timestamptz :timestamp))     (cons "string" "date-time"))
    ((eq ftype :date)                              (cons "string" "date"))
    ((eq ftype :time)                              (cons "string" "time"))
    ((eq ftype :uuid)                              (cons "string" "uuid"))
    ((eq ftype :jsonb)                             (cons "string" nil)) ; JSON encodé
    (t                                             (cons "string" nil))))


;;; ----------------------------------------------------------------------------
;;; OpenAPI helpers: query params (filters / tri / pagination)
;;; ----------------------------------------------------------------------------
(defun %kw-col (x)
  "Normalise une 'colonne' en keyword.
   Accepte : KEYWORD, SYMBOL, ou STRING (éventuellement \":col\").
   Retourne NIL si X est inconvertible."
  (cond
    ((null x) nil)
    ((keywordp x) x)
    ((symbolp x)  (intern (string-upcase (symbol-name x)) :keyword))
    ((stringp x)
     (let* ((s (string-trim '(#\Space #\Tab #\Newline #\Return) x))
            (s (if (and (> (length s) 0) (char= (char s 0) #\:))
                   (subseq s 1)
                   s)))
       (if (string= s "") nil (intern (string-upcase s) :keyword))))
    (t nil)))

(defun %merge-alists (a b)
  (let ((ht (make-hash-table :test 'equal)))
    (dolist (p a) (setf (gethash (car p) ht) (cdr p)))
    (dolist (p b) (setf (gethash (car p) ht) (cdr p)))
    (let (out) (maphash (lambda (k v) (push (cons k v) out)) ht) (nreverse out))))

(defun %field-label (field)
  (or (getf field :label)
      (string-capitalize (string-downcase (symbol-name (getf field :col))))))

(defun %field-help (field)
  (getf field :help))

(defun %field-choices (field)
  "Retourne une liste de paires (value . label) ou NIL."
  (getf field :choices))

(defun %field-ui (field) (getf field :ui))
(defun %field-hidden-p (field) (eq (getf field :hidden?) t))

(defun %oapi-x-ui (field)
  "Transforme :ui DSL → x-ui (pour le front)."
  (let ((ui (%field-ui field))
        (label (or (getf field :label)
                   (string-capitalize
		    (string-downcase (symbol-name (getf field :col)))))))
    (remove nil
	    `((:label . ,label)
              ,@(when ui
		  (mapcan (lambda (k)
			    (let ((v (getf ui k)))
                              (when v (list (cons k v)))))
			  '(:index? :formatter :width :grow :sortable? :format)))))))

(defun %enum-schema-from-choices (choices)
  "Construit (:enum [...]) + (:x-enumNames [...]) si labels présents."
  (when (and choices (listp choices))
    (let* ((vals (mapcar #'car choices))
           (labs (mapcar #'cdr choices))
           (have-labels (some #'identity labs)))
      (append `((:enum . ,vals))
              (when have-labels `((:x-enumNames . ,labs)))))))

(defun %oapi-type+format (ftype)
  "Mappe un type champ → (type . format) OpenAPI."
  (cond
    ((and (consp ftype) (eq (first ftype) :array))
     (cons "array" nil)) ; item type géré ailleurs au besoin
    ((member ftype '(:integer :bigint) :test #'eq) (cons "integer" nil))
    ((eq ftype :boolean) (cons "boolean" nil))
    ((member ftype '(:timestamptz :timestamp) :test #'eq) (cons "string" "date-time"))
    ((eq ftype :date) (cons "string" "date"))
    ((eq ftype :time) (cons "string" "time"))
    ((eq ftype :uuid) (cons "string" "uuid"))
    ((member ftype '(:email)) (cons "string" nil))
    ((eq ftype :jsonb) (cons "string" nil)) ; côté query, on passe du texte (JSON encodé)
    (t (cons "string" nil))))

(defun %oapi-make-param (name in type &key format description required)
  `((:name . ,name)
    (:in   . ,in)
    ,@(when required '((:required . t)))
    ,@(when description `((:description . ,description)))
    (:schema . ,(append (list (cons :type type))
                        (when format (list (cons :format format)))))))

(defun %oapi-filter-params-for-field (field)
  "Construit les paramètres q[...] pour un FIELD (dao field alist) avec enum si :choices."
  (let* ((col    (getf field :col))
         (ftype  (getf field :type))
         (choices (%field-choices field))
         (nm     (string-downcase (symbol-name col)))
         (label  (%field-label field))
         (help   (%field-help field))
         (mkdesc (lambda (base)
                   (remove nil (list base
                                     (when label (format nil " — ~a" label))
                                     (when help  (format nil " — ~a" help))))))
         (tf     (%oapi-type+format-for-param ftype))
         (base   (format nil "q[~a]" nm))

         (schema= (append (list (cons :type (car tf)))
                          (when (cdr tf) (list (cons :format (cdr tf))))
                          (%enum-schema-from-choices choices)))

         (range-p (or (member ftype '(:integer :bigint))
                      (member ftype '(:date :time :timestamptz :timestamp))))

         (schema-num (append (list (cons :type (car tf)))
                             (when (cdr tf) (list (cons :format (cdr tf))))))

         (p= `((:name . ,base) (:in . "query")
               (:description . ,(apply #'concatenate 'string (funcall mkdesc (format nil "Filtre égalité pour ~a" nm))))
               (:schema . ,schema=)))

         (pmin (when range-p
                 `((:name . ,(format nil "q[min_~a]" nm)) (:in . "query")
                   (:description . ,(apply #'concatenate 'string (funcall mkdesc (format nil "Filtre borne min (>=) pour ~a" nm))))
                   (:schema . ,schema-num))))

         (pmax (when range-p
                 `((:name . ,(format nil "q[max_~a]" nm)) (:in . "query")
                   (:description . ,(apply #'concatenate 'string (funcall mkdesc (format nil "Filtre borne max (<=) pour ~a" nm))))
                   (:schema . ,schema-num))))

         (pin (let ((desc (apply #'concatenate 'string
                                 (funcall mkdesc (format nil "Filtre IN (CSV) pour ~a" nm)))))
                (if choices
                    `((:name . ,(format nil "q[in_~a]" nm)) (:in . "query")
                      (:description . ,desc)
                      (:schema . ((:type . "string") (:description . "Liste CSV. Valeurs autorisées.")
                                  ,@(%enum-schema-from-choices choices))))
                    `((:name . ,(format nil "q[in_~a]" nm)) (:in . "query")
                      (:description . ,desc)
                      (:schema . ((:type . "string"))))))))

    ;; Ne documente pas les champs :hidden? t
    (unless (%field-hidden-p field)
      (remove nil (list p= pmin pmax pin)))))

(defun %oapi-common-list-params ()
  "Paramètres communs pour listage/tri/pagination."
  (list
   (%oapi-make-param "select" "query" "string"
                     :description "Colonnes (ex: id,ref,title) ou *")
   (%oapi-make-param "order"  "query" "string"
                     :description "Tri: col.asc|desc[,col2.asc|desc...]")
   (%oapi-make-param "page"   "query" "integer" :description "Page (>=1)")
   (%oapi-make-param "limit"  "query" "integer" :description "Taille de page (1..200)")
   ;; Keyset (optionnel)
   (%oapi-make-param "key"    "query" "string" :description "Keyset: colonne(s) clé (ex: created_at,id)")
   (%oapi-make-param "after"  "query" "string" :description "Curseur NEXT (valeur simple ou JSON array)")
   (%oapi-make-param "before" "query" "string" :description "Curseur PREV (valeur simple ou JSON array)")
   (%oapi-make-param "q[search]" "query" "string" :description "Recherche texte (FTS/ILIKE selon config)")))

(defun %synthetic-ts-fields-for-oapi (entity)
  (let* ((md (entity-metadata entity))
         (ts (getf md :timestamps))
         (created (%kw-col (getf ts :created)))
         (updated (%kw-col (getf ts :updated))))
    (remove nil
            (list (when created `(:col ,created :type :timestamptz :label "Créé le"))
                  (when updated `(:col ,updated :type :timestamptz :label "MAJ le"))))))

(defun %oapi-list-params-for-entity (entity)
  "Concatène params communs + q[...] spécifiques aux champs (avec label/help)."
  (let* ((fields (append (entity-fields entity) (%synthetic-ts-fields-for-oapi entity)))
         (per-field (mapcan #'%oapi-filter-params-for-field fields)))
    (append (%oapi-common-list-params) per-field)))

(defun %register-crud (entity table seg base-list base-item pk order-whitelist)
  (setf (gethash (list (string-downcase (symbol-name entity)) seg) *crud-registry*)
        (list :entity entity
              :table  table
              :segment seg
              :base-list  base-list
              :base-item  base-item
              :primary-key pk
              :order-whitelist order-whitelist)))

(defun %custom-routes->viewer-format ()
  "Transforme les routes custom pour le format attendu par ApiRoutesViewer.js"
  (let ((grouped (make-hash-table :test 'equal)))
    
    ;; 1. Groupement par Tag
    (dolist (r *custom-route-registry*)
      (let ((tag (or (getf r :tag) "Divers")))
        (push r (gethash tag grouped))))
    
    ;; 2. Formatage
    (let (acc)
      (maphash (lambda (tag routes)
                 (push `((:entity . ,tag) ;; On utilise le Tag comme nom d'entité
                         (:table . "N/A")
                         (:segment . "custom")
                         (:primary-key . "-")
                         (:order-whitelist . ())
                         (:endpoints . ,(mapcar (lambda (x)
                                                  `((:method . ,(getf x :method))
                                                     (:path . ,(getf x :path))
                                                     (:summary . ,(getf x :summary))))
                                                (nreverse routes))))
                       acc))
               grouped)
      acc)))

(defun %crud-registry->routes ()
  "Retourne une liste d’objets décrivant les endpoints CRUD montés.
   Gère les order-whitelist sous forme de paires (:col :desc)."
  (let (acc)
    (maphash
     (lambda (_k v)
       (declare (ignore _k))
       (push `(
               (:entity .       ,(string-downcase (symbol-name (getf v :entity))))
               (:table  .       ,(getf v :table))
               (:segment .      ,(getf v :segment))
               (:primary-key .  ,(string-downcase (symbol-name (getf v :primary-key))))
               
               ;; --- CORRECTION ICI ---
               (:order-whitelist . ,(mapcar (lambda (entry)
                                              ;; Si c'est une liste (:col :desc), on prend le car.
                                              ;; Si c'est un symbole :col, on le prend tel quel.
                                              (let ((col (if (consp entry) (car entry) entry)))
                                                (string-downcase (symbol-name col))))
                                            (getf v :order-whitelist)))
               ;; ----------------------

               (:endpoints .
                   (((:method . "GET") (:path . ,(getf v :base-list))
                     (:summary . "List / search (offset or keyset)"))
                    ((:method . "GET")
                     (:path . ,(concatenate 'string
                                            (getf v :base-list) "/{id}"))
                     (:summary . "Get by id"))
                    ((:method . "POST") (:path . ,(getf v :base-list))
                     (:summary . "Create"))
                    ((:method . "PATCH")
                     (:path . ,(concatenate 'string
                                            (getf v :base-list) "/{id}"))
                     (:summary . "Patch update"))
                    ((:method . "DELETE")
                     (:path . ,(concatenate 'string
                                            (getf v :base-list) "/{id}"))
                     (:summary . "Delete")))))
             acc))
     lumen.http.crud::*crud-registry*)
    (nreverse acc)))

(defun %oapi-safe-name (s)
  (string-downcase (etypecase s
                     (symbol (symbol-name s))
                     (string s))))

(defun %oapi-components-schema (entity)
  (let* ((fields   (entity-fields entity))
	 (table    (entity-table entity))
         (required (loop for f in fields
                         when (getf f :required?)
                         collect (string-downcase (symbol-name (getf f :col)))))
         (props
          (loop for f in fields
                unless (%field-hidden-p f)       ; <— ne pas exposer hidden
                collect
                (let* ((col-s  (string-downcase (symbol-name (getf f :col))))
                       (ftype  (getf f :type))
                       (tf     (%oapi-type+format-for-schema ftype))
                       (title  (%field-label f))
                       (desc   (%field-help f))
                       (enum   (%enum-schema-from-choices (%field-choices f)))
                       (xui    (%oapi-x-ui f))
		       (xref   (getf f :references))
		       (xref-c (getf f :ref-col))
		       (xref-f (getf f :ref-format))
		       )
                  (cons col-s
                        (remove nil
                          (append
                           (list (cons :type (car tf)))			   
                           (when (cdr tf) (list (cons :format (cdr tf))))
                           (when title (list (cons :title title)))
                           (when desc  (list (cons :description desc)))
                           enum
                           (when xui   (list (cons :x-ui xui)))
			   (when xref  (list (cons :x-ref xref)))
			   (when xref-c (list (cons :x-ref-col xref-c)))
			   (when xref-f (list (cons :x-ref-format xref-f)))
                           (let ((choices (%field-choices f)))
                             (when choices (list (cons :x-choices choices)))))))))))
    `((:type . "object")
      (:table . ,table)
      (:properties . ,props)
      ,@(when required `((:required . ,required))))))

  (defun %oapi-list-response (schema-ref)
  "Réponse 200 pour list: array pur OU objet {data,total,limit,page}."
  `((:description . "OK")
    (:content .
      (("application/json" .
         ((:schema .
            ((:oneOf .
               (((:type . "array")       ; ex: API retourne un array
                 (:items . ((:$ref . ,schema-ref))))
                ((:type . "object")      ; ex: API retourne {data,total,...}
                 (:properties .
                   ((:data  . ((:type . "array") (:items . ((:$ref . ,schema-ref)))))
                    (:total . ((:type . "integer")))
                    (:limit . ((:type . "integer")))
                    (:page  . ((:type . "integer")))))
                 (:required . ("data")))))))))))))

(defun %oapi-paths-for-entity (rec)
  (let* ((entity (getf rec :entity))
         (seg    (getf rec :segment))
         (base   (getf rec :base-list))
         (item   (getf rec :base-item))
         (name   (string-downcase (symbol-name entity)))
         (ref    (format nil "#/components/schemas/~a" name))
         (tag    (string-capitalize name)))
    (list
     (cons base
           `((:get .
               ((:summary . ,(format nil "Lister ~a" seg))
                (:tags . (,tag))
                (:security . ((("BearerAuth" . ()))) )
                (:parameters . ,(%oapi-list-params-for-entity entity))
                (:responses . ((:200 . ,(%oapi-list-response ref))))))
             (:post .
               ((:summary . ,(format nil "Créer ~a" seg))
                (:tags . (,tag))
                (:security . ((("BearerAuth" . ()))) )
                (:requestBody . ((:required . t)
                                 (:content . (("application/json" . ((:schema . ((:$ref . ,ref)))))))))
                (:responses . ((:201 . ((:description . "Created")))))))))
     (cons item
           `((:get .
               ((:summary . ,(format nil "Lire ~a par id" seg))
                (:tags . (,tag))
                (:security . ((("BearerAuth" . ()))) )
                (:parameters . (((:name . "id") (:in . "path") (:required . t) (:schema . ((:type . "string"))))))
                (:responses . ((:200 . ((:description . "OK")
                                        (:content . (("application/json" . ((:schema . ((:$ref . ,ref)))))))))
                               (:404 . ((:description . "Not Found")))))))
             (:patch .
               ((:summary . ,(format nil "Mettre à jour ~a (PATCH)" seg))
                (:tags . (,tag))
                (:security . ((("BearerAuth" . ()))) )
                (:parameters . (((:name . "id") (:in . "path") (:required . t) (:schema . ((:type . "string"))))
                                ((:name . "If-Match") (:in . "header") (:required . nil) (:schema . ((:type . "string"))))
                                ((:name . "If-Unmodified-Since") (:in . "header") (:required . nil) (:schema . ((:type . "string"))))))
                (:requestBody . ((:required . t)
                                 (:content . (("application/json" . ((:schema . ((:$ref . ,ref))))))))
                )
                (:responses . ((:200 . ((:description . "OK")))
                               (:412 . ((:description . "Precondition Failed")))
                               (:404 . ((:description . "Not Found")))))))
             (:delete .
               ((:summary . ,(format nil "Supprimer ~a" seg))
                (:tags . (,tag))
                (:security . ((("BearerAuth" . ()))) )
                (:parameters . (((:name . "id") (:in . "path") (:required . t) (:schema . ((:type . "string"))))
                                ((:name . "If-Match") (:in . "header") (:required . nil) (:schema . ((:type . "string"))))
                                ((:name . "If-Unmodified-Since") (:in . "header") (:required . nil) (:schema . ((:type . "string"))))))
                (:responses . ((:200 . ((:description . "OK")))
                               (:404 . ((:description . "Not Found"))))))))))))

(defun %custom-route->openapi-path (r)
  "Convertit une route custom en entrée (:path . (:method . def))"
  (let* ((path (getf r :path))
         (method (intern (string-downcase (getf r :method)) :keyword))
         (summary (getf r :summary))
         (tag (getf r :tag))
         (security (getf r :security))
         (params (getf r :params)) ;; List: (name type location)

         ;; Séparation des params (Path/Query vs Body)
         (path-query-params  
          (loop for (name ptype loc) in params
                when (member loc '(:path :query :header))
                collect `((:name . ,name)
                          (:in . ,(string-downcase (symbol-name loc)))
                          (:required . ,(eq loc :path)) ;; Path toujours required
                          (:schema . ((:type . ,(string-downcase (symbol-name (if (eq ptype :uuid) :string ptype)))))))))
         
         (body-params 
           (loop for (name ptype loc) in params
                 when (eq loc :body)
                 collect (cons name ptype))))

    (print params)
    (print path-query-params)
    (print body-params)
    ;; Construction de l'objet opération
    (list (cons path
                (list (cons method
                            `((:summary . ,summary)
                              ,@(when tag `((:tags . (,tag))))
                              ,@(when security `((:security . ,security)))
                              ,@(when path-query-params `((:parameters . ,path-query-params)))
                              ;; Gestion simplifiée du Body (JSON object plat)
                              ,@(when body-params
                                  `((:requestBody 
                                     . ((:content 
                                         . (("application/json" 
                                             . ((:schema 
                                                 . ((:type . "object")
                                                    (:properties 
                                                     . ,(loop for (n . ptype) in body-params
                                                              collect (list (cons n `((:type . "string")))))) ;; Simplification type
                                                    ))))))))))
                              (:responses . ((:200 . ((:description . "OK"))))))))))))

;;; Document OpenAPI depuis le registry (et sécurité Bearer)
(defun %oapi-security-schemes ()
  '((:securitySchemes .
     ((:BearerAuth .
       ((:type . "http")
        (:scheme . "bearer")
        (:bearerFormat . "JWT")))))))

(defun %openapi-document (&key title version servers extra-paths extra-components)
  (let* ((crud-paths (let ((acc (make-hash-table :test 'equal)))
                  (maphash
                   (lambda (_k v)
		     (declare (ignore _k))
                     (dolist (pair (%oapi-paths-for-entity v))
                       (setf (gethash (car pair) acc) (cdr pair))))
                   *crud-registry*)
                  (let (out)
                    (maphash (lambda (k v) (push (cons k v) out)) acc)
                    (sort out #'string< :key #'car))))
	 ;; Conversion des routes custom
         (custom-paths (mapcan #'%custom-route->openapi-path *custom-route-registry*))
         (components-schemas
           (let (alist)
             (maphash
              (lambda (_k v)
		(declare (ignore _k))
		(let* ((ent  (getf v :entity))
                       (name (string-downcase (symbol-name ent))))
                  (push (cons name (%oapi-components-schema ent)) alist)))
              *crud-registry*)
             (nreverse alist)))
         ;; ⬇️ on construit directement l’alist :schemas puis on y "append" la sécurité
         (comps (append
                      (list (cons :schemas components-schemas))
                      (%oapi-security-schemes)))
	 ;; Fusion CRUD + Custom + Extra
         (all-paths (%merge-alists crud-paths 
                                   (%merge-alists custom-paths extra-paths)))
	 ;;(paths*  (if extra-paths      (%merge-alists paths extra-paths) paths))
         (comps*  (if extra-components (%merge-alists comps extra-components) comps)))
    `((:openapi . "3.1.0")
      (:info . ((:title . ,(or title "Lumen API"))
                (:version . ,(or version "1.0.0"))))
      ,@(when servers
          `((:servers . ,(mapcar (lambda (url) (list (cons :url url))) servers))))
      (:paths . ,all-paths)
      (:components . ,comps*)
      (:security . (((:BearerAuth . ())))))))

;; PATCH de la fonction interne utilisée par mount-discovery!
(defun %all-routes-registry ()
  (append (%crud-registry->routes)          ;; Les CRUDs
          (%custom-routes->viewer-format))) ;; Les Customs

;;; --------------------------------------------------------------------------
;;; Montages
;;; --------------------------------------------------------------------------

;; Étend mount-crud! pour enregistrer dans *crud-registry*
(defmethod mount-crud! :around (entity &key base name order-whitelist auth-guard)
  (declare (ignore auth-guard base))
  (let* ((res (call-next-method))
         (md  (entity-metadata entity))
         (table (getf md :table))
         (seg   (or name table))
         (pk    (or (getf md :primary-key) :id))
         (base-list (first res))
         (base-item (second res)))
    (%register-crud entity table seg base-list base-item pk order-whitelist)
    res))

(defun mount-discovery! (&key (routes-path "/_routes") (openapi-path "/openapi.json")
                           (title "Lumen API") (version "1.0.0") (servers nil)
			   extra-paths extra-components)
  "Monte:
  - GET /_routes       → inventaire des endpoints CRUD
  - GET /openapi.json  → document OpenAPI 3.1 (paths + components/schemas) "
  (defroute GET routes-path (ctx)
    (let ((routes (make-hash-table :test #'equal)))
      (setf (gethash "routes" routes) (%all-routes-registry))
    (respond-json routes)))
  (defroute GET openapi-path (ctx)
    (let* ((doc (%openapi-document :title title :version version :servers servers
                                   :extra-paths extra-paths
                                   :extra-components extra-components))
           (raw (with-output-to-string (s) (prin1 doc s)))
           (etag (format nil "W/\"~a\""
			 (subseq (ironclad:byte-array-to-hex-string
				  (ironclad:digest-sequence
				   :sha1 (babel:string-to-octets raw)))
				 0 16))))
      (respond-json doc :headers (list (cons "ETag" etag)))))
  (list routes-path openapi-path))
