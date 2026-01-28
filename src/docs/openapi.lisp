(in-package :cl)

(defpackage :lumen.docs.openapi
  (:use :cl)
  (:import-from :lumen.core.router :construct-route)
  (:import-from :lumen.core.http :respond-json)
  (:import-from :lumen.data.dao 
                :entity-metadata :entity-fields :entity-table)
  (:import-from :lumen.dev.module 
                :get-modules 
                :module-meta-routes 
                :module-meta-resources 
                :module-meta-path-prefix)
  (:export :mount-discovery!))

(in-package :lumen.docs.openapi)

;;; ============================================================================
;;; 1. HELPERS D'INTROSPECTION (Récupérés de votre version Legacy)
;;; ============================================================================

(defun %kw-col (x)
  (cond ((null x) nil)
        ((keywordp x) x)
        ((symbolp x) (intern (string-upcase (symbol-name x)) :keyword))
        ((stringp x) (intern (string-upcase x) :keyword))
        (t nil)))

(defun %field-label (field)
  (or (getf field :label)
      (string-capitalize (string-downcase (symbol-name (getf field :col))))))

(defun %field-hidden-p (field) (eq (getf field :hidden?) t))

(defun %oapi-type+format (ftype)
  (cond
    ((and (consp ftype) (eq (first ftype) :array)) (cons "array" nil))
    ((member ftype '(:integer :bigint))            (cons "integer" nil))
    ((eq ftype :boolean)                           (cons "boolean" nil))
    ((member ftype '(:timestamptz :timestamp))     (cons "string" "date-time"))
    ((eq ftype :date)                              (cons "string" "date"))
    ((eq ftype :time)                              (cons "string" "time"))
    ((eq ftype :uuid)                              (cons "string" "uuid"))
    ((eq ftype :jsonb)                             (cons "object" nil)) ;; Schema = Object
    (t                                             (cons "string" nil))))

(defun %oapi-type+format-for-param (ftype)
  "Pour les query params, jsonb devient string."
  (if (eq ftype :jsonb) 
      (cons "string" nil) 
      (%oapi-type+format ftype)))

(defun %enum-schema-from-choices (choices)
  (when (and choices (listp choices))
    (let ((vals (mapcar #'car choices))
          (labs (mapcar #'cdr choices)))
      (append `((:enum . ,vals))
              (when (some #'identity labs) `((:x-enumNames . ,labs)))))))

(defun %oapi-x-ui (field)
  "Génère les métadonnées x-ui attendues par le Frontend QALM."
  (let ((ui (getf field :ui))
        (label (%field-label field)))
    (remove nil
            `((:label . ,label)
              ,@(when ui
                  (mapcan (lambda (k)
                            (let ((v (getf ui k)))
                              (when v (list (cons k v)))))
                          '(:index? :formatter :width :grow :sortable? :format)))))))

;;; ============================================================================
;;; 2. GÉNÉRATION DES SCHÉMAS (COMPONENTS)
;;; ============================================================================

(defun %oapi-schema-for-entity (entity-sym)
  "Génère le schéma riche (avec x-ui, x-ref, etc.)."
  (let* ((md     (entity-metadata entity-sym))
         (table  (getf md :table))
         (fields (getf md :fields))
         (required (loop for f in fields 
                         when (getf f :required?) 
                         collect (string-downcase (symbol-name (getf f :col)))))
         (props
          (loop for f in fields
                unless (%field-hidden-p f)
                collect
                (let* ((col-s  (string-downcase (symbol-name (getf f :col))))
                       (ftype  (getf f :type))
                       (tf     (%oapi-type+format ftype))
                       (title  (%field-label f))
                       (desc   (getf f :help))
                       (choices (getf f :choices))
                       (xui    (%oapi-x-ui f))
                       (xref   (getf f :references))
                       (xref-c (getf f :ref-col))
                       (xref-f (getf f :ref-format)))
                  
                  (cons col-s
                        (remove nil
                                (append
                                 (list (cons :type (car tf)))
                                 (when (cdr tf) (list (cons :format (cdr tf))))
                                 (when title    (list (cons :title title)))
                                 (when desc     (list (cons :description desc)))
                                 (%enum-schema-from-choices choices)
                                 ;; EXTENSIONS FRONTEND CRITIQUES
                                 (when xui    (list (cons :x-ui xui)))
                                 (when xref   (list (cons :x-ref xref)))
                                 (when xref-c (list (cons :x-ref-col xref-c)))
                                 (when xref-f (list (cons :x-ref-format xref-f)))
                                 (when choices (list (cons :x-choices choices))))))))))
    
    `((:type . "object")
      (:title . ,(string-downcase (symbol-name entity-sym)))
      (:x-table . ,table)
      (:properties . ,props)
      ,@(when required `((:required . ,required))))))

(defun %collect-components-schemas ()
  (let ((schemas (make-hash-table :test 'equal)))
    (dolist (mod (get-modules))
      ;; On parcourt les entités déclarées dans les modules
      ;; module-meta-entities retourne une liste de symboles (ex: ACTION)
      (dolist (ent-sym (lumen.dev.module:module-meta-entities mod))
        (setf (gethash (string-downcase (symbol-name ent-sym)) schemas)
              (%oapi-schema-for-entity ent-sym))))
    
    (let (out) (maphash (lambda (k v) (push (cons k v) out)) schemas) out)))

;;; ============================================================================
;;; 3. GÉNÉRATION DES PARAMÈTRES DE FILTRE (q[...])
;;; ============================================================================

(defun %oapi-make-param (name in type &key format description required)
  `((:name . ,name) (:in . ,in)
    ,@(when required '((:required . t)))
    ,@(when description `((:description . ,description)))
    (:schema . ,(append (list (cons :type type))
                        (when format (list (cons :format format)))))))

(defun %oapi-filter-params-for-field (field)
  "Reconstruction exacte de votre logique de filtres q[]."
  (let* ((col    (getf field :col))
         (ftype  (getf field :type))
         (choices (getf field :choices))
         (nm     (string-downcase (symbol-name col)))
         (label  (%field-label field))
         (tf     (%oapi-type+format-for-param ftype))
         (base   (format nil "q[~a]" nm))
         
         (range-p (or (member ftype '(:integer :bigint))
                      (member ftype '(:date :time :timestamptz :timestamp)))))
    
    (let ((p-eq 
            `((:name . ,base) (:in . "query")
              (:description . ,(format nil "Egalité pour ~A (~A)" nm label))
              (:schema . ,(append (list (cons :type (car tf)))
                                  (when (cdr tf) (list (cons :format (cdr tf))))
                                  (%enum-schema-from-choices choices))))))
      
      (remove nil
              (list p-eq
                    (when range-p
                      `((:name . ,(format nil "q[min_~a]" nm)) (:in . "query")
                        (:schema . ((:type . ,(car tf)) ,@(when (cdr tf) `((:format . ,(cdr tf))))))))
                    (when range-p
                      `((:name . ,(format nil "q[max_~a]" nm)) (:in . "query")
                        (:schema . ((:type . ,(car tf)) ,@(when (cdr tf) `((:format . ,(cdr tf))))))))
                    `((:name . ,(format nil "q[in_~a]" nm)) (:in . "query")
                      (:schema . ((:type . "string"))))))))) ;; CSV

(defun %oapi-list-params-for-entity (entity-sym)
  "Concatène params communs + q[...] spécifiques."
  (let* ((md     (entity-metadata entity-sym))
         (fields (getf md :fields))
         ;; TODO: Ajouter timestamps synthétiques si besoin comme dans votre code legacy
         (per-field (mapcan #'%oapi-filter-params-for-field fields)))
    (append 
     (list (%oapi-make-param "page" "query" "integer")
           (%oapi-make-param "limit" "query" "integer")
           (%oapi-make-param "order" "query" "string")
           (%oapi-make-param "q[search]" "query" "string"))
     per-field)))

;;; ============================================================================
;;; 4. GÉNÉRATION DES PATHS (CRUD + CUSTOM)
;;; ============================================================================

(defun %oapi-crud-paths (mod entity-sym)
  "Génère les 5 endpoints CRUD standards pour une entité donnée."
  (let* ((md        (entity-metadata entity-sym))
         (table     (getf md :table))
         (path-pfx  (lumen.dev.module:module-meta-path-prefix mod))
         ;; On assume que mount-crud! utilise le nom de la table ou du symbole
         (seg       (or table (string-downcase (symbol-name entity-sym))))
         (base-url  (format nil "~a/~a" (string-right-trim "/" path-pfx) seg))
         (item-url  (format nil "~a/{id}" base-url))
         (ref       (format nil "#/components/schemas/~a" (string-downcase (symbol-name entity-sym))))
         (tag       (string-capitalize seg)))

    (list
     ;; Collection: GET / POST
     (cons base-url
           `((:get . ((:summary . ,(format nil "Lister ~a" seg))
                      (:tags . (,tag))
                      (:security . (((:BearerAuth . ()))))
                      (:parameters . ,(%oapi-list-params-for-entity entity-sym))
                      (:responses . ((:200 . ((:description . "OK") 
                                              (:content . (("application/json" . 
                                                            ((:schema . ((:type . "array") 
                                                                         (:items . ((:$ref . ,ref)))))))))))))))
             (:post . ((:summary . ,(format nil "Créer ~a" seg))
                       (:tags . (,tag))
                       (:security . (((:BearerAuth . ()))))
                       (:requestBody . ((:required . t)
                                        (:content . (("application/json" . ((:schema . ((:$ref . ,ref)))))))))
                       (:responses . ((:201 . ((:description . "Created")))))))))
     
     ;; Item: GET / PATCH / DELETE
     (cons item-url
           `((:get . ((:summary . ,(format nil "Voir ~a" seg))
                      (:tags . (,tag))
                      (:security . (((:BearerAuth . ()))))
                      (:parameters . (((:name . "id") (:in . "path") (:required . t) (:schema . ((:type . "string"))))))
                      (:responses . ((:200 . ((:description . "OK")
                                              (:content . (("application/json" . ((:schema . ((:$ref . ,ref)))))))))))))
             (:patch . ((:summary . ,(format nil "Modifier ~a" seg))
                        (:tags . (,tag))
                        (:security . (((:BearerAuth . ()))))
                        (:parameters . (((:name . "id") (:in . "path") (:required . t) (:schema . ((:type . "string"))))))
                        (:requestBody . ((:required . t)
                                         (:content . (("application/json" . ((:schema . ((:$ref . ,ref)))))))))
                        (:responses . ((:200 . ((:description . "Updated")))))))
             
             (:delete . ((:summary . ,(format nil "Supprimer ~a" seg))
                         (:tags . (,tag))
                         (:security . (((:BearerAuth . ()))))
                         (:parameters . (((:name . "id") (:in . "path") (:required . t) (:schema . ((:type . "string"))))))
                         (:responses . ((:200 . ((:description . "Deleted"))))))))))))

(defun %convert-custom-route (r)
  "Convertit la métadonnée brute de defmodule en PathItem."
  (destructuring-bind (&key method path summary params tag &allow-other-keys) r
    (let ((method-kw (intern (string-downcase method) :keyword))
          ;; Reconstruction basique des paramètres si présents
          (oapi-params (loop for (name type loc) in params
                             when (member loc '(:path :query))
                             collect `((:name . ,name)
                                       (:in . ,(string-downcase (symbol-name loc)))
                                       (:required . ,(eq loc :path))
                                       (:schema . ((:type . "string")))))))
      
      (cons path 
            (list (cons method-kw 
                        `((:summary . ,summary)
                          (:tags . ,(if tag (list tag) '("Custom")))
                          (:security . (((:BearerAuth . ()))))
                          ,@(when oapi-params `((:parameters . ,oapi-params)))
                          (:responses . ((:200 . ((:description . "OK"))))))))))))

(defun %collect-paths ()
  (let ((paths (make-hash-table :test 'equal)))
    (dolist (mod (get-modules))
      ;; 1. ROUTES CRUD (Générées virtuellement à partir des entités)
      (dolist (res-sym (lumen.dev.module:module-meta-resources mod))
        (dolist (pair (%oapi-crud-paths mod res-sym))
          (let ((existing (gethash (car pair) paths)))
            (setf (gethash (car pair) paths) (append existing (cdr pair))))))
      
      ;; 2. ROUTES CUSTOM (Stockées dans les métadonnées)
      (dolist (r (lumen.dev.module:module-meta-routes mod))
        (let* ((pair (%convert-custom-route r))
               (existing (gethash (car pair) paths)))
          (setf (gethash (car pair) paths) (append existing (cdr pair))))))
    
    (let (out) (maphash (lambda (k v) (push (cons k v) out)) paths) 
         (sort out #'string< :key #'car))))

;;; ============================================================================
;;; 5. MOUNT
;;; ============================================================================

(defun mount-discovery! (&key (routes-path "/_routes") (openapi-path "/openapi.json")
                              (title "Lumen API") (version "1.0.0") (servers nil))
  (list
   (construct-route :GET openapi-path (req)
     (let ((doc `((:openapi . "3.1.0")
                  (:info . ((:title . ,title) (:version . ,version)))
                  ,@(when servers `((:servers . ,(mapcar (lambda (u) `((:url . ,u))) servers))))
                  (:paths . ,(%collect-paths))
                  (:components . ((:schemas . ,(%collect-components-schemas))
                                  (:securitySchemes . ((:BearerAuth . ((:type . "http") (:scheme . "bearer") (:bearerFormat . "JWT")))))))
                  (:security . (((:BearerAuth . ()))) ))))
       (respond-json doc)))))
