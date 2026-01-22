(in-package :cl)

(defpackage :lumen.extras.forms
  (:use :cl)
  (:import-from :lumen.data.dao
    :entity-metadata :entity-fields)
  (:export :schema))

(in-package :lumen.extras.forms)

;;; ---------------------------
;;; Helpers: noms & libellés
;;; ---------------------------

(defun %stringify (x)
  (etypecase x
    (string x)
    (symbol (string-downcase (symbol-name x)))))

(defun %label-of (field)
  (or (getf field :label)
      (string-capitalize (%stringify (getf field :col)))))

(defun %json-type-format (type)
  "Mappe le type Lumen/DB vers {json-type, format?, extras?}."
  (cond
    ;; tableaux: (:array :text) etc.
    ((and (consp type) (eq (first type) :array))
     (let* ((inner (second type))
            (inner-mapping (%json-type-format inner)))
       (values "array" nil
               `((:items . ((:type . ,(first inner-mapping))
                            ,@(when (second inner-mapping)
                                (list (cons :format (second inner-mapping))))))))))
    ((member type '(:text :varchar :uuid :jsonb :timestamptz :timestamp :integer
                          :bigint :boolean :date :time :email :url)
             :test #'eq)
     (ecase type
       (:text        (values "string" nil nil))
       (:varchar     (values "string" nil nil))
       (:uuid        (values "string" "uuid" nil))
       (:jsonb       (values "object" nil '((:lumen/json . t))))
       (:timestamptz (values "string" "date-time" nil))
       (:timestamp   (values "string" "date-time" nil))
       (:integer     (values "integer" nil nil))
       (:bigint      (values "integer" nil nil))
       (:boolean     (values "boolean" nil nil))
       (:date        (values "string" "date" nil))
       (:time        (values "string" "time" nil))
       (:email       (values "string" nil nil))
       (:url         (values "string" "uri" nil))))
    (t
     (values "string" nil nil))))

(defun %ui-widget (field)
  "Détermine le widget UI par défaut."
  (let* ((itype (getf field :input-type))
         (type  (getf field :type)))
    (cond
      ((getf field :hidden?)    "hidden")
      ((eq itype :email)        "email")
      ((eq type :jsonb)         "textarea")
      ((member type '(:timestamptz :timestamp) :test #'eq) "datetime")
      (t                        nil))))

(defun %prop-entry (field)
  "Construit la propriété JSON Schema (alist) pour un champ."
  (multiple-value-bind (jtype jformat extras) (%json-type-format (getf field :type))
    (let ((prop `((:type . ,jtype)
                  ,@(when jformat `((:format . ,jformat)))
                  ,@(when (getf field :readonly?) '((:readonly . t)))
                  ,@(when (getf field :disabled?) '((:disabled . t)))
                  ,@(when (getf field :hidden?)   '((:lumen/hidden . t)))
                  ;; bornes/regex/choices/placeholder/help éventuels
                  ,@(when (getf field :min)       `((:minimum . ,(getf field :min))))
                  ,@(when (getf field :max)       `((:maximum . ,(getf field :max))))
                  ,@(when (getf field :pattern)   `((:pattern . ,(getf field :pattern))))
                  ,@(when (getf field :choices)
                      `((:enum . ,(mapcar (lambda (pair) (car pair))
                                          (getf field :choices)))
                        (:enumNames . ,(mapcar (lambda (pair) (cdr pair))
                                               (getf field :choices)))))
                  ,@(when extras extras))))
      prop)))

(defun %ui-entry (field)
  "Construit la partie uiSchema pour un champ (pair (name . ui-props))."
  (let* ((name  (%stringify (getf field :col)))
         (title (%label-of field))
         (widget (%ui-widget field))
         (ui `((:|ui:title| . ,title)
               ,@(when (getf field :placeholder)
                   `((:|ui:placeholder| . ,(getf field :placeholder))))
               ,@(when (getf field :help)
                   `((:|ui:help| . ,(getf field :help))))
               ,@(when widget
                   `((:|ui:widget| . ,widget)))
               ,@(when (getf field :readonly?)
                   '((:|ui:readonly| . t)))
               ,@(when (getf field :disabled?)
                   '((:|ui:disabled| . t))))))
    (cons name ui)))

(defun %required-list (fields)
  (loop for f in fields
        when (getf f :required?)
          collect (%stringify (getf f :col))))

;;; --------------------------------
;;; Entrée principale: (schema 'X)
;;; --------------------------------

(defun schema (entity)
  "Retourne plist :json-schema, :ui, :meta pour ENTITY."
  (let* ((md      (entity-metadata entity))
         (fields  (entity-fields entity))
         (title   (or (and md (string-capitalize (%stringify (getf md :table))))
                      (string-capitalize (%stringify entity))))
         ;; PROPERTIES
         (props   (loop for f in fields
                        for name = (%stringify (getf f :col))
                        for prop = (%prop-entry f)
                        collect (cons name prop)))
         ;; REQUIRED
         (required (%required-list fields))
         ;; UI
         (order    (mapcar (lambda (f) (%stringify (getf f :col)))
                           fields))
         (ui-map   (loop for f in fields collect (%ui-entry f)))
         (json-schema
           `((:title . ,title)
             (:type . "object")
             (:properties . ,props)
             ,@(when required `((:required . ,required)))))
         (ui
           ;; uiSchema = alist: (:|ui:order| ...) + ("field" . (ui-props)) ...
           (append (list (cons :|ui:order| order))
                   ui-map))
         (meta
           `(:entity ,(%stringify entity)
             :table  ,(%stringify (getf md :table))
             :primary-key ,(%stringify (getf md :primary-key)))))
    (list :json-schema json-schema
          :ui          ui
          :meta        meta)))
