(in-package :cl)

(defpackage :lumen.core.validate
  (:use :cl :alexandria)
  (:import-from :cl-ppcre)
  (:export
   :validate                  ; (validate input &body rules) -> (values data errors)
   :permit :coerce :required :string :number :enum :regex :custom))

(in-package :lumen.core.validate)

;;; -------------------------------
;;; Changeset structure
;;; -------------------------------
(defstruct (changeset (:constructor %mk-cs))
  input   ; alist/plist d’entrée
  data    ; alist nettoyée/typée
  errors) ; alist field -> (list messages keywords)

(defun %alist (x)
  "Normalise plist/alist → alist avec clés en keyword downcase."
  (cond
    ((null x) '())
    ((and (listp x) (every #'consp x))
     (mapcar (lambda (kv) (cons (%kw (car kv)) (cdr kv))) x))
    ((and (listp x) (evenp (length x))) ; plist
     (loop for (k v) on x by #'cddr collect (cons (%kw k) v)))
    (t (error "Unsupported map format"))))

(defun %kw (k)
  (cond
    ((keywordp k) k)
    ((symbolp k) (intern (string-upcase (symbol-name k)) :keyword))
    ((stringp k) (intern (string-upcase k) :keyword))
    (t (error "Invalid key ~S" k))))

(defun %get (alist key)
  (cdr (assoc (%kw key) alist)))

(defun %put (alist key val)
  (let* ((kk (%kw key))
         (cell (assoc kk alist)))
    (if cell (progn (setf (cdr cell) val) alist)
        (acons kk val alist))))

(defun %add-error (cs key msg)
  (let* ((errs (changeset-errors cs))
         (kk (%kw key))
         (cell (assoc kk errs)))
    (setf (changeset-errors cs)
          (if cell
              (progn (push msg (cdr cell)) errs)
              (acons kk (list msg) errs)))
    cs))

(defun %ok (cs) cs)

;;; -------------------------------
;;; Règles (fonctions pures → nouveau CS)
;;; -------------------------------
(defun v-permit (cs &rest keys)
  (let* ((in  (%alist (changeset-input cs)))
         (keep (mapcar #'%kw keys))
         (filtered (remove-if-not (lambda (kv) (member (car kv) keep)) in)))
    (setf (changeset-data cs) filtered)
    cs))

(defun %coerce-one (val type)
  (ecase type
    (:int   (etypecase val
              (null nil)
              (integer val)
              (string (parse-integer val :junk-allowed t))))
    (:float (etypecase val
              (null nil)
              (real (coerce val 'double-float))
              (string (let ((n (ignore-errors (read-from-string val))))
                        (and (numberp n) (coerce n 'double-float))))))
    (:bool  (etypecase val
              (null nil)
              (string (let ((s (string-downcase (string-trim '(#\Space) val))))
                        (cond ((member s '("1" "t" "true" "yes" "on") :test #'string=) t)
                              ((member s '("0" "f" "false" "no" "off" "") :test #'string=) nil)
                              (t nil))))
              (integer (not (zerop val)))
              (symbol  (not (eq val nil)))
              (t t)))
    (:string (etypecase val
               (null nil)
               (string val)
               (t (princ-to-string val))))))

(defun v-coerce (cs key type)
  (let* ((src (or (changeset-data cs) (%alist (changeset-input cs))))
         (val (%get src key)))
    (setf (changeset-data cs)
          (%put src key (%coerce-one val type)))
    cs))

(defun v-required (cs &rest keys)
  (let ((src (or (changeset-data cs) (%alist (changeset-input cs)))))
    (dolist (k keys cs)
      (let ((v (%get src k)))
        (when (or (null v)
                  (and (stringp v) (string= (string-trim '(#\Space) v) "")))
          (%add-error cs k :required))))))

(defun v-string (cs key &key min max format)
  (let* ((src (or (changeset-data cs) (%alist (changeset-input cs))))
         (v (%get src key)))
    (when (and v (not (stringp v)))
      (return-from v-string (%add-error cs key :not_a_string)))
    (when (and v min (< (length v) min)) (%add-error cs key :too_short))
    (when (and v max (> (length v) max)) (%add-error cs key :too_long))
    (when (and v format)
      (ecase format
        (:email (unless (cl-ppcre:scan "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$" v)
                  (%add-error cs key :invalid_email)))
        (t (%add-error cs key :invalid_format))))
    cs))

(defun v-number (cs key &key min max)
  (let* ((src (or (changeset-data cs) (%alist (changeset-input cs))))
         (v (%get src key)))
    (when (and (null v)) (return-from v-number cs))
    (unless (numberp v) (return-from v-number (%add-error cs key :not_a_number)))
    (when (and min (< v min)) (%add-error cs key :min))
    (when (and max (> v max)) (%add-error cs key :max))
    cs))

(defun v-enum (cs key values &key (test #'equal))
  (let* ((src (or (changeset-data cs) (%alist (changeset-input cs))))
         (v (%get src key)))
    (when (and v (not (member v values :test test)))
      (%add-error cs key :not_in_enum))
    cs))

(defun v-regex (cs key pattern)
  (let* ((src (or (changeset-data cs) (%alist (changeset-input cs))))
         (v (%get src key)))
    (when (and v (not (cl-ppcre:scan pattern v)))
      (%add-error cs key :invalid_format))
    cs))

(defun v-custom (cs key fn)
  "FN reçoit (value data) et doit renvoyer T/NIL ou (values T/NIL reason-keyword)."
  (let* ((src (or (changeset-data cs) (%alist (changeset-input cs))))
         (v (%get src key)))
    (multiple-value-bind (ok reason) (funcall fn v src)
      (unless ok (%add-error cs key (or reason :invalid))))
    cs))

;;; -------------------------------
;;; DSL (macros) : enchaîne les v-*
;;; -------------------------------
(defmacro validate (input &body rules)
  (let ((cs (gensym "CS")))
    `(let* ((,cs (%mk-cs :input ,input :data nil :errors '())))
       ,@(mapcar (lambda (r)
                   (destructuring-bind (op . args) r
                     (ecase op
                       (permit  `(setf ,cs (v-permit ,cs ,@args)))
                       (coerce  `(setf ,cs (v-coerce ,cs ,@args)))
                       (required `(setf ,cs (v-required ,cs ,@args)))
                       (string  `(setf ,cs (v-string ,cs ,@args)))
                       (number  `(setf ,cs (v-number ,cs ,@args)))
                       (enum    `(setf ,cs (v-enum ,cs ,@args)))
                       (regex   `(setf ,cs (v-regex ,cs ,@args)))
                       (custom  `(setf ,cs (v-custom ,cs ,@args))))))
		 rules)
       (values (changeset-data ,cs) (changeset-errors ,cs)))))
