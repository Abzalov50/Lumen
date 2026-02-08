(defpackage :lumen.core.validation
  (:use :cl)
  (:import-from :cl-ppcre :scan)
  (:export :validate-field))

(in-package :lumen.core.validation)

;;; 1. Primitives de validation
;;; Retournent NIL si OK, ou un message d'erreur (String) si KO.

(defun v-required (val)
  (when (or (null val) (and (stringp val) (zerop (length val))))
    "Ce champ est obligatoire."))

(defun v-type (val type)
  (when val
    (case type
      (:string  (unless (stringp val) "Doit être une chaîne de caractères."))
      (:integer (unless (integerp val) "Doit être un nombre entier."))
      (:float   (unless (numberp val) "Doit être un nombre."))
      (:boolean (unless (typep val 'boolean) "Doit être un booléen (true/false)."))
      (:email   (unless (and (stringp val) (scan "^[^@]+@[^@]+\\.[^@]+$" val)) 
                  "Format email invalide."))
      (:uuid    (unless (and (stringp val) (= 36 (length val))) 
                  "Format UUID invalide.")))))

(defun v-min (val n type)
  (declare (ignore type))
  (when val
    (cond ((stringp val) 
           (when (< (length val) n) (format nil "Minimum ~A caractères." n)))
          ((numberp val)
           (when (< val n) (format nil "Valeur minimale : ~A." n))))))

(defun v-max (val n type)
  (declare (ignore type))
  (when val
    (cond ((stringp val) 
           (when (> (length val) n) (format nil "Maximum ~A caractères." n)))
          ((numberp val)
           (when (> val n) (format nil "Valeur maximale : ~A." n))))))

(defun v-choices (val choices)
  (when val
    ;; choices est une liste de pairs (val . label) ou juste val
    (let ((allowed (mapcar (lambda (c) (if (consp c) (car c) c)) choices)))
      (unless (member val allowed :test #'equal)
        "Valeur non autorisée."))))

(defun v-pattern (val regex)
  (when (and val (stringp val))
    (unless (cl-ppcre:scan regex val)
      "Format invalide (ne correspond pas au motif requis).")))

;;; 2. Le Cœur : Validate Payload
(defun %run-custom-validator (fun value)
  "Exécute un validateur custom (symbole ou fonction)."
  (cond
    ((null fun) nil) ;; Pas d'erreur
    ((functionp fun) (funcall fun value))
    ((and (symbolp fun) (fboundp fun))
     (funcall (symbol-function fun) value))
    (t (error "Validateur invalide : ~S" fun))))

(defun validate-field (value field-def)
  "Vérifie une valeur par rapport à la définition d'un champ defentity.
   Retourne une liste de strings (erreurs)."
  (let ((errors '())
        (type (getf field-def :type))
        (req? (getf field-def :required?)))
    
    ;; 1. Required
    (when req?
      (let ((err (v-required value)))
        (when err (push err errors))))
    
    ;; Si vide et non requis, on arrête là
    (when (and (null value) (not req?))
      (return-from validate-field nil))

    ;; 2. Type
    (let ((err (v-type value type)))
      (when err (push err errors)))

    ;; 3. Min/Max
    (let ((min (getf field-def :min))
          (max (getf field-def :max)))
      (when min (let ((e (v-min value min type))) (when e (push e errors))))
      (when max (let ((e (v-max value max type))) (when e (push e errors)))))

    ;; 4. Pattern / Choices
    (let ((pat (getf field-def :pattern))
          (cho (getf field-def :choices)))
      (when pat (let ((e (v-pattern value pat))) (when e (push e errors))))
      (when cho (let ((e (v-choices value cho))) (when e (push e errors)))))

    ;; 5. Validateur Custom (Fonction Lisp)
    (let ((custom (getf field-def :validator)))
      (when custom
	;; Le validateur custom doit retourner NIL si OK, ou String si Erreur
	;; (Attention: ton ancien code retournait T si OK, on adapte ici)
	(let ((res (%run-custom-validator custom value)))
          ;; Si le résultat est une chaîne, c'est une erreur explicite
          (when (stringp res) (push res errors))
          ;; Si le résultat est NIL (et qu'on attendait T dans l'ancien système),
          ;; il faudra adapter tes validateurs existants pour qu'ils retournent NIL quand tout va bien
          ;; ou inverser la logique ici.
          ;; ADAPTATION STANDARD LUMEN 2.0 :
          ;; - Validateur retourne NIL = OK
          ;; - Validateur retourne "Message" = ERREUR
          )))

    (nreverse errors)))
