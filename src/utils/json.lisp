(in-package :cl)

(defpackage :lumen.utils.json
  (:use :cl)
  (:export :parse))

(in-package :lumen.utils.json)

(defun %trim (s)
  (string-trim '(#\Space #\Tab #\Newline #\Return) s))

(defun %maybe-number (s)
  (ignore-errors (parse-integer s :junk-allowed nil)))

(defun %maybe-bool/null (s)
  (cond
    ((string-equal s "true") t)
    ((string-equal s "false") nil)
    ((string-equal s "null") nil)
    (t :no-match)))

(defun %parse-value (s)
  (let ((s (%trim s)))
    (cond
      ;; string
      ((and (>= (length s) 2)
            (char= (char s 0) #\")
            (char= (char s (1- (length s))) #\"))
       (subseq s 1 (1- (length s))))
      ;; number
      ((%maybe-number s))
      ;; boolean / null
      ((not (eq (%maybe-bool/null s) :no-match)) (%maybe-bool/null s))
      ;; fallback → renvoyer brut
      (t s))))

(defun %split-array (s)
  "Découpe un JSON array '[...]' en éléments bruts (strings)."
  (let* ((inner (subseq s 1 (1- (length s))))
         (raws (uiop:split-string inner :separator ",")))
    (mapcar #'%trim raws)))

(defun parse (s)
  "Petit parseur JSON: gère valeurs simples + tableaux de base."
  (let ((s (%trim s)))
    (cond
      ((and (plusp (length s)) (char= (char s 0) #\[))
       (mapcar #'%parse-value (%split-array s)))
      (t (%parse-value s)))))
