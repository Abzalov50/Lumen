(in-package :cl)

(defpackage :lumen.utils
  (:use :cl :alexandria)
  (:export :copy-plist :-> :->> :probe-directory :col-get :alist-fuzzy-get
	   ;; strings
           :str-prefix-p :str-suffix-p :str-contains-p
	   :ensure-trailing :ensure-leading
   :ends-with-slash-p :starts-with-slash-p :%trim
   :join-lines
	   ;; Query string
	   :url-decode-qs
	   ;; alists
	   :alist-get :alist-set :ensure-header :plist-put
	   ;; HTTP Dates
   :parse-http-date :format-http-date :current-db-time-string
   ;; Crypto
   :hmac-sha256 :gen-uuid-string
   :subsetp-list :to-snake-case
   :secure-uuid-equal))

(in-package :lumen.utils)

(defun copy-plist (plist)
  "Safe copy of a property list (just a shallow copy)."
  (copy-list plist))

(defmacro -> (x &rest forms)
  "Thread-first macro. Ex:
   (-> '() (cons 1) (cons 2))
   => (cons 2 (cons 1 '()))"
  (reduce (lambda (acc form)
            (if (listp form)
                `(,(car form) ,acc ,@(cdr form))
                `(,form ,acc)))
          forms
          :initial-value x))

(defmacro ->> (x &rest forms)
  "Thread-last macro."
  (reduce (lambda (acc form)
            (if (listp form)
                `(,(car form) ,@(cdr form) ,acc)
                `(,form ,acc)))
          forms
          :initial-value x))

(defun probe-directory (pathname)
  "Retourne un pathname de répertoire s'il existe, sinon NIL."
  (let ((pn (ignore-errors (probe-file pathname))))
    (when (and pn (uiop:directory-pathname-p pn))
      pn)))

;;; -------------------------------
;;; Chaînes (primitives explicites)
;;; -------------------------------

(defun str-prefix-p (prefix s &key (test #'char=))
  "Vrai si S commence par PREFIX. TEST compare les caractères."
  (declare (type string prefix s))
  (let* ((lp (length prefix))
         (ls (length s)))
    (and (<= lp ls)
         (null (mismatch prefix s :end1 lp :end2 lp :test test)))))

(defun str-suffix-p (suffix s &key (test #'char=))
  "Vrai si S se termine par SUFFIX. TEST compare les caractères."
  (declare (type string suffix s))
  (let* ((lf (length suffix))
         (ls (length s)))
    (and (<= lf ls)
         (null (mismatch suffix s :start2 (- ls lf) :test test)))))

(defun str-prefix-ci-p (prefix s)
  (str-prefix-p (string-downcase prefix) (string-downcase s)))

(defun str-suffix-ci-p (suffix s)
  (str-suffix-p (string-downcase suffix) (string-downcase s)))

(defun str-contains-p (needle haystack &key (test #'char=))
  "Vrai si HAYSTACK contient NEEDLE."
  (declare (type string needle haystack))
  (not (null (search needle haystack :test test))))

(defun ensure-trailing (s ch)
  "Ajoute CH à la fin de S si absent (retourne une nouvelle string)."
  (declare (type string s))
  (if (and (> (length s) 0) (char= (char s (1- (length s))) ch))
      s
      (concatenate 'string s (string ch))))

(defun ensure-leading (s ch)
  "Ajoute CH au début de S si absent."
  (declare (type string s))
  (if (and (> (length s) 0) (char= (char s 0) ch))
      s
      (concatenate 'string (string ch) s)))

(defun ends-with-slash-p (s) (str-suffix-p "/" s))
(defun starts-with-slash-p (s) (str-prefix-p "/" s))

;;; -------------------------------
;;; Alists (headers, params…)
;;; -------------------------------

(defun alist-get (alist key &key (test #'string-equal) default)
  "Récupère la première valeur dans une ALIST pour KEY (ou DEFAULT)."
  (let ((cell (assoc key alist :test test)))
    (if cell (cdr cell) default)))

(defun alist-set (alist key value &key (test #'string-equal))
  "Retourne une nouvelle ALIST avec KEY=VALUE (remplace si présent)."
  (let ((lname key) (res '()) (replaced nil))
    (dolist (cell alist (nreverse (if replaced res (cons (cons lname value) res))))
      (if (and (not replaced) (funcall test (car cell) lname))
          (progn (push (cons lname value) res) (setf replaced t))
          (push cell res)))))

(defun ensure-header (headers name value)
  "Insère/remplace (name . value) dans une ALIST de headers (noms en minuscules)."
  (alist-set headers (string-downcase name) value :test #'string=))

(defun %trim (s) (string-trim '(#\Space #\Tab #\Newline #\Return) s))

(defun url-decode-qs (s)
  (when s
    (with-output-to-string (out)
      (loop for i from 0 below (length s) do
            (let ((c (char s i)))
              (cond
                ((char= c #\+) (write-char #\Space out))
                ((and (char= c #\%)
                      (<= (+ i 2) (1- (length s))))
                 (let ((h1 (digit-char-p (char s (1+ i)) 16))
                       (h2 (digit-char-p (char s (+ i 2)) 16)))
                   (if (and h1 h2)
                       (progn (write-char (code-char (+ (* h1 16) h2)) out)
                              (incf i 2))
                       (write-char c out))))
                (t (write-char c out))))))))

;; ----- format "IMF-fixdate" RFC 7231 (ex: "Sun, 06 Nov 1994 08:49:37 GMT") -----
(defparameter +weekday-names+ #("Sun" "Mon" "Tue" "Wed" "Thu" "Fri" "Sat"))
(defparameter +month-names+   #("Jan" "Feb" "Mar" "Apr" "May" "Jun" "Jul" "Aug" "Sep" "Oct" "Nov" "Dec"))

(defun format-http-date (universal-time)
  "Universal-time -> string RFC 1123 en GMT."
  (multiple-value-bind (sec min hour day month year dow)
      (decode-universal-time universal-time 0)
    (format nil "~A, ~2,'0D ~A ~4,'0D ~2,'0D:~2,'0D:~2,'0D GMT"
            (aref +weekday-names+ dow)
            day (aref +month-names+ (1- month)) year hour min sec)))

(defun %parse-int (s &optional (start 0) (end (length s)))
  (parse-integer s :start start :end end :junk-allowed nil))

(defun parse-http-date (s)
  "Parse une date HTTP (IMF-fixdate uniquement). Retourne universal-time ou NIL."
  (handler-case
      (let* ((len (length s)))
        (when (and (>= len 29)            ; "Sun, 06 Nov 1994 08:49:37 GMT" = 29
                   (char= (char s 3) #\,) (char= (char s 4) #\Space)
                   (char= (char s 7) #\Space) (char= (char s 11) #\Space)
                   (char= (char s 16) #\Space)
                   (string= (subseq s (- len 3)) "GMT"))
          (let* ((day   (%parse-int s 5 7))
                 (mon-s (subseq s 8 11))
                 (year  (%parse-int s 12 16))
                 (hour  (%parse-int s 17 19))
                 (min   (%parse-int s 20 22))
                 (sec   (%parse-int s 23 25))
                 (month (position mon-s +month-names+ :test #'string=)))
            (when month
              (encode-universal-time sec min hour day (1+ month) year 0)))))
    (error () nil)))

;; Crypto
(defun hmac-sha256 (key bytes)
  (ironclad:hmac-digest (ironclad:make-hmac key :sha256) :buffer bytes))

(defun gen-uuid-string ()
  (format nil "~(~A~)" (uuid:make-v4-uuid)))

(defun plist-put (plist key val)
  "Ajoute (ou met à jour) la valeur VAL associée à KEY dans la plist PLIST.
Retourne la nouvelle plist."
  (let ((pos (position key plist :test #'eq)))
    (if pos
        ;; La clé existe déjà : remplacer la valeur suivante
        (progn
          (setf (nth (1+ pos) plist) val)
          plist)
        ;; Sinon, ajouter à la plist
        (list* key val plist))))

(defun subsetp-list (a b &key (test #'eql))
  "Retourne T si chaque élément de la liste A se trouve dans la liste B, sinon NIL.
   On peut fournir un :test (par défaut eql)."
  (every (lambda (x) (member x b :test test)) a))

(defun join-lines (&rest lines)
  (with-output-to-string (s)
    (dolist (l lines) (write-string l s) (terpri s))))

(defun current-db-time-string ()
  (local-time:format-timestring nil (local-time:now)))

(defun alist-fuzzy-get (alist &rest keys)
  "Recherche la valeur associée à l'une des clés fournies dans une ALIST, 
   en normalisant les noms (insensible à la casse, interchangeabilité _ et -).
   Retourne la première valeur trouvée correspondant à l'une des clés."
   (labels ((norm (k)
             (let ((s (string-downcase (etypecase k
                                         (symbol (symbol-name k))
                                         (string k)))))
               ;; unifier - et _
               (substitute #\_ #\- s))))
    (let* ((table (make-hash-table :test 'equal)))
      (dolist (kv alist)
        (let* ((k (car kv))
               (v (cdr kv))
               (nk (norm k)))
          (setf (gethash nk table) v)))
      (loop for k in keys
            for nk = (norm k)
            thereis (gethash nk table)))) )

(defun col-get (entity key &optional default)
  "Récupère une valeur dans une entité, que ce soit un Objet CLOS ou une Alist."
  (cond
    ;; CAS 1 : C'est un Objet CLOS (instance de classe defentity)
    ((typep entity 'standard-object)
     (let* ((pkg (symbol-package (class-name (class-of entity))))
            ;; On cherche le symbole du slot dans le même package que la classe
            (slot-sym (find-symbol (string key) pkg)))
       (if (and slot-sym (slot-exists-p entity slot-sym))
           (if (slot-boundp entity slot-sym)
               (slot-value entity slot-sym)
               default)
           default)))

    ;; CAS 2 : C'est une liste associative (Alist)
    ((listp entity) (alist-fuzzy-get entity key))

    ;; CAS 3 : Autre (NIL ou erreur)
    (t default)))

(defun to-snake-case (designator)
  "Convertit un designator (Symbole, Keyword ou String) de kebab-case vers snake_case.
   Utilisé pour mapper les noms de slots Lisp vers les colonnes SQL.
   
   Exemples:
     :created-at  -> \"created_at\"
     'user-id     -> \"user_id\"
     \"My-Table\" -> \"my_table\""
  (let ((str (string designator))) ;; 1. Convertit tout (Symbole/Keyword) en string
    (substitute #\_ #\-            ;; 3. Remplace les tirets par des underscores
                (string-downcase str)))) ;; 2. Passe tout en minuscules

(defun to-kebab-keyword (str)
  "Convertit une string snake_case en keyword kebab-case.
   Exemple: \"created_at\" -> :CREATED-AT"
  (let ((up (string-upcase str)))      ;; 1. Majuscules (Convention Lisp)
    (intern                            ;; 3. Crée le keyword interné
     (substitute #\- #\_ up)           ;; 2. Remplace _ par -
     :keyword)))

(defun secure-uuid-equal (u1 u2)
  "Compare deux UUIDs qu'ils soient string, symbol ou vector."
  (let ((s1 (string-downcase (princ-to-string u1)))
        (s2 (string-downcase (princ-to-string u2))))
    (string= s1 s2)))
