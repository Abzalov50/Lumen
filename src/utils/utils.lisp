(in-package :cl)

(defpackage :lumen.utils
  (:use :cl :alexandria)
  (:export :copy-plist :-> :->> :probe-directory :col-get :alist-fuzzy-get :shuffle-list
   :format-money :send-mail
   ;; strings
   :str-prefix-p :str-suffix-p :str-contains-p
	   :ensure-trailing :ensure-leading
	   :ends-with-slash-p :starts-with-slash-p :%trim
   :join-lines :slugify
   ;; Query string
   :url-decode-qs
   ;; alists
	   :alist-get :alist-get-all :alist-set :ensure-header :plist-put
   :remove-from-alist :subst-null-with-nil :clean-alists
   ;; Lists
	   ;;:ensure-list	   
	   ;; HTTP Dates
   :parse-http-date :format-http-date :current-db-time-string :date-now
   ;; Crypto
   :hmac-sha256 :gen-uuid-string
   :subsetp-list :to-snake-case :keyword-to-kebab :to-kebab-case :lookup
	   :secure-uuid-equal

	   ;; Dates
   :%val->date-input :%val->date-display :to-timestamp :format-timestamp
   :format-now-fr :ts-filename :timestamp-diff :nb-workdays :format-hours-to-hms

   ;; Files
   :gen-safe-filename :get-extension
	   
   :now-year :now-month :now-day :pad-left

   ;; Logging
   :*log-level* :*silent-workflows* :*log-colors-p* :log-msg :with-logged-exec
   :db-network-error-p :reset-current-db-connection :run-db-with-reconnect))

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

(defun lookup (collection key &optional default)
  "Récupère une valeur dans une ALIST ou PLIST de manière robuste.
   Gère :
   1. Les clés exactes (eq/equalp)
   2. Les clés normalisées (kebab-case vs snake_case)
   3. Les Strings vs Keywords
   Retourne DEFAULT si rien n'est trouvé."
  
  (let ((kebab-key (to-kebab-case key))
        ;; Détection heuristique : Si le premier élément est une cons (pair), c'est une Alist.
        (is-alist (and (listp collection) (consp (first collection)))))
    
    (if is-alist
        ;; --- Logique ALIST ---
        (let ((pair (or (assoc key collection :test #'equalp)       ;; Essai 1 : Clé exacte
                        (assoc kebab-key collection :test #'eq))))  ;; Essai 2 : Clé Kebab
          (if pair (cdr pair) default))
        
        ;; --- Logique PLIST ---
        (let ((val (getf collection key '%%not-found%%)))
          (if (eq val '%%not-found%%)
              ;; Essai 2 : On tente avec la clé Kebab
              (getf collection kebab-key default)
              val)))))

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

(defun to-kebab-case (kw-or-str)
  "Convertit :my_value ou 'my_value' en :my-value."
  (let ((s (string-upcase (string kw-or-str))))
    (intern (substitute #\- #\_ s) :keyword)))

(defun to-kebab-keyword (str)
  "Convertit une string snake_case en keyword kebab-case.
   Exemple: \"created_at\" -> :CREATED-AT"
  (let ((up (string-upcase str)))      ;; 1. Majuscules (Convention Lisp)
    (intern                            ;; 3. Crée le keyword interné
     (substitute #\- #\_ up)           ;; 2. Remplace _ par -
     :keyword)))

(defun keyword-to-kebab (kw)
  "Convertit :project_id en :project-id"
  (if (keywordp kw)
      (intern (substitute #\- #\_ (string-upcase (symbol-name kw))) :keyword)
      kw))

(defun secure-uuid-equal (u1 u2)
  "Compare deux UUIDs qu'ils soient string, symbol ou vector."
  (let ((s1 (string-downcase (princ-to-string u1)))
        (s2 (string-downcase (princ-to-string u2))))
    (string= s1 s2)))

;;; Gestion des Dates
(defun %val->date-input (val)
  "Format ISO (YYYY-MM-DD) pour les inputs date."
  (cond
    ((null val) "")
    ;; Gestion native local-time
    #+local-time
    ((typep val 'local-time:timestamp)
     (local-time:format-timestring nil val :format '((:year 4) #\- (:month 2) #\- (:day 2))))
    ((integerp val)
     (multiple-value-bind (s m h dd mm yy) (decode-universal-time val)
       (declare (ignore s m h))
       (format nil "~4,'0D-~2,'0D-~2,'0D" yy mm dd)))
    ((stringp val) (subseq val 0 (min 10 (length val))))
    (t (format nil "~A" val))))

(defun %val->date-display (val)
  "Format Français (DD/MM/YYYY) pour l'affichage tableau."
  (cond
    ((null val) "")
    
    ;; CORRECTION 1 : Vérifier que c'est une string avant d'utiliser string=
    ((and (stringp val) (string= val "NULL")) "")
    
    ;; CORRECTION 2 : Gestion native local-time (objets @...)
    #+local-time
    ((typep val 'local-time:timestamp)
     (local-time:format-timestring nil val :format '((:day 2) #\/ (:month 2) #\/ (:year 4))))

    ((integerp val)
     (multiple-value-bind (s m h dd mm yy) (decode-universal-time val)
       (declare (ignore s m h))
       (format nil "~2,'0D/~2,'0D/~4,'0D" dd mm yy)))
    
    ;; Cas chaîne ISO existante YYYY-MM-DD
    ((and (stringp val) (>= (length val) 10) (char= (char val 4) #\-))
     (let ((y (subseq val 0 4))
           (m (subseq val 5 7))
           (d (subseq val 8 10)))
       (format nil "~A/~A/~A" d m y)))
    
    (t (format nil "~A" val))))

(defun format-hours-to-hms (hours-float)
  "Convertit une durée décimale en heures vers le format HH:MM:SS."
  (if (or (null hours-float) (<= hours-float 0))
      "00:00:00"
      (let* ((h (floor hours-float))
             (rem-m (* (- hours-float h) 60))
             (m (floor rem-m))
             (s (round (* (- rem-m m) 60))))
        (format nil "~2,'0D:~2,'0D:~2,'0D" h m s))))

(defun slugify (string)
  "Convertit une chaîne en slug URL-friendly (ex: 'Hôtel & Spa!' -> 'hotel-spa')."
  (if (or (null string) (zerop (length string)))
      ""
      (let ((s (string-downcase string)))
        ;; 1. Translitération basique (Accents français courants)
        ;; Note : Pour une translitération complète, on utiliserait une lib comme cl-unidecode,
        ;; mais ceci suffit pour 99% des cas B2B.
        (setf s (cl-ppcre:regex-replace-all "[àáâãäå]" s "a"))
        (setf s (cl-ppcre:regex-replace-all "[ç]" s "c"))
        (setf s (cl-ppcre:regex-replace-all "[èéêë]" s "e"))
        (setf s (cl-ppcre:regex-replace-all "[ìíîï]" s "i"))
        (setf s (cl-ppcre:regex-replace-all "[ñ]" s "n"))
        (setf s (cl-ppcre:regex-replace-all "[òóôõö]" s "o"))
        (setf s (cl-ppcre:regex-replace-all "[ùúûü]" s "u"))
        (setf s (cl-ppcre:regex-replace-all "[ýÿ]" s "y"))
        (setf s (cl-ppcre:regex-replace-all "[œ]" s "oe"))
        (setf s (cl-ppcre:regex-replace-all "[æ]" s "ae"))

        ;; 2. Remplacement des caractères non-alphanumériques par des tirets
        (setf s (cl-ppcre:regex-replace-all "[^a-z0-9]" s "-"))

        ;; 3. Réduction des tirets multiples (ex: "a---b" -> "a-b")
        (setf s (cl-ppcre:regex-replace-all "-+" s "-"))

        ;; 4. Nettoyage des extrémités
        (string-trim "-" s))))

(defun alist-get-all (alist key &key (test #'equal))
  "Retourne la liste de toutes les valeurs associées à la clé KEY dans ALIST.
   Par défaut utilise EQUAL pour la comparaison (fonctionne pour les Strings et Keywords)."
  (loop for (k . v) in alist
        when (funcall test k key)
          collect v))

(defun to-timestamp (val)
  "Convertit une valeur (Integer, String, Timestamp) en objet local-time:timestamp."
  (cond
    ((null val) nil)
    
    ;; Déjà un timestamp local-time
    ((typep val 'local-time:timestamp) val)
    
    ;; Universal Time (Integer standard Lisp)
    ((integerp val) (local-time:universal-to-timestamp val))
    
    ;; String ISO (ex: "2024-01-01T12:00:00Z")
    ((stringp val) 
     (handler-case 
         (local-time:parse-timestring val)
       (error () nil)))
    
    (t nil)))

(defun format-timestamp (val &key (format '(:day "/" (:month 2) "/" :year " " (:hour 2) ":" (:min 2) ":" (:sec 2))))
  "Formate une date pour l'affichage (JJ/MM/YYYY HH:mm:ss)."
  (let ((ts (to-timestamp val)))
    (if ts
        (local-time:format-timestring 
         nil 
         ts
         :format format
         :timezone local-time:+utc-zone+) ;; Ou local-time:*default-timezone* si configuré
        
        ;; Fallback si la date est nulle ou invalide
        "-")))

(defun format-now-fr ()
  (format-timestamp (local-time:now)
		    :format '((:day 2) "/" (:month 2) "/" :year)))

(defun ts-filename (filename)
  (let* ((stamp (format-timestamp (local-time:now)
				  :format '((:day 2) "" (:month 2) "" (:year 2)
					    "_" (:hour 2) "" (:min 2)))))
    (format nil "~A_~A" filename stamp)))

;; Helper pour générer un nom de fichier unique sécurisé
(defun gen-safe-filename (ext)
  (format nil "~A~A" (lumen.utils:gen-uuid-string) (if ext (format nil ".~A" ext) "")))

;; Helper pour récupérer l'extension
(defun get-extension (filename)
  (let ((pos (position #\. filename :from-end t)))
    (if pos (subseq filename (1+ pos)) nil)))

(defun %normalize-key-name (k)
  "Convertit une clé (Symbole ou String) en format canonique KEBAB-CASE majuscule.
   Ex: :user_id -> \"USER-ID\"
       \"user_id\" -> \"USER-ID\"
       :user-id -> \"USER-ID\""
  (string-upcase (substitute #\- #\_ (string k))))

(defun remove-from-alist (alist key)
  "Retourne une alist sans la clé KEY, en ignorant la casse et la différence _/-.
   
   Exemples:
     (remove-from-alist '((:user_id . 1)) :user-id)  => NIL
     (remove-from-alist '((\"user_id\" . 1)) :user-id) => NIL"
  (let ((target (if (listp key) 
                    (mapcar #'%normalize-key-name key)
                    (%normalize-key-name key))))
    (remove-if (lambda (item-key)
                 (let ((norm-item (%normalize-key-name item-key)))
                   (if (listp target)
                       (member norm-item target :test #'string=)
                       (string= norm-item target))))
               alist
               :key #'car)))

(defun now-year () (local-time:timestamp-year (local-time:now)))
(defun now-month () (local-time:timestamp-month (local-time:now)))
(defun now-day () (local-time:timestamp-day-of-week (local-time:now)))
(defun date-now () (local-time:format-timestring nil (local-time:now)))

(defun pad-left (val len char)
  "Formatage '04' ou '007'."
  (let ((s (princ-to-string val)))
    (if (< (length s) len)
        (format nil "~V,V,'~A~A" len len char s) ;; Utilise format pour le padding
        (format nil "~V,'0d" len (parse-integer s :junk-allowed t))))) 
        ;; Ou plus simple en Lisp standard:
        ;; (format nil "~2,'0d" val) pour 2 chiffres

(defun send-mail (&key from display-name
		    to subject message attachments cc bcc)
  (cl-smtp:send-email "localhost" from to subject ""
		      :html-message message
		      :display-name display-name
		      :attachments attachments
		      :cc cc
		      :bcc bcc
		      :extra-headers '(("Content-Type" "text/html; charset=UTF-8")))
  )

(defun shuffle-list (list)
  "Mélange aléatoirement les éléments d'une liste (Algorithme de Fisher-Yates)."
  (let ((vec (coerce list 'vector)))
    (loop for i from (length vec) downto 2
          do (rotatef (aref vec (1- i))
                      (aref vec (random i))))
    (coerce vec 'list)))

(defun format-money (amount &optional (suffix ""))
  "Formate un montant avec des espaces comme séparateurs de milliers. 
   Ex: (format-money 1500000 \" FCFA\") -> '1 500 000 FCFA'"
  (if (not (numberp amount))
      (format nil "0~A" suffix)
      (let* ((rounded (round amount))
             ;; ~:D formate avec des virgules (ex: 1,500,000)
             (formatted (format nil "~:D" rounded))
             ;; On substitue la virgule par un espace
             (spaced (substitute #\Space #\, formatted)))
        (format nil "~A~A" spaced suffix))))

(defun timestamp-diff (time-a time-b unit)
   (let* ((diff-in-sec (local-time:timestamp-difference time-a time-b))
	  )
     (cond ((eq unit :day)
	    (ceiling diff-in-sec (* 24 60 60)))
	   ((eq unit :hour)
	    (ceiling diff-in-sec (* 60 60)))
	   ((eq unit :min)
	    (ceiling diff-in-sec 60)))
     ))

(defun nb-workdays (time-a time-b &key (base-year (now-year)))
  (let* ((yy-a (local-time:timestamp-year time-a))
	 (mm-a (local-time:timestamp-month time-a))
	 (dd-a (local-time:timestamp-day time-a))
	 (yy-b (local-time:timestamp-year time-b))
	 (mm-b (local-time:timestamp-month time-b))
	 (dd-b (local-time:timestamp-day time-b))
	 (cal (cl-dates:make-calendar :civ :base-year base-year))
	 )
    (cl-dates:diff-workdays (cl-dates:ymd->date yy-a mm-a dd-a)
			    (cl-dates:ymd->date yy-b mm-b dd-b)
			    cal))
    )

(defparameter *log-level* :debug 
  "Niveau de log actuel : :debug, :info, :warn, :error")
(defvar *silent-workflows* nil)
(defparameter *log-colors-p* t "Mettre à NIL si les codes [32m s'affichent dans le REPL.")

(defun log-msg (context &rest args &key (level :info) &allow-other-keys)
  "Logge un message structuré. 
   Exemple : (log-msg \"EXEC DB\" :level :debug :uid \"123\" :sql \"SELECT...\")"
  
  ;; 1. Filtrage par niveau
  (let ((levels '(:debug 0 :info 1 :warn 2 :error 3)))
    (when (>= (getf levels level 0) (getf levels *log-level* 0))
      (fresh-line *standard-output*)
      
      ;; 1. Gestion des couleurs avec le caractère d'échappement correct (#\Esc)
      (let* ((esc (code-char 27))
             (color-code (case level
                           (:error "31")   ; Rouge
                           (:warn  "33")   ; Jaune
                           (:debug "36")   ; Cyan
                           (t      "32"))) ; Vert
             (blue-code "34"))

        ;; Affichage de l'en-tête
        (if *log-colors-p*
            (format t "~C[~Am[~A] ~A~C[0m" esc color-code level context esc)
            (format t "[~A] ~A" level context))

        ;; 2. Itération sur les arguments
        (loop for (key val) on args by #'cddr
              unless (eq key :level)
              do (let ((key-name (string-upcase (string-trim ":" (symbol-name key)))))
                   (if *log-colors-p*
                       (format t "~&  ~C[~Am- ~A:~C[0m" esc blue-code key-name esc)
                       (format t "~&  - ~A:" key-name))
                   
                   (cond
                     ((and (eq key :sql) (stringp val))
                      (format t "~%~{      ~A~%~}" (uiop:split-string val :separator '(#\Newline))))
                     ((listp val) (format t " ~S" val))
                     (t (format t " ~A" val))))))
      
      (fresh-line *standard-output*)
      (force-output))))

(defmacro with-logged-exec ((context &rest log-args) &body body)
  `(let ((start (get-internal-real-time)))
     (handler-case
         (multiple-value-prog1 (progn ,@body)
           (let* ((elapsed (/ (- (get-internal-real-time) start) 
                             internal-time-units-per-second))
		 (elapsed-str (format nil "~,3fs" elapsed)))
	     (apply #'log-msg ,context (append (list :duration elapsed-str :status "OK") 
                                  (list ,@log-args)))
             ))
     
       (error (e)
         (let ((elapsed (/ (- (get-internal-real-time) start) 
                           internal-time-units-per-second)))
           ;; Log de l'échec en niveau ERROR
           (apply #'log-msg ,context 
                  :level :error 
                  :duration (format nil "~,3fs" elapsed)
                  :status "FAILED"
                  :error-msg (format nil "~A" e)
                  ,@log-args)
           ;; On relance l'erreur pour le ROLLBACK de la transaction
     (error e)))
       )
     ))

#|
(defun ensure-list (item)
  "Garantit que l'élément retourné est une liste. Pratique pour les formulaires HTML où un seul élément n'est pas parsé comme un array."
  (if (listp item)
      item
      (list item)))
|#

(defun clean-alists (data)
  "Parcourt une liste d'alists et remplace les valeurs :NULL par NIL."
  (mapcar (lambda (alist)
            (mapcar (lambda (pair)
                      (if (eq (cdr pair) :null)
                          (cons (car pair) nil)
                          pair))
                    alist))
          data))

(defun subst-null-with-nil (tree)
  "Remplace récursivement toutes les occurrences de :NULL par NIL dans n'importe quelle structure."
  (cond ((eq tree :null) nil)
        ((atom tree) tree)
        (t (cons (subst-null-with-nil (car tree))
                 (subst-null-with-nil (cdr tree))))))
