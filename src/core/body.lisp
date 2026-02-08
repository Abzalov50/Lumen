(in-package :cl)

(defpackage :lumen.core.body
  (:use :cl)
  (:import-from :lumen.utils :%trim :url-decode-qs)
  (:import-from :lumen.core.mime :guess-content-type)
  (:export :parse-urlencoded :parse-multipart :json-get :parse-json 
	   :read-exact-bytes :bytes->string-utf8 :string->bytes-utf8))

(in-package :lumen.core.body)

(defun read-exact-bytes (stream n &key limit)
  "Lit exactement N octets. Si N > LIMIT, erreur."
  (when (and limit (> n limit))
    (error "Body too large (actual read attempt)"))
  (let ((buf (make-array n :element-type '(unsigned-byte 8))))
    (read-sequence buf stream)
    buf))

(defun bytes->string-utf8 (octets)
  (trivial-utf-8:utf-8-bytes-to-string octets))

(defun string->bytes-utf8 (s)
  (trivial-utf-8:string-to-utf-8-bytes s))

(defun json-get (obj key)
  "Récupère une valeur dans OBJ pour KEY, en tolérant alist/plist/hash-table
   et clés string | :keyword | symbol."
  (labels ((key=? (a b)
             (cond ((and (stringp a) (stringp b)) (string-equal a b))
                   ((and (symbolp a) (symbolp b)) (string-equal (symbol-name a)
                                                                (symbol-name b)))
                   ((and (stringp a) (symbolp b)) (string-equal a (symbol-name b)))
                   ((and (symbolp a) (stringp b)) (string-equal (symbol-name a) b))
                   (t nil))))
    (cond
      ((hash-table-p obj)
       (or (gethash key obj)
           (and (stringp key) (gethash (intern (string-upcase key) :keyword) obj))
           (and (symbolp key) (gethash (string-downcase (symbol-name key)) obj))))
      ((and (listp obj) (every #'consp obj)) ; alist
       (let ((cell (or (assoc key obj :test #'key=?)
                       (assoc (intern (string-upcase (prin1-to-string key)) :keyword) obj
                              :test #'key=?))))
         (when cell (cdr cell))))
      ((and (listp obj) (evenp (length obj))) ; plist
       (getf obj key))
      (t nil))))

(defun parse-json (stream length &key limit)
  (when (and stream length (> length 0))
    ;; 1. Lecture sécurisée des octets
    (let* ((octets (handler-case 
                       (read-exact-bytes stream length :limit limit)
                     (error (e) 
                       (declare (ignore e))
                       (return-from parse-json nil)))) ;; ou signal erreur spécifique
           (txt (trivial-utf-8:utf-8-bytes-to-string octets)))
      
      ;; 2. Parsing JSON
      (handler-case
          (cl-json:decode-json-from-string txt)
        (error (e)
          (format *error-output* "~&[json] decode error: ~A~%" e)
          nil)))))

;;; ---------- x-www-form-urlencoded ------------------------------------------
(defun url-decode-utf8 (s)
  "Décode une chaîne URL-encoded en UTF-8 réel.
   Transforme %C3%A9 en octets #xC3 #xA9 puis en string 'é'."
  (when s
    (let* ((len (length s))
           ;; On prépare un buffer d'octets (unsigned-byte 8)
           ;; La taille décodée sera <= la taille encodée.
           (bytes (make-array len :element-type '(unsigned-byte 8) :fill-pointer 0)))
      
      (loop with i = 0
            while (< i len)
            for c = (char s i)
            do (cond
                 ;; 1. Gestion du + (Espace)
                 ((char= c #\+)
                  (vector-push 32 bytes) ;; 32 = Code ASCII de l'espace
                  (incf i))
                 
                 ;; 2. Gestion du %XX (Hexadécimal)
                 ((and (char= c #\%)
                       (< (+ i 2) len))
                  ;; On parse les 2 caractères suivants comme un entier HEXA
                  (let ((hex-val (parse-integer s :start (1+ i) :end (+ i 3) :radix 16 :junk-allowed t)))
                    (if hex-val
                        (progn
                          (vector-push hex-val bytes) ;; On pousse l'OCTET, pas le char
                          (incf i 3))
                        ;; Fallback si % n'est pas suivi de hex valide
                        (progn
                          (vector-push (char-code c) bytes)
                          (incf i)))))
                 
                 ;; 3. Caractères standards (ASCII 7-bit)
                 (t
                  (vector-push (char-code c) bytes)
                  (incf i))))
      
      ;; Conversion finale : Tableau d'octets -> String UTF-8
      (trivial-utf-8:utf-8-bytes-to-string bytes))))

;; Mise à jour de votre parseur pour utiliser la nouvelle fonction
(defun parse-urlencoded-string (s)
  "Transforme \"a=1&b=%C3%A9\" -> alist '( (\"a\" . \"1\") (\"b\" . \"é\") )."
  (let ((pairs (uiop:split-string (or s "") :separator "&")))
    (loop for p in pairs
          for pos = (position #\= p)
          ;; Utilisation de url-decode-utf8 au lieu de url-decode-qs
          for k   = (url-decode-utf8 (subseq p 0 (or pos (length p))))
          for v   = (url-decode-utf8 (and pos (subseq p (1+ pos))))
          when (and k (plusp (length k)))
          collect (cons k (or v "")))))

;; Votre point d'entrée reste inchangé
(defun parse-urlencoded (stream length)
  "Lit LENGTH octets du STREAM et renvoie une alist (name . value)."
  (when (and stream length (> length 0))
    ;; Note: bytes->string-utf8 ici est sûr car le body url-encoded est 100% ASCII
    ;; (les caractères spéciaux sont déjà échappés en %).
    (parse-urlencoded-string (bytes->string-utf8 (read-exact-bytes stream length)))))

;;; ---------- multipart/form-data (in-memory minimal) -------------------------

;; Structure renvoyée: plist
;;  :fields => alist '(("name" . "value") ...)
;;  :files  => list de plists  '((:name \"field\" :filename \"f.txt\" :content-type \"text/plain\" :bytes #(u8 ...)) ...)
(defun %find-boundary (ct)
  "Retourne la boundary (string) depuis Content-Type multipart."
  (when (and ct (search "multipart/form-data" (string-downcase ct)))
    (let* ((parts (uiop:split-string ct :separator ";"))
           (b (some (lambda (p)
                      (let* ((s (string-trim '(#\Space #\Tab) p))
                             (pos (position #\= s)))
                        (when (and pos (string-equal (subseq s 0 pos) "boundary"))
                          (subseq s (1+ pos)))))
                    parts)))
      (and b (string-trim '(#\Space #\Tab #\") b)))))

;; -- Helpers bas niveau ------------------------------------------------------
(defun %ascii-bytes (s)
  "String ASCII -> vector d’octets."
  (let* ((len (length s))
         (v (make-array len :element-type '(unsigned-byte 8))))
    (dotimes (i len v) (setf (aref v i) (char-code (char s i))))))

(defun %bytes-index-of (hay needle &optional (start 0))
  "Retourne l’index du 1er occurence de NEEDLE (vector d’octets) dans HAY à partir de START, ou NIL."
  (let* ((n (length needle))
         (h (length hay)))
    (when (<= n (- h start))
      (loop for i from start to (- h n) do
           (when (loop for j from 0 below n
                       always (= (aref hay (+ i j)) (aref needle j)))
             (return i))))))

(defun %strip-trailing-crlf (bytes)
  "Retire un CRLF final si présent."
  (let ((len (length bytes)))
    (cond
      ((and (>= len 2)
            (= (aref bytes (- len 2)) 13)
            (= (aref bytes (- len 1)) 10))
       (subseq bytes 0 (- len 2)))
      (t bytes))))

(defun %parse-headers (bytes start end)
  "Parse les headers en alist (k . v). Bytes ASCII entre START et END (exclus)."
  (let ((lines '())
        (i start))
    ;; découpe par CRLF
    (loop for j = (%bytes-index-of bytes #(#x0D #x0A) i)
          while (and j (< j end)) do
          (push (subseq bytes i j) lines)
          (setf i (+ j 2)))
    (nreverse
     (mapcar (lambda (bline)
               (let* ((line (trivial-utf-8:utf-8-bytes-to-string bline))
                      (pos (and line (position #\: line))))
                 (if (and pos (> pos 0))
                     (cons (string-downcase (string-trim '(#\Space #\Tab)
                                                         (subseq line 0 pos)))
                           (string-trim '(#\Space #\Tab)
                                        (subseq line (1+ pos))))
                     (cons "" ""))))
             lines))))

(defun %parse-content-disposition (v)
  "Parse Content-Disposition: \"form-data; name=...; filename=...\" -> alist."
  (let* ((parts (cl-ppcre:split "\\s*;\\s*" v))
         (kv '()))
    (dolist (p parts)
      (let* ((pos (position #\= p))
             (k (string-downcase (string-trim '(#\Space #\Tab) (if pos (subseq p 0 pos) p))))
             (raw (and pos (subseq p (1+ pos))))
             (val (and raw
                       (if (and (> (length raw) 1)
                                (char= (char raw 0) #\")
                                (char= (char raw (1- (length raw))) #\"))
                           (subseq raw 1 (1- (length raw)))
                           raw))))
        (when (> (length k) 0) (push (cons k val) kv))))
    (nreverse kv)))

(defun %decode-text-field (bytes &optional (charset "utf-8"))
  "Décode bytes d’un champ texte. Fallback latin-1 si UTF-8 échoue."
  (declare (ignore charset)) ;; pour l’instant on force UTF-8
  (handler-case
      (trivial-utf-8:utf-8-bytes-to-string bytes)
    (trivial-utf-8:utf-8-decoding-error ()
      ;; fallback très basique ISO-8859-1 si tu as Babel, sinon remonter brut
      (ignore-errors
        (babel:octets-to-string bytes :encoding :latin1))
      ;; si Babel indispo, renvoyer quelque chose d’inoffensif
      (or (ignore-errors (babel:octets-to-string bytes :encoding :latin1))
          ""))))

;; -- Parser multipart en octets ----------------------------------------------
(defun parse-multipart (stream length content-type &key limit)
  "Parse multipart/form-data (en mémoire). Retourne plist :fields (alist) / :files (list d’alists)."
  (let* ((boundary (%find-boundary content-type)))
    (when (null boundary) (return-from parse-multipart nil))
    
    (let* ((raw (read-exact-bytes stream length :limit limit))
           (sep-bytes (%ascii-bytes (format nil "--~a" boundary)))
           (pos 0)
           parts)
      
      ;; 1. Trouver le premier séparateur
      (let ((first (%bytes-index-of raw sep-bytes 0)))
        (when (null first) (return-from parse-multipart nil))
        (setf pos (+ first (length sep-bytes))))
      
      (loop
        ;; A. Sauter le CRLF après le boundary
        (when (and (<= (+ pos 2) (length raw))
                   (= (aref raw pos) 13) (= (aref raw (1+ pos)) 10))
          (incf pos 2))
        
        ;; B. Fin de flux (--boundary--)
        (when (and (<= (+ pos 2) (length raw))
                   (= (aref raw pos) #x2D) (= (aref raw (1+ pos)) #x2D))
          (return))
        
        ;; C. Délimitation du bloc Headers
        (let* ((hdrs-end (%bytes-index-of raw #(#x0D #x0A #x0D #x0A) pos)))
          (when (null hdrs-end) (return))
          
          (let* ((bstart (+ hdrs-end 4))
                 (needle (concatenate 'vector #(#x0D #x0A) sep-bytes))
                 (next (%bytes-index-of raw needle bstart)))
            
            (when (null next) (setf next (length raw)))
            
            ;; --- EXTRACTION ROBUSTE ---
            ;; 1. On prend le bloc header brut et on le convertit en String ASCII
            ;;    On s'affranchit totalement de %parse-headers
            (let* ((header-bytes (subseq raw pos hdrs-end))
                   (header-str (map 'string #'code-char header-bytes)) ;; ASCII conversion simple
                   (body (subseq raw bstart next))
                   
                   ;; 2. Extracteur "chirurgical" : cherche `key="val"` n'importe où dans le header string
                   (extract-quoted 
                    (lambda (key)
                      (let* ((pattern (format nil "~A=\"" key)) ;; ex: name="
                             (p0 (search pattern header-str :test #'string-equal)))
                        (when p0
                          (let* ((start (+ p0 (length pattern)))
                                 (end (position #\" header-str :start start)))
                            (when end
                              (subseq header-str start end)))))))
                   
                   ;; 3. Extracteur simple pour Content-Type (pas de guillemets)
                   (extract-ct 
                    (lambda ()
                      (let ((p0 (search "Content-Type:" header-str :test #'string-equal)))
                        (if p0
                            (let* ((start (+ p0 13)) ;; length of "Content-Type:"
                                   (end (position #\Return header-str :start start))) ;; s'arrête au \r
                              (string-trim " " (subseq header-str start (or end (length header-str)))))
                            nil))))
                   
                   ;; 4. Récupération des valeurs
                   (fname (funcall extract-quoted "filename"))
                   (name  (or (funcall extract-quoted "name") "")) ;; Fallback vide
                   (ctype (or (funcall extract-ct) 
                              (and fname (lumen.core.mime:guess-content-type fname))
                              "text/plain")))
              
              ;; (format *error-output* "[DEBUG] Found Name: '~A' Fname: '~A'~%" name fname)
              
              (if fname
                  ;; FICHIER
                  (push `((:name . ,name)
                          (:filename . ,fname)
                          (:content-type . ,ctype)
                          (:bytes . ,(%strip-trailing-crlf body)))
                        parts)
                  
                  ;; CHAMP TEXTE
                  (let* ((charset "utf-8") ;; On assume UTF-8 pour les champs texte
                         (txt (%decode-text-field (%strip-trailing-crlf body) charset)))
                    (push (cons name txt) parts))))
            
            ;; Avance POS
            (let ((after (+ next 2 (length sep-bytes))))
              (if (>= after (length raw))
                  (return)
                  (setf pos after))))))
      
      ;; 3. Tri final
      (let ((fields '()) (files '()))
        (dolist (p (nreverse parts))
          (if (and (listp p) (consp (car p)) (keywordp (caar p)))
              (push p files)
              (push p fields)))
        
        `((:fields . ,(nreverse fields))
          (:files  . ,(nreverse files)))))))
