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
          (values (cl-json:decode-json-from-string txt) txt)
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
  "Version Hybride : Spooling Disque -> Parsing RAM -> Extraction Disque.
   Retourne : ((:fields . alist) (:files . list-of-alists))"
  
  (let ((boundary (%find-boundary content-type)))
    (when (null boundary) (return-from parse-multipart nil))

    ;; 1. SPOOLING : On décharge le réseau vers un fichier temporaire
    (let ((spool-file (merge-pathnames (format nil "req_~A.tmp" (lumen.utils:gen-uuid-string)) 
                                       lumen.core.config:*tmp-dir*)))
      
      (unwind-protect
           (progn
             ;; A. Copie Flux Réseau -> Disque (LECTURE BORNÉE STRICTE)
             (with-open-file (out spool-file :direction :output 
                                             :element-type '(unsigned-byte 8) 
                                             :if-exists :supersede)
               (let ((remaining length)
                     ;; Buffer de 8Ko pour la performance
                     (buffer (make-array 8192 :element-type '(unsigned-byte 8))))
                 (loop while (> remaining 0) do
                   (let* ((chunk-size (min remaining 8192))
                          ;; On lit au maximum chunk-size octets
                          (read-count (read-sequence buffer stream :end chunk-size)))
                     (when (zerop read-count) 
                       (return)) ;; EOF prématuré (connexion coupée)
                     (write-sequence buffer out :end read-count)
                     (decf remaining read-count)))))
             
             ;; B. Parsing (On charge le fichier spool en RAM pour parser)
             (with-open-file (in spool-file :direction :input :element-type '(unsigned-byte 8))
               (let ((file-len (file-length in)))
                 (if (> file-len (or limit (* 50 1024 1024)))
                     (error "Multipart body too large for RAM parsing")
                     
                     (let ((raw (make-array file-len :element-type '(unsigned-byte 8))))
                       (read-sequence raw in)
                       (%parse-multipart-bytes raw boundary))))))
        
        ;; C. Nettoyage du fichier spool
        (ignore-errors (delete-file spool-file))))))

(defun %parse-multipart-bytes (raw boundary)
  "Logique originale adaptée pour écrire les fichiers sur le disque."
  (let* ((sep-bytes (%ascii-bytes (format nil "--~a" boundary)))
         (pos 0)
         (parts '()))
    
    ;; 1. Trouver le premier séparateur
    (let ((first (%bytes-index-of raw sep-bytes 0)))
      (when (null first) (return-from %parse-multipart-bytes nil))
      (setf pos (+ first (length sep-bytes))))
    
    (loop
      ;; A. Sauter le CRLF
      (when (and (<= (+ pos 2) (length raw))
                 (= (aref raw pos) 13) (= (aref raw (1+ pos)) 10))
        (incf pos 2))
      
      ;; B. Fin de flux (--boundary--)
      (when (and (<= (+ pos 2) (length raw))
                 (= (aref raw pos) #x2D) (= (aref raw (1+ pos)) #x2D))
        (return))
      
      ;; C. Headers
      (let* ((hdrs-end (%bytes-index-of raw #(#x0D #x0A #x0D #x0A) pos)))
        (when (null hdrs-end) (return))
        
        (let* ((bstart (+ hdrs-end 4))
               (needle (concatenate 'vector #(#x0D #x0A) sep-bytes))
               (next (%bytes-index-of raw needle bstart)))
          
          (when (null next) (setf next (length raw)))
          
          (let* ((header-bytes (subseq raw pos hdrs-end))
                 (header-str (map 'string #'code-char header-bytes))
                 
                 ;; Extraction métadonnées
                 (fname (extract-param header-str "filename"))
                 (name  (or (extract-param header-str "name") ""))
                 (ctype (or (extract-header header-str "Content-Type") 
                            "text/plain")))
            
            (if fname
                ;; --- CAS FICHIER : ÉCRITURE DISQUE ---
                (let* ((upload-path (merge-pathnames (format nil "upload_~A_~A" (lumen.utils:gen-uuid-string) fname)
                                                     lumen.core.config:*tmp-dir*))
                       (part-len (- next bstart)))
                  
                  ;; On écrit la slice du buffer directement dans le fichier final
                  (with-open-file (out upload-path :direction :output 
                                                   :element-type '(unsigned-byte 8) 
                                                   :if-exists :supersede)
                    (write-sequence raw out :start bstart :end next))
                  
                  (push `((:name . ,name)
                          (:filename . ,fname)
                          (:content-type . ,ctype)
                          (:path . ,upload-path) ;; <--- On retourne le CHEMIN
                          (:size . ,part-len))
                        parts))
                
                ;; --- CAS TEXTE : MÉMOIRE ---
                (let* ((val-bytes (subseq raw bstart next))
                       (val-str (babel:octets-to-string val-bytes :encoding :utf-8)))
                  (push (cons name val-str) parts)))
            
            ;; Avance POS
            (let ((after (+ next 2 (length sep-bytes))))
              (if (>= after (length raw))
                  (return)
                  (setf pos after)))))))
    
    ;; Tri final
    (let ((fields '()) (files '()))
      (dolist (p (nreverse parts))
        (if (listp (cdr p))
            (push p files)
            (push p fields)))
      `((:fields . ,fields) (:files . ,files)))))

;; --- Helpers de parsing (Simplifiés) ---

(defun extract-param (header-str key)
  "Cherche key=\"val\""
  (let* ((pattern (format nil "~A=\"" key))
         (p0 (search pattern header-str :test #'string-equal)))
    (when p0
      (let* ((start (+ p0 (length pattern)))
             (end (position #\" header-str :start start)))
        (when end (subseq header-str start end))))))

(defun extract-header (header-str key)
  "Cherche Key: Val"
  (let ((p0 (search (format nil "~A:" key) header-str :test #'string-equal)))
    (if p0
        (let* ((start (+ p0 1 (length key)))
               (end (position #\Return header-str :start start)))
          (string-trim " " (subseq header-str start (or end (length header-str)))))
        nil)))
