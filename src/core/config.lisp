(in-package :cl)

(defpackage :lumen.core.config
  (:use :cl :alexandria)
  (:import-from :uiop :getenv) ;; (on va l'appeler via un hook)
  (:export
   :cfg                       ; (cfg :get key &key default) | (cfg :set! key val) | (cfg :load-env path &key override)
   :cfg-get :cfg-get-int :cfg-get-bool :cfg-get-duration :cfg-get-list
   :cfg-set! :cfg-load-env :cfg-profile
   :*getenv-fn* :*profile*))

(in-package :lumen.core.config)

;; Hook d'environnement : par défaut, lit l'ENV réel
(defparameter *getenv-fn* #'uiop:getenv)

(defun %env (name)
  (funcall *getenv-fn* name))

;;; -------------------------------
;;; Stores (priorité: runtime > ENV > file > defaults)
;;; -------------------------------
(defvar *cfg-defaults* (make-hash-table :test 'equal))  ; faible
(defvar *cfg-file*     (make-hash-table :test 'equal))  ; .env
(defvar *cfg-runtime*  (make-hash-table :test 'equal))  ; overrides à chaud

(defparameter *profile*
  (let ((p (or (%env "LUMEN_PROFILE") (%env "APP_ENV") "dev")))
    (values (intern (string-upcase p) :keyword))))

(defun cfg-profile () *profile*)

;;; -------------------------------
;;; Helpers key <-> env
;;; -------------------------------
(defun %kw (thing)
  "Normalise une clé en keyword hiérarchique.
Règles :
- Les points '.' séparent les niveaux → '/':  \"db.pool.max_size\" → :db/pool/max-size
- S’il n’y a PAS de point :
  * le **premier** underscore '_' sépare 2 niveaux,
  * les underscores restants deviennent des tirets '-':
    \"HTTP_PORT\" → :http/port
    \"TENANT_CACHE_TTL_MS\" → :tenant/cache-ttl-ms
- S’il Y A au moins un point :
  * les underscores à l’intérieur de chaque segment deviennent des tirets,
  * ils ne créent pas de niveaux supplémentaires.
    \"db.pool.max_size\" → :db/pool/max-size"
  (cond
    ((keywordp thing) thing)
    ((symbolp thing) (intern (string-upcase (symbol-name thing)) :keyword))
    ((stringp thing)
     (let* ((s (string-downcase thing))
            (dot-segs (uiop:split-string s :separator ".")))
       (labels
           ((join-path (segments)
              (intern (string-upcase
                       (map 'string #'identity
                            (with-output-to-string (out)
                              (loop for i from 0 below (length segments) do
                                    (when (> i 0) (write-char #\/ out))
                                    (princ (aref segments i) out)))))
                      :keyword)))
         (cond
           ;; Pas de point → règle “premier underscore hiérarchique”
           ((= (length dot-segs) 1)
            (let* ((seg (first dot-segs))
                   (us (uiop:split-string seg :separator "_")))
              (cond
                ((= (length us) 1)
                 (join-path (vector seg)))
                ((>= (length us) 2)
                 (let* ((head (first us))
                        (tail (rest us))
                        (tail-joined (format nil "~{~a~^-~}" tail)))
                   (join-path (vector head tail-joined))))
                (t (join-path (vector seg))))))
           ;; Au moins un point → chaque segment: '_' → '-' ; les points forment les niveaux
           (t
            (let* ((norm (map 'vector
                              (lambda (seg)
                                (substitute #\- #\_ seg))
                              dot-segs)))
              (join-path norm)))))))
    (t (error "Invalid config key: ~S" thing))))

(defun %env-name (key)
  "Transforme :http/port → \"HTTP_PORT\"."
  (let* ((s (string-downcase (symbol-name (%kw key)))))
    (string-upcase (substitute #\_ #\/ s))))

;;; -------------------------------
;;; Put/Get par couche
;;; -------------------------------
(defun %put! (ht key value) (setf (gethash (%kw key) ht) value))
(defun %get (ht key) (gethash (%kw key) ht))

(defun cfg-set! (key value) (%put! *cfg-runtime* key value))

;;; -------------------------------
;;; .env loader (KEY=val, lignes vides & #comment ignorées)
;;; -------------------------------
(defun %trim (s) (string-trim '(#\Return #\Newline #\Tab) s))

(defun %strip-quotes (s)
  (if (and (>= (length s) 2)
           (or (and (char= (char s 0) #\")
                    (char= (char s (1- (length s))) #\"))
               (and (char= (char s 0) #\')
                    (char= (char s (1- (length s))) #\'))))
      (subseq s 1 (1- (length s)))
      s))

(defun cfg-load-env (path &key (override nil))
  "Charge un fichier .env KEY=VAL. Si OVERRIDE=T, remplace les valeurs existantes de *cfg-file*."
  (print "IN CFG LOAD")
  ;;(print path)
  ;;(print override)
  (clrhash *cfg-file*)
  ;;(print *cfg-file*)
  (when (probe-file path)
    (with-open-file (in path :direction :input :external-format :utf-8)
      (loop for line = (read-line in nil nil)
            while line do
              (let ((l (%trim line)))
                (unless (or (string= l "") (char= (char l 0) #\#))
                  (let* ((pos (position #\= l))
                         (k (and pos (%trim (subseq l 0 pos))))
                         (v (and pos (%strip-quotes (%trim (subseq l (1+ pos)))))))
                    (when k
                      (let ((kk (%kw k)))
                        (when (or override (null (%get *cfg-file* kk)))
                          (%put! *cfg-file* kk v))))))))))
  t)

;;; -------------------------------
;;; Resolve (avec ENV live)
;;; -------------------------------
(defun %resolve (key &key default)
  "Priorité: runtime > ENV > file > defaults > default."
  (let* ((kk (%kw key))
         (env (%env (%env-name kk))))
    (or (%get *cfg-runtime* kk)
        (and env (not (string= env "")) env)
        (%get *cfg-file* kk)
        (%get *cfg-defaults* kk)
        default)))

;;; -------------------------------
;;; Parsers typés
;;; -------------------------------
(defun %parse-bool (v)
  (etypecase v
    (null nil)
    (string
     (let ((s (string-downcase (string-trim '(#\Space) v))))
       (cond ((member s '("1" "t" "true" "yes" "on") :test #'string=) t)
             ((member s '("0" "f" "false" "no" "off" "") :test #'string=) nil)
             (t nil))))
    (integer (not (zerop v)))
    (symbol  (not (eq v nil)))
    (t t)))

(defun %parse-int (v &optional default)
  (etypecase v
    (integer v)
    (string (or (parse-integer v :junk-allowed t) default))
    (t default)))

(defun %parse-list (v &optional (sep #\,))
  (etypecase v
    (null '())
    (string (remove "" (mapcar #'%trim (uiop:split-string v :separator (string sep)))
		    :test #'string=))
    (list v)
    (t (list v))))

(defun %parse-duration-seconds (v &optional default)
  "Supporte: 500ms, 2s, 3m, 1h, 1d. Nu ‘42’ ⇒ secondes."
  (labels ((as-int (x) (and x (parse-integer x :junk-allowed t))))
    (etypecase v
      (integer v)
      (string
       (let* ((s (string-downcase (string-trim '(#\Space) v)))
              (ms (cl-ppcre:register-groups-bind (num unit)
                      ("^([0-9]+)\\s*(ms|s|m|h|d)?$" s)
                    (list (as-int num) unit))))
         (destructuring-bind (n unit) ms
           (if (null n) (or default 0)
               (* n (case (intern (string-upcase (or unit "S")) :keyword)
                      (:MS (max 1 (ceiling (/ n 1000.0))))
                      (:S  1)
                      (:M  60)
                      (:H  3600)
                      (:D  86400)
                      (t 1)))))))
      (t (or default 0)))))

;;; -------------------------------
;;; API conviviale
;;; -------------------------------
(defun cfg-get (key &key default)
  (%resolve key :default default))

(defun cfg-get-int (key &key default)
  (%parse-int (%resolve key) default))

(defun cfg-get-bool (key &key default)
  (let ((x (%resolve key))) (if (null x) default (%parse-bool x))))

(defun cfg-get-duration (key &key (default "0s"))
  (%parse-duration-seconds (%resolve key :default default)))

(defun cfg-get-list (key &key (sep #\,))
  (%parse-list (%resolve key) sep))

(defun cfg (&rest args)
  "Mini dispatcher: (cfg :get k [:default v]) | (cfg :set! k v) | (cfg :load-env path [:override t]) | (cfg :profile)"
  (let ((op (first args)))
    (ecase op
      (:get        (apply #'cfg-get (rest args)))
      (:set!       (apply #'cfg-set! (rest args)))
      (:load-env   (apply #'cfg-load-env (rest args)))
      (:profile    (cfg-profile)))))

