(in-package :cl)

(defpackage :lumen.data.prepare
  (:use :cl)
  (:import-from :postmodern :*database* :prepare)
  (:export :get-prepared-plan :reset-prepare-cache
           :*prepare-cache-ttl-ms*))

(in-package :lumen.data.prepare)

(defparameter *prepare-cache-ttl-ms* nil)

;; conn -> (hash (sql format) -> (fn . created-at-ms))
;; On peut utiliser un weak-hash si dispo ; ici simple hash eq pour conn.
(defvar *conn->plans* (make-hash-table :test 'eq))
(defvar *lock* (bt:make-lock "lumen.prepare.lock"))

(defun %now-ms ()
  (* 1000.0 (/ (get-internal-real-time) internal-time-units-per-second)))

(defun reset-prepare-cache (&optional (connection :all))
  (bt:with-lock-held (*lock*)
    (cond ((eq connection :all)
           (clrhash *conn->plans*))
          (t
           (remhash connection *conn->plans*))))
  t)

(defun %current-connection ()
  (and (boundp 'postmodern:*database*) postmodern:*database*))

(defun %subcache (conn)
  (or (gethash conn *conn->plans*)
      (setf (gethash conn *conn->plans*) (make-hash-table :test 'equal))))

;; ----------------------------------------------------------------------------
;; Fabrique de fonctions exécutables pour un SQL donné + format
;; ----------------------------------------------------------------------------
(defun %key (sql format)
  ;; NB: on pourrait normaliser SQL (trim, collapse spaces) si besoin
  (list format sql))

(defun %mk-fn (sql format)
  "Construit une fonction à appeler (lambda (&rest params) ...) pour SQL/FORMAT.
- :alist   → une ligne (aliste) ou NIL   (via postmodern:query, pas de prepared)
- :alists  → liste d'alistes            (via postmodern:query)
- :row     → une ligne (liste de valeurs) (via postmodern:query)
- :rows    → liste de lignes (liste de listes) (via postmodern:query)
- :single  → une valeur scalaire (ex: COUNT(*)) (via postmodern:query)
- :none    → exécution sans résultat (INSERT/UPDATE/DELETE), renvoie n lignes affectées (via execute)
- :raw     → équivalent :rows (compat interne)"
  (ecase format
    (:alist
     ;; SELECT 1 ligne → :alist ; on évite les prepared (pas de collision STATEMENT_x)
     (lambda (&rest params)
       #+postmodern (eval (append (list 'postmodern:query sql) params (list :alist)))
       #-postmodern (declare (ignore params)) )
    )
    (:alists
     (lambda (&rest params)
       #+postmodern (eval (append (list 'postmodern:query sql) params (list :alists)))
       #-postmodern (declare (ignore params)) )
    )
    (:row
     (lambda (&rest params)
       #+postmodern (eval (append (list 'postmodern:query sql) params (list :row)))
       #-postmodern (declare (ignore params)) )
    )
    (:rows
     (lambda (&rest params)
       #+postmodern (eval (append (list 'postmodern:query sql) params (list :rows)))
       #-postmodern (declare (ignore params)) )
    )
    (:single
     (lambda (&rest params)
       #+postmodern (eval (append (list 'postmodern:query sql) params (list :single)))
       #-postmodern (declare (ignore params)) )
    )
    (:raw
     ;; Compat interne → :rows
     (lambda (&rest params)
       #+postmodern (eval (append (list 'postmodern:query sql) params (list :rows)))
       #-postmodern (declare (ignore params)) )
    )
    (:none
     ;; Exécution sans résultat (UPDATE/INSERT/DELETE), retourne n (rows affected).
     ;; postmodern:execute est une macro → appel via EVAL.
     (lambda (&rest params)
       #+postmodern (eval (list* 'postmodern:execute sql params))
       #-postmodern 0))))

(defun get-prepared-plan (sql &key (format :alists))
  "Retourne une FONCTION préparée pour SQL au FORMAT (par défaut :ALISTS).
Formats acceptés côté Postmodern :
  :rows   :lists   :alists :alist :plists :plist
  :raw(= :rows)    :none   (exécution sans résultat → n lignes affectées)

Cache par connexion (clé = (conn sql format))."
  (let* ((conn (%current-connection))
         (key  (list sql format)))
    (unless conn
      (error "Aucune connexion Postmodern active (get-prepared-plan ~S)" sql))
    (bt:with-lock-held (*lock*)
      (let* ((tab  (%subcache conn))
             (pair (gethash key tab))
             (fn   (car pair))
             (t0   (cdr pair)))
        ;; TTL : invalider l'entrée si expirée
        (when (and fn *prepare-cache-ttl-ms*
                   (> (- (%now-ms) (or t0 0)) *prepare-cache-ttl-ms*))
          (setf fn nil pair nil)
          (remhash key tab))
        (if fn
            (progn
              (lumen.data.metrics:incr-prepare-cache-hit sql)
              fn)
            (let ((new-fn (%mk-fn sql format)))
              (setf (gethash key tab) (cons new-fn (%now-ms)))
              (lumen.data.metrics:incr-prepare-cache-miss sql)
              new-fn))))))
