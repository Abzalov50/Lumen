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

(defun %get-row-reader (format)
  "Associe le format Lumen aux Row Readers de cl-postgres."
  (ecase format
    ((:rows :raw) 'cl-postgres:list-row-reader)
    ((:alist :alists) 'pomo::symbol-alist-row-reader) ;;'cl-postgres:alist-row-reader)
    (:none 'cl-postgres:ignore-row-reader)
    (:row 
     ;; Reader personnalisé : retourne la 1ère ligne uniquement
     (lambda (socket fields) 
       (let ((rows (cl-postgres:list-row-reader socket fields)))
         (first rows))))
    (:single
     ;; Reader personnalisé : retourne la 1ère colonne de la 1ère ligne
     (lambda (socket fields)
       (let ((rows (cl-postgres:list-row-reader socket fields)))
         (and rows (car (first rows))))))))

(defun %mk-fn (sql format)
  "Crée une fonction d'exécution ULTRA-ROBUSTE et AUTO-RÉPARATRICE.
   1. Gère la désynchronisation Tx Lisp vs DB (25P01).
   2. Gère les doublons de statements (42P05).
   3. Gère l'invalidation du plan d'exécution après migration (0A000)."
  
  (let ((stmt-name (format nil "L_~A" (abs (sxhash sql))))
        (reader (%get-row-reader format)))
    
    (lambda (&rest params)
      (let ((conn postmodern:*database*))
        
        ;; On définit une fonction locale pour pouvoir relancer le processus complet
        (labels ((execute-logic (&optional (retry-count 0))
                   ;; Sécurité anti-boucle infinie
                   (when (> retry-count 1)
                     (error "Échec critique : Impossible de réparer le statement ~A après tentative." stmt-name))

                   ;; --- PARTIE 1 : PRÉPARATION (Votre code "Ultra-Robuste") ---
                   (handler-case
                       ;; Tentative optimiste avec protection transactionnelle
                       (postmodern:with-savepoint sp
                         (cl-postgres:prepare-query conn stmt-name sql))
                     
                     (cl-postgres-error::database-error (e)
                       (let ((code (cl-postgres-error::database-error-code e)))
                         (cond
                           ;; CAS 1 : DOUBLON DANS UNE TX (42P05) -> OK, on continue
                           ((string= code "42P05") nil)
                           
                           ;; CAS 2 : PAS DE TRANSACTION (25P01) -> Fallback sans savepoint
                           ((string= code "25P01")
                            (handler-case
                                (cl-postgres:prepare-query conn stmt-name sql)
                              (cl-postgres-error::database-error (e2)
                                ;; Si doublon hors transaction, on ignore
                                (unless (string= (cl-postgres-error::database-error-code e2) "42P05")
                                  (error e2)))))
                           
                           ;; CAS 3 : VRAIE ERREUR -> On remonte
                           (t (error e))))))

                   ;; --- PARTIE 2 : EXÉCUTION (Avec gestion du 0A000) ---
                   (handler-case
                       (cond
                         ((eq format :none)
                          (multiple-value-bind (res count)
                              (cl-postgres:exec-prepared conn stmt-name params reader)
                            (declare (ignore res))
                            count))
                         (t
                          (cl-postgres:exec-prepared conn stmt-name params reader)))
                     
                     ;; INTERCEPTION DU PLAN INVALIDÉ
                     (cl-postgres-error::database-error (e)
                       (if (string= (cl-postgres-error::database-error-code e) "0A000")
                           (progn
                             ;; Log (Optionnel, utile pour le debug)
                             ;; (format t "~&[DB FIX] Plan obsolète pour ~A. Reset...~%" stmt-name)
                             
                             ;; 1. On supprime le statement corrompu de la session Postgres
                             (cl-postgres:exec-query conn (format nil "DEALLOCATE ~A" stmt-name))
                             
                             ;; 2. On RE-PRÉPARE et RÉ-EXÉCUTE tout via récursion
                             (execute-logic (1+ retry-count)))
                           
                           ;; Sinon, c'est une vraie erreur SQL (ex: contrainte violée), on laisse passer
                           (error e))))))

          ;; Lancement initial
          (execute-logic))))))

;; Reset du cache pour forcer la recompilation des fermetures
(reset-prepare-cache)

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
