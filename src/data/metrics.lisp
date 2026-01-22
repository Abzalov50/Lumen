(in-package :cl)

(defpackage :lumen.data.metrics
  (:use :cl)
  (:export
   ;; API demandée
   :record-query-latency
   :incr-prepare-cache-hit
   :incr-prepare-cache-miss
   :record-pool-stats
   :record-slow-query
   ;; Hooks optionnels (à configurer par l’app)
   :*on-record-query-latency*
   :*on-prepare-cache-hit*
   :*on-prepare-cache-miss*
   :*on-record-pool-stats*   
   :*on-slow-query*
   ;; Outils pratiques (debug)
   :metrics-snapshot
   :reset-metrics))

(in-package :lumen.data.metrics)

;;; ----------------------------------------------------------------------------
;;; Hooks (callbacks optionnels)
;;; ----------------------------------------------------------------------------
;; Si ces variables sont des fonctions (ou closures), elles seront appelées.
;; Sinon (NIL), on ne fait rien → zéro coût pour ceux qui n'ont pas de backend.
(defvar *on-record-query-latency* nil
  "fn (sql elapsed-ms rows) → nil")
(defvar *on-prepare-cache-hit* nil
  "fn (sql) → nil")
(defvar *on-prepare-cache-miss* nil
  "fn (sql) → nil")
(defvar *on-record-pool-stats* nil
  "fn (in-use wait-ms) → nil")
(defvar *on-slow-query* nil
  "fn (sql elapsed-ms &key params rows affected) → nil")

;;; ----------------------------------------------------------------------------
;;; Compteurs internes (debug/snapshot)
;;; ----------------------------------------------------------------------------
(defvar *lock* (ignore-errors (bt:make-lock "lumen.metrics.lock")))
(defvar *q-count* 0)
(defvar *q-latency-total-ms* 0.0d0)
(defvar *q-latency-min-ms* most-positive-double-float)
(defvar *q-latency-max-ms* 0.0d0)
(defvar *q-rows-total* 0)

(defvar *prep-hit* 0)
(defvar *prep-miss* 0)

(defvar *pool-last-in-use* 0)
(defvar *pool-wait-total-ms* 0.0d0)
(defvar *pool-samples* 0)

(defvar *slow-count* 0)
(defvar *slow-max-ms* 0.0d0)

;;; ----------------------------------------------------------------------------
;;; Helpers
;;; ----------------------------------------------------------------------------
(defmacro %with-lock (&body body)
  `(if *lock*
       (bt:with-lock-held (*lock*) ,@body)
       (progn ,@body)))

(defun %min (a b) (if (< a b) a b))
(defun %max (a b) (if (> a b) a b))

;;; ----------------------------------------------------------------------------
;;; API
;;; ----------------------------------------------------------------------------
(defun record-query-latency (sql elapsed-ms rows)
  "Point d’accroche : latence d’une requête.
Arguments:
  SQL        — string (peut être long ; on ne le stocke pas)
  ELAPSED-MS — nombre (ms)
  ROWS       — entier (nb de lignes renvoyées)"
  ;; Hook externe
  (when (functionp *on-record-query-latency*)
    (ignore-errors (funcall *on-record-query-latency* sql elapsed-ms rows)))
  ;; Stats internes
  (%with-lock
    (incf *q-count*)
    (incf *q-latency-total-ms* (coerce elapsed-ms 'double-float))
    (incf *q-rows-total* (or rows 0))
    (setf *q-latency-min-ms* (%min *q-latency-min-ms*
                                   (coerce elapsed-ms 'double-float)))
    (setf *q-latency-max-ms* (%max *q-latency-max-ms*
                                   (coerce elapsed-ms 'double-float))))
  nil)

(defun incr-prepare-cache-hit (sql)
  "Point d’accroche : hit du cache PREPARE."
  ;; Hook externe
  (when (functionp *on-prepare-cache-hit*)
    (ignore-errors (funcall *on-prepare-cache-hit* sql)))
  ;; Stat interne
  (%with-lock (incf *prep-hit*))
  nil)

(defun incr-prepare-cache-miss (sql)
  "Point d’accroche : miss du cache PREPARE."
  ;; Hook externe
  (when (functionp *on-prepare-cache-miss*)
    (ignore-errors (funcall *on-prepare-cache-miss* sql)))
  ;; Stat interne
  (%with-lock (incf *prep-miss*))
  nil)

(defun record-pool-stats (in-use wait-ms)
  "Point d’accroche : métriques du pool (si utilisées par Lumen).
Arguments:
  IN-USE   — entier (connexions en cours d’utilisation)
  WAIT-MS  — nombre (ms d’attente pour obtenir une connexion)"
  ;; Hook externe
  (when (functionp *on-record-pool-stats*)
    (ignore-errors (funcall *on-record-pool-stats* in-use wait-ms)))
  ;; Stats internes
  (%with-lock
    (setf *pool-last-in-use* (or in-use 0))
    (incf *pool-wait-total-ms* (coerce (or wait-ms 0.0) 'double-float))
    (incf *pool-samples*))
  nil)

(defun record-slow-query (sql elapsed-ms &key params rows affected)
  "Signale une requête lente. Appelle *on-slow-query* si défini et met à jour
des compteurs internes simples (compte / max)."
  ;; Hook externe
  (when (functionp *on-slow-query*)
    (ignore-errors (funcall *on-slow-query* sql elapsed-ms
                            :params params :rows rows :affected affected)))
  ;; Stats internes
  (%with-lock
    (incf *slow-count*)
    (when (> (coerce elapsed-ms 'double-float) *slow-max-ms*)
      (setf *slow-max-ms* (coerce elapsed-ms 'double-float))))
  ;; Log minimal en fallback (garde l’existant si tu veux)
  (format *error-output*
          "~&[SLOW-QUERY ~,1f ms] ~a~@[  params=~s~]~%"
          elapsed-ms sql (and params (not (endp params)) params))
  nil)

;;; ----------------------------------------------------------------------------
;;; Debug / inspection
;;; ----------------------------------------------------------------------------
(defun metrics-snapshot ()
  "Retourne un PLIST des compteurs internes (pour logging / debug / tests)."
  (%with-lock
    (let* ((q-avg (if (plusp *q-count*)
                      (/ *q-latency-total-ms* *q-count*)
                      0.0d0)))
      (list
       :queries              *q-count*
       :rows-total           *q-rows-total*
       :latency-ms-min       (if (equal *q-latency-min-ms* most-positive-double-float)
				 0.0d0 *q-latency-min-ms*)
       :latency-ms-max       *q-latency-max-ms*
       :latency-ms-avg       q-avg
       :prepare-hit          *prep-hit*
       :prepare-miss         *prep-miss*
       :pool-last-in-use     *pool-last-in-use*
       :pool-wait-total-ms   *pool-wait-total-ms*
       :pool-samples         *pool-samples*
       :slow-count           *slow-count*
       :slow-max-ms          *slow-max-ms*))))

(defun reset-metrics ()
  "Réinitialise les compteurs internes (n’affecte pas les hooks)."
  (%with-lock
    (setf *q-count* 0
          *q-latency-total-ms* 0.0d0
          *q-latency-min-ms* most-positive-double-float
          *q-latency-max-ms* 0.0d0
          *q-rows-total* 0
          *prep-hit* 0
          *prep-miss* 0
          *pool-last-in-use* 0
          *pool-wait-total-ms* 0.0d0
          *pool-samples* 0))
  t)
