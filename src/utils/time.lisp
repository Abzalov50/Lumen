(in-package :cl)

(defpackage :lumen.utils.time
  (:use :cl)
  (:import-from :local-time
                :timestamp
                :timestamp-to-universal
                :parse-timestring)
  (:export
   :+unix->universal-delta+
   :%->universal-time
   :->universal-time
   :universal-time->unix-seconds
   :universal-time->unix-millis
   :unix-seconds->universal-time
   :unix-millis->universal-time))

(in-package :lumen.utils.time)

(defconstant +unix->universal-delta+ 2208988800
  "Décalage entre l’époque UNIX (1970-01-01) et l’époque CL (1900-01-01), en secondes.")

(declaim (inline %->universal-time ->universal-time
                 universal-time->unix-seconds universal-time->unix-millis
                 unix-seconds->universal-time unix-millis->universal-time))

(defun %->universal-time (x)
  "Normalise X en universal-time (entier CL).
   Gère NIL, LOCAL-TIME:TIMESTAMP, universal-time entier, epoch UNIX (sec|ms),
   et string ISO. Retourne NIL si non convertible."
  (cond
    ((null x) nil)

    ;; 1) Déjà un timestamp local-time
    ((typep x 'local-time:timestamp)
     (timestamp-to-universal x))

    ;; 2) Entier : UT, epoch sec, ou epoch ms (on discrimine par ordre de grandeur)
    ((integerp x)
     (cond
       ;; epoch en millisecondes (≈ 1e12+)
       ((> x 10000000000) (+ (floor x 1000) +unix->universal-delta+))
       ;; UT déjà (en 2025 ~3.9e9)
       ((> x 3000000000) x)
       ;; epoch en secondes
       (t (+ x +unix->universal-delta+))))

    ;; 3) String : on tente un parse ISO
    ((stringp x)
     (let ((ts (ignore-errors (parse-timestring x))))
       (and ts (timestamp-to-universal ts))))

    ;; 4) Dernier recours : type timestamp driver
    (t (ignore-errors (timestamp-to-universal x)))))

(defun ->universal-time (x)
  "Alias public de %->universal-time."
  (%->universal-time x))

;; Conversions utilitaires complémentaires
(defun universal-time->unix-seconds (ut)
  (when ut (- ut +unix->universal-delta+)))

(defun universal-time->unix-millis (ut)
  (when ut (* 1000 (universal-time->unix-seconds ut))))

(defun unix-seconds->universal-time (secs)
  (when secs (+ secs +unix->universal-delta+)))

(defun unix-millis->universal-time (ms)
  (when ms (+ (floor ms 1000) +unix->universal-delta+)))
