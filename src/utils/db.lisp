(in-package :lumen.utils)

(defun db-network-error-p (condition)
  "Détecte les erreurs réseau PostgreSQL typiques : socket morte, timeout,
connexion fermée côté serveur, coupure réseau, NAT expiré, etc."
  (let ((msg (string-downcase (princ-to-string condition))))
    (or (search "couldn't write" msg)
        (search "couldn't read" msg)
        (search "socket" msg)
        (search "broken pipe" msg)
        (search "connection reset" msg)
        (search "connection refused" msg)
        (search "connection timed out" msg)
        (search "server closed the connection" msg)
        (search "terminating connection" msg)
        (search "end of file" msg)
        (search "eof" msg)
        (search "nom réseau" msg)
        (search "n’est plus disponible" msg)
        (search "n'est plus disponible" msg))))

(defun reset-current-db-connection ()
  "Force l'abandon de la connexion Postmodern courante et du cache de prepared plans.

Important avec une BD distante : une connexion TCP peut rester référencée
côté Lisp alors qu'elle est déjà morte côté réseau.
"
  (ignore-errors
    (when (find-package :lumen.data.prepare)
      (funcall (find-symbol "RESET-PREPARE-CACHE" :lumen.data.prepare))))

  (ignore-errors
    (postmodern:disconnect-toplevel))
  t)

(defun run-db-with-reconnect (thunk &key (retries 3) (sleep-ms 500))
  "Exécute THUNK avec reconnexion automatique si la connexion DB est morte."
  (loop for attempt from 0 do
    (handler-case
        (let ((values (multiple-value-list (funcall thunk))))
          (return-from run-db-with-reconnect
            (values-list values)))

      (error (e)
        (if (and (< attempt retries)
                 (db-network-error-p e))
            (progn
              (format t "~&[DB] Connexion PostgreSQL morte. Reconnexion (~A/~A) : ~A~%"
                      (1+ attempt) retries e)
              (reset-current-db-connection)
              (sleep (/ sleep-ms 1000.0)))
            (error e))))))
