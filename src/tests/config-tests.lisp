(in-package :lumen.test)

(def-suite :config-suite)
(in-suite :config-suite)

(defmacro with-temporary-environment (pairs &body body)
  "Override temporaire de l'ENV via lumen.core.config:*getenv-fn*.
PAIRS peut être '((\"K\" \"V\") ...) ou '((\"K\" . \"V\") ...)."
  (let ((orig (gensym "ORIG"))
        (alist (gensym "ALIST")))
    `(let* ((,orig lumen.core.config:*getenv-fn*)
            (,alist (mapcar (lambda (p)
                              (etypecase p
                                (cons (if (consp (cdr p)) ; ("K" "V") -> ("K" . "V")
                                          (cons (car p) (cadr p))
                                          p))
                                (t (error "Bad env pair: ~S" p))))
                            ',pairs)))
       (unwind-protect
            (let ((lumen.core.config:*getenv-fn*
                    (lambda (name)
                      (or (cdr (assoc name ,alist :test #'string=))
                          (funcall ,orig name)))))
              ,@body)
         (setf lumen.core.config:*getenv-fn* ,orig)))))

(defun %reset-config-stores ()
  "Remet à zéro les tables internes (runtime/file/defaults)."
  (setf (symbol-value (find-symbol "*CFG-RUNTIME*" :lumen.core.config))
	(make-hash-table :test 'equal))
  (setf (symbol-value (find-symbol "*CFG-FILE*"    :lumen.core.config))
	(make-hash-table :test 'equal))
  (setf (symbol-value (find-symbol "*CFG-DEFAULTS*" :lumen.core.config))
	(make-hash-table :test 'equal)))

(test load-env-and-parsers
  (%reset-config-stores)
  (uiop:with-temporary-file (:pathname p :stream s :keep t
                                       :direction :output
                                       :element-type 'character)
    ;; 👉 écrire directement dans le flux temporaire
    (format s "HTTP_PORT=9090~%")
    (format s "HTTPS_ENABLED=true~%")
    (format s "SESSION_TTL=15m~%")
    (format s "CSRF_EXCEPT=\";/sse;/ws\"~%")
    (finish-output s)
    ;; 👉 sépare le code exécuté AVANT/APRÈS la fermeture du stream
    :close-stream
    ;; 👉 maintenant on peut relire le fichier par chemin `p`
    (is-true (lumen.core.config:cfg-load-env p))
    (is (= 9090 (lumen.core.config:cfg-get-int :http/port :default 8080)))
    (is (eq t   (lumen.core.config:cfg-get-bool :https/enabled)))
    (is (= 900  (lumen.core.config:cfg-get-duration :session/ttl :default "0s"))) ; 15m
    (is (equal '("/sse" "/ws") (lumen.core.config:cfg-get-list :csrf/except :sep #\;)))
    ))

(test env-overrides-file
  (%reset-config-stores)
  (uiop:with-temporary-file (:pathname p :stream s :keep t
                                       :direction :output
                                       :element-type 'character)
    (format s "HTTP_PORT=8080~%")
    (finish-output s)
    :close-stream
    (lumen.core.config:cfg-load-env p)
    ;; 👉 override “ENV” simulé via le hook *getenv-fn*
    (with-temporary-environment (("HTTP_PORT" "7777"))
      (is (= 7777 (lumen.core.config:cfg-get-int :http/port))))))

(test runtime-overrides-env
  (%reset-config-stores)
  (with-temporary-environment (("HTTP_PORT" "5555"))
    (cfg-set! :http/port 4242)
    (is (= 4242 (cfg-get-int :http/port)))))

(test boolean-and-list-fallbacks
  (%reset-config-stores)
  (is (eq nil (cfg-get-bool :features/unknown)))
  (is (equal '() (cfg-get-list :anything)))
  (is (= 0 (cfg-get-duration :not/set :default "0s"))))
