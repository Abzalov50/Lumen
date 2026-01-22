;;;; --- lumen-raw.lisp ---  (SBCL + sb-bsd-sockets)  ---
;;;; Serveur HTTP minimal, robuste, Windows-friendly (pas d'usocket, pas de flexi)

(defpackage :lumen.raw
  (:use :cl)
  (:export :start-raw :stop-raw))
(in-package :lumen.raw)

;; ----------------------- Etat serveur -----------------------
(defvar *sock* nil)
(defvar *thr*  nil)
(defvar *running* nil)

;; ---------- utils (octets) ----------
(defun %str->bytes (s) (trivial-utf-8:string-to-utf-8-bytes (or s "")))
(defun %bytes->str (v) (trivial-utf-8:utf-8-bytes-to-string v))
(defparameter +crlfcrlf+ (coerce #(13 10 13 10) '(simple-array (unsigned-byte 8) (*)))) ; "\r\n\r\n"

(defun recv-until (sock sep &key (chunk 4096) (limit (* 64 1024)))
  "Lit des octets sur SOCK jusqu'à trouver SEP (vecteur d'octets) ou atteindre LIMIT.
   Retourne un vecteur d'octets contenant les en-têtes (terminés par SEP)."
  (let ((buf (make-array chunk
                         :element-type '(unsigned-byte 8)
                         :fill-pointer 0
                         :adjustable t)))
    (labels ((capacity () (array-dimension buf 0)))
      (loop
        ;; SEP trouvé ?
        (let ((pos (search sep buf)))
	  (format t "~&POS: ~A~%" pos)
          (when pos
            (return (subseq buf 0 (+ pos (length sep))))))
        ;; limite dure
        (when (>= (length buf) limit)
          (error "Request headers too large"))
        ;; lire un bloc
        (let* ((need (min chunk (- limit (length buf))))
               (tmp  (make-array need :element-type '(unsigned-byte 8)))
               (n    (sb-bsd-sockets:socket-receive sock tmp need)))
	  (format t "~&n: ~A~%" n)
          (when (or (null n) (<= n 0))
            (error "Client closed during headers read"))
          ;; append tmp[0..n)
          (let* ((old (length buf))
                 (new (+ old n)))
	    (format t "~&OLD: ~A~%" old)
	  (format t "~&NEW: ~A~%" new)
            (when (> new (capacity))
              (adjust-array buf (max new (* 2 (capacity))) :fill-pointer old))
            (replace buf tmp :start1 old :end2 n)
            (setf (fill-pointer buf) new)))))))

(defun send-all (sock bytes &key (start 0) (end (length bytes)))
  "Envoie tous les octets [start,end) (bouclé avec :start/:end)."
  (loop with i = start
        while (< i end) do
          (let ((sent (sb-bsd-sockets:socket-send sock bytes :start i :end end)))
            (when (or (null sent) (<= sent 0)) (error "send failed"))
            (incf i sent))))

(defun build-response (status reason headers body)
  "Construit headers+CRLFCRLF+body en OCTETS."
  (let* ((bodyb (%str->bytes body))
         (head  (with-output-to-string (s)
                  (format s "HTTP/1.1 ~D ~A\r\n" status reason)
                  (format s "Content-Length: ~D\r\n" (length bodyb))
                  (format s "Connection: close\r\n")
                  (dolist (h headers) (format s "~A: ~A\r\n" (car h) (cdr h)))
                  (format s "\r\n")))
         (headb (%str->bytes head))
         (out   (make-array (+ (length headb) (length bodyb))
                            :element-type '(unsigned-byte 8))))
    (replace out headb)
    (replace out bodyb :start1 (length headb))
    out))

(defun reason (code)
  (case code
    (200 "OK") (201 "Created") (204 "No Content")
    (400 "Bad Request") (401 "Unauthorized") (403 "Forbidden")
    (404 "Not Found") (500 "Internal Server Error")
    (t "OK")))

;; ---------- parsing request line ----------
(defun parse-request-line (bytes)
  "Retourne (values method path http) à partir des octets du début jusqu'au 1er CRLF."
  (let* ((pos (search (coerce #(13 10) '(simple-array (unsigned-byte 8) (*))) bytes))
         (lineb (if pos (subseq bytes 0 pos) bytes))
         (line  (%bytes->str lineb))
         (sp1   (position #\Space line))
         (sp2   (and sp1 (position #\Space line :start (1+ sp1)))))
    (values (and sp1 (subseq line 0 sp1))
            (and sp1 sp2 (subseq line (1+ sp1) sp2))
            (and sp2 (subseq line (1+ sp2))))))

;; ---------- client handler ----------
(defun handle-client (client)
  (unwind-protect
      ;;(handler-case
          (progn
            ;; 1) Lire jusqu'à CRLFCRLF (request-line + headers)
	    (print "OKKKKK")
            (let* ((hdrs-bytes (recv-until client +crlfcrlf+))
		   (x (print "XXX"))
                   method path http)
              (multiple-value-setq (method path http) (parse-request-line hdrs-bytes))
              ;; 2) Router minimal
	      (print "OKKKKK 1")
              (let* ((code (if (and (string= method "GET") (string= path "/health")) 200 404))
                     (body (if (= code 200) "ok" "not found"))
                     (headers '(("Content-Type" . "text/plain; charset=utf-8")))
                     (resp (build-response code (reason code) headers body)))
                ;; 3) Envoyer *tous* les octets
                (send-all client resp)
                ;; 4) Shutdown sortie (FIN propre) puis close
                (ignore-errors (sb-bsd-sockets:socket-shutdown client :direction :output)))))

    #|
        (error (e)
          ;; En cas d’erreur, envoyer un 500 minimal en octets
          (ignore-errors
            (let ((resp (build-response 500 (reason 500) nil "")))
              (send-all client resp)
    (sb-bsd-sockets:socket-shutdown client :direction :output)))))
    |#
    
    (ignore-errors (sb-bsd-sockets:socket-close client))))

(defun accept-loop ()
  (setf *running* t)
  (loop while *running* do
        (handler-case
            (let ((client (sb-bsd-sockets:socket-accept *sock*)))
              (sb-thread:make-thread (lambda () (handle-client client))
                                     :name "lumen-raw-client"))
          (error (e)
            (format *error-output* "~&[raw] accept error: ~A~%" e)))))

;; ----------------------- API publique -----------------------
(defun start-raw (&key (port 54321))
  "Démarre le serveur (IPv4 only: 127.0.0.1)."
  (when *sock* (ignore-errors (sb-bsd-sockets:socket-close *sock*)))
  (setf *sock* (make-instance 'sb-bsd-sockets:inet-socket
                              :type :stream :protocol :tcp))
  (sb-bsd-sockets:sockopt-reuse-address *sock*)
  (sb-bsd-sockets:socket-bind *sock* #(127 0 0 1) port)
  (sb-bsd-sockets:socket-listen *sock* 128)
  (format t "~&[raw] listening on 127.0.0.1:~A~%"
          port)
  (setf *thr* (sb-thread:make-thread #'accept-loop
                                     :name (format nil "raw-accept-~A" port)))
  t)

(defun stop-raw ()
  (setf *running* nil)
  (when *sock*
    (ignore-errors (sb-bsd-sockets:socket-close *sock*))
    (setf *sock* nil))
  t)
