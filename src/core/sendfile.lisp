(in-package :cl)

(defpackage :lumen.core.sendfile
  (:use :cl)
  (:export :stream-bytes :stream-file-segment))

(in-package :lumen.core.sendfile)

(defun stream-bytes (out in &key (count nil) (offset 0) (bufsize 65536))
  "Copie COUNT octets (ou jusqu’à EOF si NIL) de IN vers OUT, à partir de OFFSET."
  (when offset (file-position in offset))
  (let* ((buf (make-array bufsize :element-type '(unsigned-byte 8)))
         (left count)
         (sent 0))
    (loop
      for need = (or (and left (min left bufsize)) bufsize)
      for n = (read-sequence buf in :end need)
      do (when (or (null n) (= n 0)) (return))
         (write-sequence buf out :end n)
         (incf sent n)
         (when left (decf left n) (when (<= left 0) (return))))
    (finish-output out)
    sent))

(defun stream-file-segment (out path start end)
  "Envoie [start..end] inclus (en octets) du fichier PATH vers OUT."
  (with-open-file (in path :direction :input :element-type '(unsigned-byte 8))
    (stream-bytes out in :count (1+ (- end start)) :offset start)))
