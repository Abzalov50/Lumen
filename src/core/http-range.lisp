(in-package :cl)

(defpackage :lumen.core.http-range
  (:use :cl)
  (:import-from :lumen.core.mime :guess-content-type :content-type-for)
  (:import-from :lumen.core.http
    :request :respond-404 :req-headers :req-method :response)
  (:export
    :respond-file
   :respond-file-range
   :resolve-file-path))

(in-package :lumen.core.http-range)

(defun %file-size (pn)
  (with-open-file (in pn :direction :input :element-type '(unsigned-byte 8))
    (file-length in)))

(defun %http-date (pn)
  (ignore-errors
    (let ((ut (file-write-date pn)))
      (when ut (lumen.utils:format-http-date ut)))))

(defun %weak-etag-from-file (pn)
  (ignore-errors
    (let* ((size (%file-size pn))
           (ut   (file-write-date pn)))
      (format nil "W/~S" (logxor size (or ut 0))))))

(defun read-file-into-byte-vector (pathname)
  "Lit un fichier entier et retourne un vecteur d'octets (unsigned-byte 8)."
  (with-open-file (stream pathname :direction :input :element-type '(unsigned-byte 8))
    (let ((seq (make-array (file-length stream) :element-type '(unsigned-byte 8))))
      (read-sequence seq stream)
      seq)))

(defun %ensure-h (hdrs k v)
  (lumen.utils:ensure-header hdrs k v))

(defun %parse-range (range-header total)
  "Parse un header Range «bytes=start-end», retourne (values start end) ou NIL."
  (when (and range-header (plusp (length range-header)))
    (multiple-value-bind (m regs)
        (cl-ppcre:scan-to-strings "^bytes=([0-9]*)-([0-9]*)$"
                                   (string-downcase range-header))
      (declare (ignore m))
      (when regs
        (let* ((sraw (aref regs 0))
               (eraw (aref regs 1)))
          (cond
            ;; bytes=START-
            ((and (> (length sraw) 0) (= (length eraw) 0))
             (let ((s (parse-integer sraw :junk-allowed t)))
               (when (and s (< s total))
                 (values s (1- total)))))
            ;; bytes=-SUFFIX
            ((and (= (length sraw) 0) (> (length eraw) 0))
             (let* ((suffix (parse-integer eraw :junk-allowed t))
                    (len    (min suffix total))
                    (start  (max 0 (- total len))))
               (values start (1- total))))
            ;; bytes=START-END
            ((and (> (length sraw) 0) (> (length eraw) 0))
             (let ((s (parse-integer sraw :junk-allowed t))
                   (e (parse-integer eraw :junk-allowed t)))
               (when (and s e (<= s e) (< s total))
                 (values s (min e (1- total))))))
            (t nil)))))))
  
(defun resolve-file-path (fid &key (uploads-dir (lumen.core.config:cfg-get "FILES_UPLOADS_DIR")))
  "Résout le pathname absolu du fichier identifié par FID.
   Ici, on suppose que les pièces jointes sont stockées sous
   *files-dir*/courriers/ et nommées par leur identifiant.
   À adapter si tu as une table DB qui mappe fid → chemin."

  (let* ((safe-fid (string-downcase (princ-to-string fid))) ; force en string safe
         ;; on évite toute injection de ../
         (rel (substitute #\_ #\/ safe-fid)))
    (merge-pathnames rel uploads-dir)))

(defun respond-file-range (req pn start end
                            &key content-type etag last-modified-rfc1123 cache-secs)
  "Répond en 206 pour le segment [start,end] inclus du fichier PN."
  (let* ((total (%file-size pn))
         (start (max 0 (min start (max 0 (1- total)))))
         (end   (max start (min end (1- total))))
         (len   (max 0 (1+ (- end start))))
         (ct    (or content-type (guess-content-type pn)))
         (hdrs  `(("Content-Type"   . ,ct)
                  ("Accept-Ranges"  . "bytes")
                  ("Content-Range"  . ,(format nil "bytes ~D-~D/~D" start end total))
                  ("Content-Length" . ,(write-to-string len))
                  ,@(when cache-secs
                      `(("Cache-Control" . ,(format nil "public, max-age=~D" cache-secs))))
                  ,@(when last-modified-rfc1123
                      `(("Last-Modified" . ,last-modified-rfc1123)))
                  ,@(when etag
                      `(("ETag" . ,etag))))))
    (if (string= (lumen.core.http:req-method req) "HEAD")
        (make-instance 'lumen.core.http:response
                       :status 206 :headers hdrs :body "")
      (make-instance 'lumen.core.http:response
                     :status 206
                     :headers hdrs
                     :body (lambda (send)
                             (with-open-file (in pn :direction :input
                                                    :element-type '(unsigned-byte 8))
                               (file-position in start)
                               (let ((left len)
                                     (buf  (make-array (min len 65536)
                                                       :element-type '(unsigned-byte 8))))
                                 (loop while (> left 0) do
                                   (let* ((n  (min left (length buf)))
                                          (rd (read-sequence buf in :end n)))
                                     (when (or (null rd) (= rd 0)) (return))
                                     (funcall send (if (= rd (length buf))
                                                       buf
                                                       (subseq buf 0 rd)))
                                     (decf left rd))))))))))

(defun respond-file (req pn &key content-type headers last-modified-rfc1123 etag cache-secs)
  "Répond 200 (entier) ou 206 (Range) selon le header Range. Respecte HEAD."
  (unless (probe-file pn)
    (return-from respond-file (lumen.core.http:respond-404 "File not found")))
  (let* ((total (%file-size pn))
         (ct    (or content-type (guess-content-type pn)))
         (lm    (or last-modified-rfc1123 (%http-date pn)))
         (tg    (or etag (%weak-etag-from-file pn)))
         (range (cdr (assoc "range" (lumen.core.http:req-headers req)
                            :test #'string-equal))))
    (format t "~&TOTAl: ~A~%RANGE: ~A~%LM: ~A~%TAG: ~A~%" total range lm tg)
    (multiple-value-bind (s e) (%parse-range range total)
      (format t "~&S: ~A~%E: ~A~%" s e)
      (if (and s e)
          ;; 206
          (respond-file-range req pn s e
                              :content-type ct :etag tg
                              :last-modified-rfc1123 lm :cache-secs cache-secs)
        ;; 200 plein
        (let* ((hdrs `(("Content-Type"   . ,ct)
                       ("Accept-Ranges"  . "bytes")
                       ("Content-Length" . ,(write-to-string total))
		       ,@(when headers headers)
                       ,@(when cache-secs
                           `(("Cache-Control" . ,(format nil "public, max-age=~D" cache-secs))))
                       ,@(when lm `(("Last-Modified" . ,lm)))
                       ,@(when tg `(("ETag" . ,tg))))))
          (if (string= (lumen.core.http:req-method req) "HEAD")
              (make-instance 'lumen.core.http:response
                             :status 200 :headers hdrs :body "")
            (make-instance 'lumen.core.http:response
                           :status 200
                           :headers hdrs
                           :body (lambda (send)
                                   (with-open-file (in pn :direction :input
                                                          :element-type '(unsigned-byte 8))
                                     (let ((buf (make-array 65536 :element-type '(unsigned-byte 8))))
                                       (loop
                                         (let ((rd (read-sequence buf in)))
                                           (when (or (null rd) (= rd 0)) (return))
                                           (funcall send (if (= rd (length buf))
                                                             buf
                                                             (subseq buf 0 rd)))))))))))))))

(defun respond-file (req pn &key content-type headers last-modified-rfc1123 etag cache-secs)
  "Répond 200 (entier) ou 206 (Range) selon le header Range. Respecte HEAD."
  (unless (probe-file pn)
    (return-from respond-file (lumen.core.http:respond-404 "File not found")))
  (let* ((total (%file-size pn))
         (ct    (or content-type (guess-content-type pn)))
         (lm    (or last-modified-rfc1123 (%http-date pn)))
         (tg    (or etag (%weak-etag-from-file pn)))
         (range (cdr (assoc "range" (lumen.core.http:req-headers req)
                            :test #'string-equal))))
    (format t "~&TOTAl: ~A~%RANGE: ~A~%LM: ~A~%TAG: ~A~%" total range lm tg)
    (multiple-value-bind (s e) (%parse-range range total)
      (format t "~&S: ~A~%E: ~A~%" s e)
      (if (and s e)
          ;; 206
          (respond-file-range req pn s e
                              :content-type ct :etag tg
                              :last-modified-rfc1123 lm :cache-secs cache-secs)
        ;; 200 plein
        (let* ((hdrs `(("Content-Type"   . ,ct)
                       ("Accept-Ranges"  . "bytes")
                       ("Content-Length" . ,(write-to-string total))
		       ,@(when headers headers)
                       ,@(when cache-secs
                           `(("Cache-Control" . ,(format nil "public, max-age=~D" cache-secs))))
                       ,@(when lm `(("Last-Modified" . ,lm)))
                       ,@(when tg `(("ETag" . ,tg))))))
          (if (string= (lumen.core.http:req-method req) "HEAD")
              (make-instance 'lumen.core.http:response
                             :status 200 :headers hdrs :body "")
            (make-instance 'lumen.core.http:response
                           :status 200
                           :headers hdrs
                           :body (read-file-into-byte-vector pn))))))))
