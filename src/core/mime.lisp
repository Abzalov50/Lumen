(in-package :cl)

(defpackage :lumen.core.mime
  (:use :cl)
  (:export :guess-content-type :content-type-for :safe-file-ext :ext-for-content-type))

(in-package :lumen.core.mime)

(defparameter *allowed-ext-re* "^[a-z0-9]{1,8}$") ; simple, strict

(defparameter *mime-by-ext*
  (let ((ht (make-hash-table :test 'equal)))
    (dolist (p '(("txt" . "text/plain") ("html" . "text/html") ("htm" . "text/html")
                 ("css" . "text/css") ("js" . "text/javascript")
		 ("mjs" . "text/javascript")
                 ("json" . "application/json") ("csv" . "text/csv")
                 ("png" . "image/png") ("jpg" . "image/jpeg") ("jpeg" . "image/jpeg")
                 ("webp" . "image/webp") ("gif" . "image/gif") ("svg" . "image/svg+xml")
                 ("pdf" . "application/pdf") ("zip" . "application/zip")
                 ("mp4" . "video/mp4") ("mov" . "video/quicktime")
                 ("mp3" . "audio/mpeg") ("wav" . "audio/wav")
                 ("woff" . "font/woff") ("woff2" . "font/woff2")
		 ("map"  . "application/json")      ; source maps
             ("ico"  . "image/x-icon") ("avif" . "image/avif")
             ("webm" . "video/webm") ("wasm" . "application/wasm")
             ("xml"  . "application/xml")
             ("webmanifest" . "application/manifest+json")))
      (setf (gethash (car p) ht) (cdr p)))
    ht))

(defun %starts-with (str prefix)
  (and str prefix
       (<= (length prefix) (length str))
       (string= prefix str :end2 (length prefix))))

(defun maybe-add-charset (ct)
  (let ((low (string-downcase (or ct ""))))
    (if (and (%starts-with low "text/")
             (not (search "charset=" low)))
        (format nil "~a; charset=utf-8" ct)
        ct)))

(defun content-type-for (pathname-or-name)
  (let* ((pn (pathname pathname-or-name))
         (ext (ignore-errors (string-downcase (or (pathname-type pn) "")))))
    (or (and ext (> (length ext) 0) (gethash ext *mime-by-ext*))
        "application/octet-stream")))

(defun guess-content-type (pathname-or-name)
  (content-type-for pathname-or-name))


(defparameter *ext-by-type*
  (let ((ht (make-hash-table :test 'equal)))
    (maphash (lambda (ext ct)
               (let ((ctype (string-downcase ct)))
                 (setf (gethash ctype ht) ext)))
             *mime-by-ext*)
    ht))

(defun %ct-base (ct)
  (string-downcase (subseq ct 0 (or (position #\; ct) (length ct)))))

(defun ext-for-content-type (ct)
  "Donne l’extension « préférée » pour un content-type (sans point), ou NIL."
  (gethash (%ct-base (or ct "")) *ext-by-type*))

(defun safe-file-ext (fname ct)
  (let* ((ext1 (ignore-errors (string-downcase (or (pathname-type fname) ""))))
         (ext  (cond
                 ((and ext1 (plusp (length ext1))
                       (cl-ppcre:scan *allowed-ext-re* ext1))
		  ext1)
                 ((lumen.core.mime:ext-for-content-type ct))  ; via mapping inverse
                 (t "bin"))))
    ext))
