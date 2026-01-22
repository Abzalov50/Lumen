(in-package :cl)

(defpackage :lumen.http.proxy
  (:use :cl)
  (:import-from :lumen.utils
   :plist-put)
  (:import-from :lumen.core.http :req-headers :req-ctx :ctx-set! :ctx-get)
  (:import-from :lumen.core.middleware :define-middleware)
  (:export :trust-proxy))

(in-package :lumen.http.proxy)

(defun %split (s ch) (uiop:split-string s :separator (string ch)))
(defun %trim (s) (string-trim '(#\Space #\Tab #\Return #\Newline) s))
(defun %down (s) (string-downcase s))
(defun %header (req name)
  (cdr (assoc (string-downcase name) (req-headers req) :test #'string=)))

(defun %parse-ipv4 (s)
  (let* ((p (mapcar (lambda (x) (parse-integer x :junk-allowed t)) (%split s #\.))))
    (when (and (= (length p) 4) (every (lambda (n) (and n (<= 0 n 255))) p))
      (reduce (lambda (a n) (+ (ash a 8) n)) p :initial-value 0))))

(defun %cidr-range (cidr)
  (let* ((pos  (position #\/ cidr))
         (ip   (subseq cidr 0 pos))
         (mask (parse-integer (subseq cidr (1+ pos)) :junk-allowed t))
         (n (%parse-ipv4 ip))
         (host-bits (- 32 mask))
         (lo (logand n (ldb (byte 32 host-bits) #xffffffff)))
         (hi (+ lo (1- (ash 1 host-bits)))))
    (cons lo hi)))

(defun %ip-in-cidr-p (ip cidr)
  (let* ((n (%parse-ipv4 ip))
         (rg (%cidr-range cidr)))
    (and n (<= (car rg) n) (<= n (cdr rg)))))

(defun %trusted-p (src specs)
  ;; specs = (:none) | (:always) | list d’IP ou CIDR (IPv4)
  (etypecase specs
    (symbol (cond ((eq specs :none) nil)
		  ((eq specs :always) t)
		  (t nil)))
    (list (some (lambda (x)
                  (if (find #\/ x) (%ip-in-cidr-p src x)
                      (eql (%parse-ipv4 x) (%parse-ipv4 src))))
                specs))))

(defun %parse-forwarded (s)
  "Retourne plist :for :proto :host depuis le premier élément d’un header Forwarded."
  (let* ((first (car (uiop:split-string s :separator ",")))
         (pairs (mapcar #'%trim (uiop:split-string first :separator ";"))))
    (loop with out = '()
          for p in pairs
          for pos = (position #\= p)
          when pos
            do (let ((k (%down (%trim (subseq p 0 pos))))
                     (v (%trim (subseq p (1+ pos)))))
                 (setf out (lumen.utils:plist-put out (intern (string-upcase k) :keyword)
                                      (string-trim '(#\") v))))
          finally (return out))))

(define-middleware trust-proxy (req next)
  "Si :remote-addr ∈ cfg :http/trusted-proxies, remappe ctx :client-ip, :scheme, :host."
  (print "***** TRUST PROXY")
  (let* ((trusted (or (lumen.core.config:cfg-get-list :http/trusted-proxies)
                      :none))
         (src     (or (ctx-get req :remote-addr) "-")))
    (if (not (%trusted-p src trusted))
        (funcall next req)
        (let* ((fwd (%parse-forwarded (%header req "forwarded")))
               (xff (%header req "x-forwarded-for"))
               (xfh (%header req "x-forwarded-host"))
               (xfp (%header req "x-forwarded-proto"))
               (real-ip   (or (getf fwd :|FOR|)
                              (and xff (%trim (car (uiop:split-string xff :separator ","))))
                              src))
               (real-prot (or (getf fwd :|PROTO|) xfp (if (ctx-get req :secure) "https" "http")))
               (real-host (or (getf fwd :|HOST|)  xfh)))
          (when real-ip   (ctx-set! req :client-ip real-ip))
          (when real-prot (ctx-set! req :scheme real-prot))
          (when real-host (ctx-set! req :host real-host))
          (funcall next req)))))
