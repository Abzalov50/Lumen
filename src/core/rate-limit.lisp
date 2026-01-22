(in-package :cl)

(defpackage :lumen.core.ratelimit
  (:use :cl :alexandria)
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :ctx-get :ctx-set!)
  (:import-from :lumen.utils :-> :->> :str-prefix-p :ensure-header :parse-http-date
		:format-http-date)
  (:import-from :lumen.core.jwt :jwt-encode :jwt-decode)
  (:export :allow?))

(in-package :lumen.core.ratelimit)

(defstruct bucket tokens last) ; seconds in universal-time

(defparameter *buckets* (make-hash-table :test #'equal))

(defun %now-seconds () (get-universal-time))

(defun %key (req window route-key)
  (let* ((h (lumen.core.http:req-headers req))
         (ip (or (cdr (assoc "x-forwarded-for" h :test #'string-equal))
                 (cdr (assoc "x-real-ip"       h :test #'string-equal))
                 "0.0.0.0")))
    (format nil "~A|~A|~A" ip window route-key)))

(defun allow? (req &key (capacity 10) (refill-per-sec 1) (route-key "default"))
  "Token bucket :capacity tokens max, refill :refill-per-sec."
  (let* ((key (%key req capacity route-key))
         (b (or (gethash key *buckets*)
		(setf (gethash key *buckets*)
		      (make-bucket :tokens capacity :last (%now-seconds))))))
    (let* ((now (%now-seconds))
           (elapsed (max 0 (- now (bucket-last b))))
           (newtokens (min capacity (+ (bucket-tokens b) (* refill-per-sec elapsed)))))
      (setf (bucket-tokens b) newtokens
            (bucket-last b) now)
      (if (>= newtokens 1)
          (progn (decf (bucket-tokens b)) t)
          nil))))
