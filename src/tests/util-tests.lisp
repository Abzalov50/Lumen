(in-package :lumen.test)

(def-suite :utils)
(in-suite :utils)

(test prefixes
  (is (str-prefix-p "/api" "/api/users"))
  (is (not (str-prefix-p "/api/" "/app")))
  (is (str-suffix-p ".js" "app.min.js"))
  (is (str-contains-p "bar" "foobarbaz")))

(test slashes
  (is (string= (ensure-trailing "/path" #\/) "/path/"))
  (is (string= (ensure-leading "path" #\/) "/path"))
  (is (ends-with-slash-p "/x/"))
  (is (starts-with-slash-p "/x")))

(test alists
  (let* ((h '(("content-type" . "text/plain"))))
    (is (string= (alist-get h "content-type") "text/plain"))
    (let ((h2 (ensure-header h "Content-Type" "text/html")))
      (is (string= (alist-get h2 "content-type") "text/html")))
    (let ((h3 (ensure-header h "x-foo" "bar")))
      (is (string= (alist-get h3 "x-foo") "bar")))))

;;; ---------------------------
;;; Runner
;;; ---------------------------
(defun run-tests ()
  (fiveam:run! :utils))
