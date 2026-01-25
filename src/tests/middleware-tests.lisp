(in-package :lumen.test)

(def-suite :middleware)
(in-suite :middleware)

;;; ---------------------------
;;; Helpers
;;; ---------------------------

(defun make-req (&key (method "GET") (path "/"))
  (make-instance 'lumen.core.http:request
                 :method method
                 :path path
                 :headers nil
                 :query nil
                 :cookies nil
                 :params nil
                 :body-stream nil
                 :context (list)))

(defun make-resp (&key (status 200) (headers nil) (body ""))
  (make-instance 'lumen.core.http:response
                 :status status
                 :headers headers
                 :body body))

(defun contains-substr? (haystack needle)
  (and haystack (search needle haystack :test #'char-equal)))

;;; ---------------------------
;;; Tests parse-query-string-to-alist
;;; ---------------------------

(test parse-query/empty
  (is (null (parse-query-string-to-alist "")))
  (is (null (parse-query-string-to-alist nil))))

(test parse-query/simple
  (is (equal (parse-query-string-to-alist "a=1&b=2")
             '(("a" . "1") ("b" . "2")))))

(test parse-query/repeated-keys
  (is (equal (parse-query-string-to-alist "x=1&x=2")
             '(("x" . "1") ("x" . "2")))))

(test parse-query/plus-and-percent
  (is (equal (parse-query-string-to-alist "y=abc+def")
             '(("y" . "abc def"))))
  (is (equal (parse-query-string-to-alist "q=hello%20world%21")
             '(("q" . "hello world!")))))

(test parse-query/missing-values
  (is (equal (parse-query-string-to-alist "a=") '(("a" . ""))))
  (is (equal (parse-query-string-to-alist "a")  '(("a" . "")))))

;;; ---------------------------
;;; Tests logger middleware
;;; ---------------------------

(test logger/returns-response-and-logs-when-debug
  (let* ((req  (make-req :method "GET" :path "/hello"))
         (resp (make-resp :status 201 :body "ok"))
         (next (lambda (r)
                 (declare (ignore r))
                 ;; simule un petit temps de traitement
                 (sleep 0.01)
                 resp))
         (out  (with-output-to-string (s)
                 (let ((*standard-output* s)
                       (lumen.core.middleware::debug-p t)
		       )
                   (funcall (funcall (lumen.core.middleware:logger) next) req)))))
    ;; Renvoie bien la réponse
    (is (typep (funcall (funcall (lumen.core.middleware:logger) next) req) 'lumen.core.http:response))
    ;; Le log contient méthode, path, status et " ms)"
    (is (contains-substr? out "[lumen]"))
    (is (contains-substr? out "GET"))
    (is (contains-substr? out "/hello"))
    (is (contains-substr? out "201"))
    (is (contains-substr? out " ms)"))))

(test logger/no-log-when-debug-nil
  (let* ((req  (make-req :method "POST" :path "/api/x"))
         (resp (make-resp :status 204))
         (next (lambda (r) (declare (ignore r)) resp))
         (out  (with-output-to-string (s)
                 (let ((*standard-output* s)
                       (lumen.core.middleware::debug-p nil)
		       )
                   (funcall (funcall (lumen.core.middleware:logger) next) req)))))
    (is (string= out ""))))

;;; ---------------------------
;;; Runner
;;; ---------------------------

(defun run-tests ()
  (fiveam:run! :middleware))
