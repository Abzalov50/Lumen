(in-package :lumen.test)

(def-suite :session)
(in-suite :session)

(defun make-session-request ()
  (make-instance 'lumen.core.http:request
                 :method "GET"
                 :path "/"
                 :headers nil
                 :query nil
                 :cookies nil
                 :params nil
                 :body-stream nil
                 :context nil))

(test accessors-tolerate-historical-data
  (let ((req (make-session-request)))
    (lumen.core.http:ctx-set!
     req :session
     '(nil
       (nil . "invalide")
       (:ROLE . "admin")
       ("user-id" . "user-42")
       ("DISPLAY-NAME" . "Ada")))
    (is (string= "admin" (lumen.http.session:session-get req "role")))
    (is (string= "user-42" (lumen.http.session:session-get req :user-id)))
    (is (string= "Ada" (lumen.http.session:session-get req "display-name")))
    (is (null (lumen.http.session:session-get req nil)))
    (lumen.http.session:session-del! req :USER-ID)
    (is (null (lumen.http.session:session-get req "user-id")))
    (lumen.http.session:session-del! req "USER-ID")
    (is (null (lumen.http.session:session-get req :user-id)))
    (is (string= "admin" (lumen.http.session:session-get req "role")))
    (lumen.http.session:session-set! req "ROLE" "viewer")
    (is (string= "viewer" (lumen.http.session:session-get req :role)))
    (is (= 1
           (count "role"
                  (lumen.http.session:session-data req)
                  :key #'car
                  :test #'string=)))))

(test cache-does-not-extend-authoritative-expiration
  (let* ((sid "session-expiration-test")
         (expires-at (+ (get-universal-time) 30)))
    (unwind-protect
         (progn
           (lumen.http.session::%session-cache-put
            sid '(("user-id" . "user-42")) expires-at)
           (multiple-value-bind (data found-p)
               (lumen.http.session::%session-cache-get sid)
             (is (not (null found-p)))
             (is (equal data '(("user-id" . "user-42")))))
           (is (= expires-at
                  (lumen.http.session::session-cache-entry-expires-at
                   (gethash sid lumen.http.session::*session-read-cache*)))))
      (lumen.http.session::%session-cache-delete sid))))
