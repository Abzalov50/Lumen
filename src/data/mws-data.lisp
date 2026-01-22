(in-package :cl)

(defpackage :lumen.data.mws
  (:use :cl :alexandria)
  (:import-from :lumen.core.config :cfg-get :cfg-get-bool :cfg-get-int)
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :req-headers :ctx-get :ctx-set!)
  (:import-from :lumen.utils :-> :->> :str-prefix-p :ensure-header :parse-http-date
		:format-http-date :alist-get)
  (:import-from :lumen.core.middleware :define-middleware)
  (:import-from :lumen.data.tenant :tenant-code-by-id :tenant-id-for-host
   :tenant-id-by-code :normalize-host)
  (:export :tenant-from-host))

(in-package :lumen.data.mws)

(defun %maybe-real-host-from-proxy (req)
  "Si un mw 'trust-proxy' alimente ctx [:real-host], l’utiliser en priorité."
  (or (ctx-get req :real-host)
      (cdr (assoc "x-forwarded-host" (req-headers req) :test #'string-equal))
      (cdr (assoc "x-real-host"      (req-headers req) :test #'string-equal))))

(defun %host-header (req)
  (or (%maybe-real-host-from-proxy req)
      (cdr (assoc "host" (req-headers req) :test #'string-equal))))

;;;; -----------------------------------------------------------------------------
;;;; Middleware (drop-in): tenant-from-host
;;;; -----------------------------------------------------------------------------
(define-middleware tenant-from-host (req next)
  "Résout le tenant à partir du Host (ou X-Forwarded-Host si trust-proxy) et
   renseigne ctx[:tenant-id] + ctx[:tenant-code].
   - Comportement configurable via ENV :
     TENANT_REQUIRE_HOST=1 (défaut 0) → si T et non résolu, 404.
     TENANT_ALLOW_HEADERS=1 (défaut 1) → si T, fallback X-Tenant-Code / X-Tenant-Id."
  (print "IN TENANT FROM HOST")
  (flet ((fallback-from-headers ()
           (let* ((h (req-headers req))
                  (tid (lumen.utils:alist-get h "x-tenant-id"))
                  (tcd (lumen.utils:alist-get h "x-tenant-code")))
             (cond
               (tid
                (values tid (tenant-code-by-id tid)))
               (tcd
                (let ((tid2 (tenant-id-by-code tcd)))
                  (when tid2 (values tid2 tcd))))
               (t (values nil nil))))))
    (let* ((require-host (lumen.core.config:cfg-get-bool :tenant/require-host
							 :default nil))
           (allow-hdrs   (lumen.core.config:cfg-get-bool :tenant/allow-headers
							 :default t))
           (host-raw     (%host-header req))
           (host         (normalize-host host-raw))
           (tid          (and host (tenant-id-for-host host)))
           (code         (and tid (tenant-code-by-id tid))))
      (format t "~&HOST RAW: ~A~%HOST: ~A~%" host-raw host)
      (format t "~&TID: ~A~%CODE: ~A~%" tid code)
      (multiple-value-bind (tid2 code2)
          (if (or tid code)
              (values tid code)
              (and allow-hdrs (fallback-from-headers)))
        (cond
          (tid2
           ;; installe dans le contexte
           (ctx-set! req :tenant-id   tid2)
           (when code2 (ctx-set! req :tenant-code code2))
           (funcall next req))
          (require-host
           ;; exigeant → 404
           (make-instance 'lumen.core.http:response
                          :status 404
                          :headers '(("Content-Type" . "application/json; charset=utf-8")
                                     ("Cache-Control" . "no-store"))
                          :body "{\"error\":{\"type\":\"tenant\",\"message\":\"unknown tenant for host\"}}"))
          (t
           ;; permissif → on laisse passer (ex: /health), sans installer de tenant
           (funcall next req)))))))
