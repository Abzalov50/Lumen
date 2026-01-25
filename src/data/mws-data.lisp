(in-package :cl)

(defpackage :lumen.data.mws
  (:use :cl :alexandria)
  (:import-from :lumen.core.config :cfg-get :cfg-get-bool :cfg-get-int)
  (:import-from :lumen.core.http :request :response :resp-body :resp-status
   :resp-headers :respond-500 :respond-404 :req-query :req-headers :ctx-get :ctx-set!)
  (:import-from :lumen.utils :-> :->> :str-prefix-p :ensure-header :parse-http-date
		:format-http-date :alist-get)
  (:import-from :lumen.core.middleware :defmiddleware)
  (:import-from :lumen.data.tenant :tenant-code-by-id :tenant-id-for-host
   :tenant-id-by-code :normalize-host)
  (:export :tenant-from-host))

(in-package :lumen.data.mws)

