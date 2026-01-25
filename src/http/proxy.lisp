(in-package :cl)

(defpackage :lumen.http.proxy
  (:use :cl)
  (:import-from :lumen.utils
   :plist-put)
  (:import-from :lumen.core.http :req-headers :req-ctx :ctx-set! :ctx-get)
  (:import-from :lumen.core.middleware :define-middleware)
  (:export :trust-proxy))

(in-package :lumen.http.proxy)
