;;; -*- mode: lisp -*-

(defpackage :lumen-app
  (:use :cl :asdf))
(in-package :lumen-app)

(defsystem lumen
  :author "Arnold N'GORAN"
  :license "MIT"
  :components
  ((:module "src"
    :components
    ((:file "bootstrap-features")
     (:file "packages")
     (:module "utils"
      :components
      ((:file "utils")
       (:file "time")
       (:file "json")))
     (:module "core"
      :components
      ((:file "package")
       (:file "config")
       (:file "mime")
       (:file "validate")
       (:file "error")
       (:file "http")
       (:file "session")
       (:file "router")
       (:file "jwt")
       (:file "auth")
       (:file "rate-limit")
       (:file "body")
       (:file "shutdown")
       (:file "server")
       (:file "sendfile")
       (:file "http-range")
       (:file "tls-acme")
       (:file "middleware")
       ))
     (:module "data"
      :components
      ((:file "config")
       (:file "metrics")
       (:file "errors")
       (:file "prepare-cache")
       (:file "db")
       (:file "dao")
       (:file "repo")
       (:file "tenant")
       (:file "repo-core")
       (:file "migrations")
       (:file "mws-data")))     
     (:module "http"
      :components
      ((:file "crud")
       (:file "openapi")
       (:file "proxy")))
     (:module "extras"
      :components
      ((:file "forms")
       (:file "router-extras")))
     (:module "mvc"
      :components
      ((:file "resource")
       (:file "controller")
       (:file "view")))     
     (:module "realtime"
      :components
      ((:file "ws")
       (:file "pubsub")))
     (:module "obs"
      :components
      ((:file "metrics")))
     (:module "dev"
      :components
      ((:file "cli")
       (:file "scaffold")
       (:file "reload")
       (:file "test-helpers")))
     (:module "docs"
      :components
      ((:file "openapi")))
     (:module "tests"
      :components
      ((:file "util")
       (:file "package")       
       (:file "middleware-tests")
       (:file "util-tests")
       (:file "config-tests")
       (:file "validate-tests")
       (:file "db-tests")
       (:file "dao-tests")
       (:file "repo-tests")
       (:file "form-tests")))
     )))
  :depends-on (alexandria usocket bordeaux-threads flexi-streams cl-ppcre cl-json local-time trivial-utf-8 fiveam salza2 ironclad cl-base64 cl+ssl uuid postmodern str))
