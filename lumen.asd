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
     ;;(:file "packages")
     (:module "utils"
      :components
      ((:file "utils")
       (:file "time")
       (:file "json")))
     (:module "core"
      :components
      ((:file "package")
       (:file "config")
       (:file "context")
       (:file "trace")
       (:file "scheduler")
       (:file "mime")
       (:file "validate")
       (:file "error")
       (:file "body")
       (:file "http")
       
       (:file "router")      ;; Le routeur peut maintenant utiliser defmiddleware
       (:file "jwt")
       (:file "auth")        ;; Auth peut utiliser defmiddleware
       (:file "rate-limit")       
       (:file "shutdown")
       
       (:file "sendfile")
       (:file "http-range")
       (:file "tls-acme")       
       (:file "validation")

       ;; --- SOCLE LUMEN 2.0 (DÉPLACÉ ICI) ---
       ;; Server utilise tout, donc à la fin
       (:file "server")
       (:file "pipeline")    ;; 1. Base (Class middleware)
       
       (:file "middleware" :depends-on ("http" "trace"))  ;; 2. Macros (defmiddleware) + Implems standards
       ))
     (:module "data"
      :depends-on ("core")
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
       ))     
     (:module "http"
      :depends-on ("data" "core")
      :components
      ((:file "crud")
       (:file "proxy")
       (:file "session")
       ))
     (:module "extras"
      :components
      ((:file "forms")
       (:file "router-extras")))
     (:module "app"
      :components
      ((:file "app")))
     (:module "realtime"
      :components
      ((:file "ws")
       (:file "pubsub")))
     (:module "obs"
      :components
      ((:file "metrics")))
     (:module "view"
      :components
      ((:file "html")
       (:file "components")
       (:file "form")
       (:file "table")
       (:file "htmx")))
     (:module "dev"
      :depends-on ("core" "data" "http")
      :components
      ((:file "cli")
       (:file "scaffold")
       (:file "reload")
       (:file "test-helpers")
       (:file "module")
       (:file "inspector")
       ))
     (:module "docs"
      :components
      ((:file "openapi")))
     (:module "modules"
      :components
      ((:file "socle")

       (:module "auth"
	:components
		((:file "view")
		 (:file "service")
		 (:file "module")))

       (:module "admin"
	:components
		((:file "utils")
		 (:file "registry")
		 (:file "introspection")		 
		 (:file "form")
		 (:file "grid")
		 (:file "actions")
		 (:file "view")
		 (:file "dashboard")
		 (:file "audit")
		 (:file "tools")
		 (:file "module")
		 ))
       (:file "landing")
       ))
     (:module "tests"
      :components
      ((:file "util")
       (:file "package")       
       ;;(:file "middleware-tests")
       (:file "util-tests")
       (:file "config-tests")
       (:file "validate-tests")
       (:file "db-tests")
       (:file "dao-tests")
       (:file "repo-tests")
       (:file "form-tests")))
     )))
  :depends-on (alexandria usocket bordeaux-threads flexi-streams cl-ppcre cl-json
			  local-time trivial-utf-8 fiveam salza2 ironclad cl-base64
			  cl+ssl uuid postmodern str dexador spinneret parenscript
			  quri))
