(defpackage :lumen.modules.socle
  (:use :cl :lumen.core.router :lumen.utils :lumen.core.http
   :lumen.core.middleware :lumen.data.db :lumen.data.dao
   :lumen.data.repo.core :lumen.data.repo.query :lumen.core.auth)
  (:import-from :lumen.http.crud :make-entity-crud-guard)
  (:import-from :lumen.core.mime #:content-type-for)
  (:import-from :lumen.dev.module #:defmodule)
  (:export :tenant :tenant-domain :user
	   :%hash-password-in-payload :%sanitize-user))

(in-package :lumen.modules.socle)

(defmodule :socle-test
  :path-prefix "/api/socle"
  :doc "Module Fondamental : Tenants, Domaines et Utilisateurs."

  ;; --- MODÈLE DE DONNÉES ---
  :entities
  ((tenant
    :table "tenants"
    :primary-key :id
    :fields
    ((:col :id       :type :uuid   :readonly? t :hidden? t)
     (:col :code     :type :string :required? t :label "Code Organisme" :unique t)
     (:col :name     :type :string :required? t :label "Raison Sociale")
     (:col :status   :type :string :default "active")
     (:col :plan     :type :string :default "free"))) ;; Utile pour le SaaS

   (tenant-domain
    :table "tenant_domains"
    :primary-key :id
    :fields
    ((:col :id        :type :uuid   :readonly? t :hidden? t)
     (:col :tenant_id :type :uuid   :required? t :references "tenants")
     (:col :host      :type :string :required? t :label "Nom de domaine" :unique t)
     (:col :is_primary :type :boolean :default t)))

   (user
    :table "users"
    :primary-key :id
    :fields
    ((:col :id        :type :uuid   :readonly? t :hidden? t)
     (:col :tenant_id :type :uuid   :readonly? t :hidden? t) ;; Rempli par le middleware context
     
     ;; Hiérarchie
     ;;(:col :manager_id :type :uuid :references "users" :label "Manager (N+1)")
     ;;(:col :unit_id    :type :uuid :references "units" :label "Unité") 

     ;; Identité
     (:col :firstname :type :string :required? t :label "Prénom")
     (:col :lastname  :type :string :required? t :label "Nom")
     (:col :email     :type :string :required? t :label "Email" :unique t) ;; Unique par Tenant
     
     ;; Sécurité (PBKDF2 / Bytea)
     (:col :password  :type :string :writeonly? t :virtual? t :input-type :password)
     (:col :pw_hash   :type :bytea  :hidden? t)
     (:col :pw_salt   :type :bytea  :hidden? t)
     (:col :pw_iters  :type :integer :hidden? t)
     
     ;; Rôles & Accès
     (:col :role      :type :string :default "user" 
                      :choices (("admin" . "Admin") ("manager" . "Manager") ("user" . "User")))
     (:col :scopes    :type :jsonb  :default "[]")
     (:col :is_active :type :boolean :default t))))

  ;; --- HOOKS ---
  :hooks
  ((user
    ;; Hashage automatique à la création et modification
    (:normalize :create (ctx payload) 
      (%hash-password-in-payload payload))
    (:normalize :patch  (ctx payload) 
      (%hash-password-in-payload payload))
    
    ;; Nettoyage automatique à la lecture
    (:after :show   (ctx res) (%sanitize-user res))
    (:after :around :create (ctx res)
	    (print "ENTERING...")
	    (print res)
	    (let ((result (%sanitize-user res)))
	      (call-next-method :create 'user ctx result :payload result)
	      (print result)
	      result)
	    )
    (:after :patch  (ctx res) (%sanitize-user res))
    
    ;; Nettoyage de liste (gestion pagination ou liste plate)
    (:after :index (ctx res)
      (if (and (listp res) (keywordp (car res)) (getf res :items))
          (progn 
            (setf (getf res :items)
		  (mapcar #'%sanitize-user
			  (getf res :items)))
            res)
          (mapcar #'%sanitize-user res)))))

  ;; --- RESSOURCES CRUD ---
  :resources
  ((user :name "users" :type :HTMX :required-p t
         ;; Guard simplifié pour l'exemple
         :guard (lambda (req &key op) 
                  (let ((role (lumen.core.http:current-role req)))
                    (if (equal op :index) t (equal role "admin")))))))

(defparameter *user-sensitive-fields* '(:pw_hash :pw-hash :pw_salt :pw-salt :pw_iters :pw-iters :password))

(defun %sanitize-user (user-alist)
  "Retire les champs sensibles d'une alist utilisateur."
  (print "IN %sanitize-user")
  (if user-alist
      (remove-if (lambda (item)
                   (member (car item) *user-sensitive-fields*))
                 user-alist)
      nil))

(defun %hash-password-in-payload (payload)
  "Vérifie si :password est présent, le hache, injecte les champs de sécu et retire le clair."
  (print "IN %hash-password-in-payload")
  (let ((clear (alist-get payload :password)))
    ;; Si un mot de passe est fourni (chaîne non vide)
    (when (and clear (stringp clear) (plusp (length clear)))
      ;; On appelle la fonction de hachage de lumen.core.auth
      (multiple-value-bind (salt iters hash)
          (lumen.core.auth:hash-password clear)
        
        ;; On injecte les résultats dans le payload
        (setf payload (alist-set payload :pw-salt salt)
              payload (alist-set payload :pw-iters iters)
              payload (alist-set payload :pw-hash hash))))
    
    ;; IMPORTANT : On retire le champ virtuel :password pour ne pas casser le SQL
    ;; (car la colonne 'password' n'existe pas en base)
    (let* ((out (remove :password payload :key #'car))
	  (out (remove "password" out :key #'car :test #'equal)))
      (print out)
      out)))
