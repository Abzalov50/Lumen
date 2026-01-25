(in-package :lumen.core.router)

;; Utilitaire: applique une liste de middlewares (formes qui RETOURNENT un MW)
;; autour d'un handler (lambda (req) ...), puis appelle le résultat avec req.
;; with-guards permet aussi de chaîner plusieurs MW sur une route, p.ex. (auth-required) et un rate-limit par endpoint.
(defmacro with-guards ((req &rest mw-forms) &body body)
  (labels ((fold (mws handler)
             (if (null mws)
                 handler
                 ;; (funcall (mw) <handler>)
                 `(funcall ,(car mws) ,(fold (cdr mws) handler)))))
    (let ((terminal `(lambda (,req) ,@body)))
      `(let ((wrapped ,(fold mw-forms terminal)))
         (funcall wrapped ,req)))))

(defmacro defprotected (method path-arg (req) &body body)
  "Route protégée (JWT requis). Bypass admin actif par défaut."
  `(lumen.core.router:defroute ,method ,path-arg (,req)
     (with-guards (,req (lumen.core.middleware:auth-required))
       ,@body)))

(defmacro defroles (method path-arg (req) roles &body body)
  "Route protégée + rôle(s) requis. Bypass admin actif par défaut."
  `(lumen.core.router:defroute ,method ,path-arg (,req)
     (with-guards (,req (lumen.core.middleware:roles-allowed ,roles))
       ,@body)))

;; macro DSL “tout en un” avec options
#|
(defmacro defguarded (method path-arg (req)
                      (&key required roles scopes scopes-mode
                            ;; rate limit
                            rate-cap rate-refill route-key
                            ;; options auth-jwt avancées
                            admin-bypass-roles? admin-bypass-scopes? admin-roles
                            allow-query-token? qs-keys
                            secret leeway-sec)
                      &body body)
  "Route protégée paramétrable par JWT (rôles, scopes, bypass admin, etc.)."
  (let* ((rk (gensym "ROUTEKEY"))
         ;; required effectif : si rôles ou scopes présents, on force le JWT requis
         (required-effective (or required (not (null roles)) (not (null scopes)))))
    `(lumen.core.router:defroute ,method ,path-arg (,req)
       (let* ((,rk (or ,route-key
                       (multiple-value-bind (host-spec real-path)
                           (%parse-route-args ,path-arg)
                         (declare (ignore host-spec))
                         (format nil "~A ~A" ,(string-upcase (string method)) real-path)))))
         
         ;; --- CORRECTION ICI : ON DÉCLARE LA VARIABLE IGNORABLE ---
         (declare (ignorable ,rk)) 
         
         (with-guards
           (,req
            ;; -------- Auth unique et configurée ----------
            (lumen.core.middleware:auth-middleware
             :required-p ,(if required-effective t nil)
             ,@(when roles       `(:roles-allow ,roles))
             ,@(when scopes      `(:scopes-allow ,scopes))
             ,@(when scopes-mode `(:scopes-mode ,scopes-mode))
             ;; bypass admin, liste des rôles admin
             ,@(when admin-roles            `(:admin-roles ,admin-roles))
             ,@(when (not (null admin-bypass-roles?))
                 `(:admin-bypass-roles-p ,admin-bypass-roles?))
             ,@(when (not (null admin-bypass-scopes?))
                 `(:admin-bypass-scopes-p ,admin-bypass-scopes?))
             ;; QS token pour SSE, etc.
             ,@(when (not (null allow-query-token?))
                 `(:allow-query-token-p ,allow-query-token?))
             ,@(when qs-keys `(:qs-keys ,qs-keys))
             ;; crypto / horloge
             ,@(when secret      `(:secret ,secret :secret-supplied-p t))
             ,@(when leeway-sec `(:leeway-sec ,leeway-sec)))
            ;; -------- Rate limit éventuel ----------
            ,@(when rate-cap
                `((lumen.core.middleware:rate-limit-middleware
                   :capacity ,rate-cap
                   :refill-per-sec ,(or rate-refill 1)
                   :route-key ,rk)))) ;; Ici rk est utilisé si rate-cap est présent
           ,@body)))))
|#

(defmacro defguarded (method path-arg (req)
                      (&key required roles scopes scopes-mode
                            ;; rate limit
                            rate-cap rate-refill route-key
                            ;; options auth-jwt avancées
                            admin-bypass? admin-roles
                            allow-query-token? qs-keys
                            secret leeway-sec)
                      &body body)
  "Route protégée paramétrable par JWT (rôles, scopes, bypass admin, etc.)."
  (let* ((rk (gensym "ROUTEKEY"))
         ;; required effectif : si rôles ou scopes présents, on force le JWT requis
         (required-effective (or required (not (null roles)) (not (null scopes)))))
    
    `(lumen.core.router:defroute ,method ,path-arg (,req)
       (let* ((,rk (or ,route-key
                       (multiple-value-bind (host-spec real-path)
                           (lumen.core.router::%parse-route-args ,path-arg)
                         (declare (ignore host-spec))
                         (format nil "~A ~A" ,(string-upcase (string method)) real-path)))))
         
         (declare (ignorable ,rk))
         
         ;; On construit la chaîne de middlewares dynamiquement pour cette route
         (lumen.core.pipeline:execute-middleware-chain
          (list
           ;; 1. AUTHENTIFICATION
           (make-instance 'lumen.core.middleware:auth-middleware
                          :required-p ,(if required-effective t nil)
                          ,@(when roles        `(:roles-allow ,roles))
                          ,@(when scopes       `(:scopes-allow ,scopes))
                          ,@(when scopes-mode  `(:scopes-mode ,scopes-mode))
                          ;; Configuration Admin
                          ,@(when admin-roles  `(:admin-roles ,admin-roles))
                          ,@(when admin-bypass? `(:bypass-admin ,admin-bypass?))
                          ;; Configuration Token Source
                          ,@(when (not (null allow-query-token?))
                              `(:allow-query ,allow-query-token?))
                          ;; Note: qs-keys n'est pas dans le standard auth-middleware v2 
                          ;; mais on le passe au cas où vous l'ayez ajouté
                          ,@(when qs-keys      `(:qs-keys ,qs-keys))
                          ;; Crypto
                          ,@(when secret       `(:secret ,secret))
                          ,@(when leeway-sec   `(:leeway ,leeway-sec)))

           ;; 2. RATE LIMIT (Conditionnel)
           ;; On utilise ,@ pour épisser le code seulement si rate-cap est défini
           ,@(when rate-cap
               `((make-instance 'lumen.core.middleware:rate-limit-middleware
                                :capacity ,rate-cap
                                :refill ,(or rate-refill 1)
                                :route-key ,rk))))
          
          ;; LE HANDLER FINAL (Corps de la route)
          (lambda (,req) 
            ,@body)
          
          ;; L'ARGUMENT REQUEST
          ,req)))))

(defmacro def-api-route (method path args (&key scopes roles (admin-bypass t)
                                                ;; Champs pour la documentation :
                                                summary params tag)
                         &body body)
  "Wrapper autour de lumen.core.router:defguarded pour QALM.
   - Force l'authentification (required=t)
   - Simplifie la syntaxe des scopes
   - Enregistre automatiquement la route pour OpenAPI/Discovery"
  
  `(progn
     ;; 1. ENREGISTREMENT DOCUMENTATION (au moment du chargement)
     ;; On suppose que register-custom-route est dans lumen.http.crud
     (lumen.http.crud::register-custom-route 
       ',method ,path 
       :summary ,summary 
       :params ',params   ;; On quote pour passer la liste de données brute
       :tag ,tag
       ;; def-api-route force :required t, donc on met toujours BearerAuth
       :security '((:BearerAuth . ())))

     ;; 2. DÉFINITION D'EXÉCUTION (Runtime)
     (lumen.core.router:defguarded ,method ,path ,args
       ;; On active auth-jwt
       (:required t
        ;; Configuration des permissions
        :scopes ,scopes
        :roles  ,roles
        ;; Bypass Admin (activé par défaut)
        :admin-bypass? ,admin-bypass
        ;; Autoriser le token en query param pour SSE
        :allow-query-token? t )
       
       ,@body)))
