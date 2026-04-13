(defpackage :lumen.admin.grid
  (:use  :spinneret :common-lisp :lumen.utils)
  (:export :fetch-grid-data :get-display-columns :render-cell-content))

(in-package :lumen.admin.grid)

;; --- 1. INTROSPECTION INTELLIGENTE DES COLONNES ---
(defun get-display-columns (entity-sym)
  "Détermine les colonnes à afficher.
   NOTE : En auto-découverte, force l'affichage de la PK (pour les actions d'admin)."
  (let* ((view   (lumen.admin.registry:get-view entity-sym))
         (fields (lumen.data.dao:entity-fields entity-sym))
         ;; 1. On récupère la PK pour l'exclure du filtrage "hidden"
         (meta   (lumen.data.dao:entity-metadata entity-sym))
         (pk     (or (getf meta :primary-key) :id))) 
    
    (if (and view (lumen.admin.registry:view-list-fields view))
        ;; CAS A : Config manuelle via defadmin
        (loop for col-spec in (lumen.admin.registry:view-list-fields view)
              collect (if (listp col-spec) 
                          ;; Si format (:col "Label" ...), on garde la liste
                          col-spec 
                          ;; Si format :col, on en fait une liste (:col)
                          (list col-spec)))
        
        ;; CAS B : Auto-découverte (Zéro-Config)
        (loop for f in fields
              for col = (getf f :col)
              
              ;; CRITÈRE DE SÉLECTION :
              ;; 1. C'est la Clé Primaire (Toujours visible en admin)
              ;; 2. OU ALORS : Ce n'est pas caché ET ce n'est pas une colonne technique blacklistée
              when (or (eq col pk)
                       (and (not (getf f :hidden?)) 
                            (not (member col 
                                         '(:password :pw_hash :pw_salt :pw_iters 
                                           :data_snapshot :tenant_id :lock_version)
                                         :test #'eq))))
              
              ;; On retourne (:col "Label")
              collect (list col (or (getf f :label) 
                                    (string-capitalize (string-downcase col))))))))

;; --- 2. RÉSOLUTION DES FK (Le "Smart" Display) ---

(defun %resolve-fk-display (ref-table fk-id)
  "Tente de trouver un nom lisible pour une clé étrangère."
  ;; Pour l'instant, on fait une requête simple (N+1 mais sur une page de 20 items c'est négligeable)
  ;; Optimisation Phase 4 : Eager Loading
  (when fk-id
    (let* ((sql (format nil "SELECT * FROM ~A WHERE id = $1" ref-table))
           (row (first (lumen.data.db:query-a sql fk-id))))
      (if row
          ;; Heuristique : on cherche 'name', 'title', 'label', 'email' ou 'firstname'+'lastname'
          (or (alist-get row :name)
              (alist-get row :title)
              (alist-get row :label)
              (alist-get row :email)
              (let ((fn (alist-get row :firstname))
                    (ln (alist-get row :lastname)))
                (if (and fn ln) (format nil "~A ~A" fn ln) nil))
              (alist-get row :code)
              (format nil "ID:~A..." (subseq (string fk-id) 0 8)))
          "Inconnu"))))

;; --- 3. RENDU DES CELLULES (HTML) ---

;; Helper interne pour parser n'importe quoi en Timestamp
(defun %to-timestamp (val)
  (typecase val
    (integer (local-time:universal-to-timestamp val))
    (string  (ignore-errors (local-time:parse-timestring val)))
    (local-time:timestamp val)
    (t nil)))

(defun render-cell-content (entity-sym col-name val row-data)
  "Formate une valeur brute pour l'affichage HTML dans la Grid."
  (declare (ignore row-data))
  
  (let* ((field-def (find col-name (lumen.data.dao:entity-fields entity-sym) 
                          :key (lambda (f) (getf f :col))))
         (type      (getf field-def :type))
         (ref       (getf field-def :references))
         (choices   (getf field-def :choices))
         (val-str   (princ-to-string val))) ;; On pré-calcule la string brute
    
    (cond
      ;; 1. CAS NULL / VIDE
      ((or (eq val :null) (null val)) 
       "<span class='text-muted small'>-</span>")

      ;; 2. CAS BADGE (Enum / Status)
      ((or choices (member col-name '(:status :role :state :type) :test #'eq))
       (let* ((label (if choices (or (cdr (assoc val choices :test #'string=)) val-str) val-str))
              (color (case (intern (string-upcase val-str) :keyword)
                       ((:active :published :success :done :paid :running) "success")
                       ((:pending :waiting :trial :warning) "warning")
                       ((:error :banned :failed :deleted :danger :stopped) "danger")
                       ((:draft :archived :inactive :gray :secondary) "secondary")
                       ((:admin :primary :info :new) "primary")
                       ((:user :member) "info")
                       (t "light text-dark border"))))
         (format nil "<span class='badge bg-~A'>~A</span>" color
                 (spinneret::escape-string label))))

      ;; 3. CAS BOOLÉEN
      ((eq type :boolean)
       (if val 
           "<i class='bi bi-check-circle-fill text-success fs-5'></i>" 
           "<i class='bi bi-dash-circle text-muted opacity-25 fs-5'></i>"))

      ;; 4. CAS CLÉ ÉTRANGÈRE (REFACTORISÉ)
      (ref
       (let* ((target-entity (lumen.admin.utils:table-to-entity (string-downcase ref))) 
              (display       (%resolve-fk-display (string-downcase ref) val))
              ;; On génère l'URL brute
              (raw-url       (format nil "/admin/list/~A?search=~A" 
                                     (if target-entity target-entity ref) val))
              ;; On la rend dynamique vis-à-vis de l'application
              (final-url     (lumen.app.app:app-path raw-url)))
         (format nil "<a href='~A' class='text-decoration-none fw-medium'>~A</a>" 
                 final-url 
                 (spinneret::escape-string display))))

      ;; 5. CAS DATE & TIMESTAMP
      ((member type '(:date :timestamptz :timestamp))
       (let ((ts (%to-timestamp val)))
         (if ts
             (local-time:format-timestring 
              nil ts 
              :format '(:day "/" (:month 2) "/" :year " " (:hour 2) ":" (:min 2))
              :timezone local-time:+utc-zone+)
             (spinneret::escape-string val-str))))

      ;; 6. CAS JSONB
      ((eq type :jsonb)
       (let ((str (if (stringp val) val (cl-json:encode-json-to-string val))))
         (format nil "<code class='small text-muted' title='~A'>~A</code>" 
                 (spinneret::escape-string str)
                 (spinneret::escape-string 
                  (if (> (length str) 30) (concatenate 'string (subseq str 0 30) "...") str)))))
      
      ;; 7. CAS EMAIL (Pas touche, c'est un mailto:)
      ((eq type :email)
       (format nil "<a href='mailto:~A' class='text-muted text-decoration-none'>~A</a>" 
               val (spinneret::escape-string val)))

      ;; --- 8. CAS SPÉCIAL : IMPERSONATION (REFACTORISÉ) ---
      ;; Utilise un BUTTON + hx-get + stopPropagation
      ((and (eq col-name :id) 
            (search "USER" (string-upcase (symbol-name entity-sym))))
       
       (let ((impersonate-url (lumen.app.app:app-path (format nil "/admin/impersonate/~A" val))))
         (format nil 
                 "<div class='d-flex align-items-center justify-content-between'>
                    <span class='font-monospace text-muted'>~A</span>
                    <button type='button' class='btn btn-sm btn-outline-warning ms-2 py-0 px-1' 
                            hx-get='~A'
                            hx-target='body'
                            onclick='event.stopPropagation();'
                            title='Se connecter en tant que cet utilisateur'
                            data-bs-toggle='tooltip'>
                      <i class='bi bi-mask'></i>
                    </button>
                  </div>" 
                 (spinneret::escape-string val-str) 
                 impersonate-url)))

      ;; 9. DÉFAUT
      (t 
       (spinneret::escape-string val-str)))))

;; --- 4. LE BUILDER SQL ---
(defun fetch-grid-data (req entity-sym &key (page 1) (per-page 20) sort-col (sort-dir :asc) search)
  "Retourne (values items total-count) en utilisant le standard repo-index."
  (lumen.data.db:ensure-connection
    (let* ((ctx (lumen.core.http:ctx-from-req req))

           (table-name (lumen.data.dao:entity-table entity-sym))
           
           ;; 1. Construction de la Whitelist (Champs entité + Système)
           (fields     (lumen.data.dao:entity-fields entity-sym))
           (field-keys (mapcar (lambda (f) (getf f :col)) fields))
           (whitelist  (append field-keys '(:id :created_at :updated_at :tenant_id)))

           ;; 2. Définition du tri brut (Raw Order)
           (raw-order  (when sort-col 
                         (list (list sort-col sort-dir))))
           
           (default-order (lumen.http.crud::%derive-default-order entity-sym))	   

           ;; 3. Validation (Whitelist Check)
           ;; Si le tri demandé est dans la whitelist, on le garde. Sinon, fallback sur default.
           (base-order (or (lumen.http.crud::%ensure-order-whitelist 
                            raw-order 
                            whitelist)
                           default-order))

           ;; 4. Qualification des colonnes (Final Order)
           ;; On transforme :created_at en "users.created_at"
           (final-order 
            (mapcar (lambda (pair)
                      (let ((col (first pair)) 
                            (dir (second pair)))
                        (list (if (find #\. (string col)) 
                                  col 
                                  (format nil "~A.~A" table-name (string-downcase col))) 
                              dir)))
                    base-order))

           ;; 5. Filtres (Recherche)
           (filters (when (and search (> (length search) 0))
                      (list (cons "q" search)))))

      ;; 6. Appel au Repository
      ;; On passe 'final-order' directement à la clé :order
      (let ((result (lumen.data.repo.core:repo-index 
                     entity-sym 
                     ctx
                     :filters filters
                     :order final-order 
                     :page page 
                     :page-size per-page)))

        ;; 7. Extraction du résultat
        (let* ((is-plist (and (listp result) (keywordp (car result))))
               (items    (if is-plist (getf result :items) result))
               (count    (if is-plist (getf result :count) (length items))))
          
          (values (or items nil) 
                  (or count 0)))))))
