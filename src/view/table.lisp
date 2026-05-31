(defpackage :lumen.view.table
  (:use :cl :spinneret :lumen.data.db :lumen.utils)
  (:import-from :lumen.data.dao :entity-fields)
  (:export :render-entity-table :render-entity-details-modal
	   :make-action-col :render-datagrid :render-row :render-row-actions
	   :render-header-col :render-pagination :col))

(in-package :lumen.view.table)

;;; ---------------------------------------------------------------------------
;;; 1. DEFINITIONS & HELPERS
;;; ---------------------------------------------------------------------------
(defun col (key label &key (sortable t) (format nil) (class "") (actions nil))
  "Constructeur de colonne pour le DataGrid."
  (list :key key 
        :label label 
        :sortable sortable 
        :format format 
        :class class
        :actions actions))

(defun make-action-col (actions)
  "Crée la définition de la colonne Actions."
  (list :key :__actions__ 
        :label "Actions" 
        :sortable nil 
        :class "text-end"
        ;; On stocke la liste des actions dans une propriété spéciale de la colonne
        :actions actions))

(defvar *fk-cache* (make-hash-table :test 'equal)
  "Cache simple pour éviter le problème N+1 requêtes (Optionnel).
   À vider à chaque requête HTTP via un middleware si utilisé.")

(defun %resolve-foreign-key (val ref-table ref-col)
  "Exécute: SELECT ref-col FROM ref-table WHERE id = val"
  (when val
    ;; 1. On injecte table et colonne via FORMAT (Lisp)
    ;; 2. On garde $1 pour la valeur de l'ID (Postgres)
    (let* ((query (format nil "SELECT \"~A\" FROM \"~A\" WHERE id = $1 LIMIT 1" 
                          ref-col ref-table))
           ;; Notez que l'argument est passé séparément, sans quotes
           (row (lumen.data.db:ensure-connection 
                  (pomo:query query val :single))))
      
      (if row
          ;; On récupère la valeur via le keyword de la colonne
          row
          ;; Fallback
          (format nil "~A (Introuvable)" val)))))

;;; ============================================================================
;;; 1. FORMATAGE DES CELLULES (DISPLAY LOGIC)
;;; ============================================================================

(defun %format-date (val)
  "Tente de formater une date (Universal Time ou ISO String)."
  (cond
    ;; Cas 1 : Timestamp Lisp (Integer)
    ((integerp val)
     (multiple-value-bind (s m h dd mm yy) (decode-universal-time val)
       (declare (ignore s m h))
       (format nil "~2,'0D/~2,'0D/~A" dd mm yy)))

    ;; Ajout pour supporter les objets local-time
    #+local-time
    ((typep val 'local-time:timestamp)
     (local-time:format-timestring nil val :format '((:day 2) #\/ (:month 2) #\/ (:year 4))))
    
    ;; Cas 2 : String ISO (Postgres retourne souvent des strings "2023-01-01...")
    ((stringp val)
     (let ((parts (uiop:split-string val :separator "T ")))
       (first parts))) ;; Retourne juste "YYYY-MM-DD"
    
    (t (format nil "~A" val))))

(defun %format-value (val type)
  "Génère le HTML pour une valeur donnée selon son type."
  (with-html
  (cond
    ;; 1. Valeur Nulle
    ((null val) 
     (:span :class "text-muted small opacity-50" "—"))

    ;; 2. Booléens (Badges visuels)
    ((eq type :boolean)
     (if val
         (:span :class "badge rounded-pill bg-success-subtle text-success border border-success-subtle"
                (:i :class "bi bi-check-lg") " Oui")
         (:span :class "badge rounded-pill bg-secondary-subtle text-secondary border border-secondary-subtle"
                (:i :class "bi bi-x") " Non")))

    ;; 3. Dates & Temps
    ((member type '(:date :timestamp :timestamptz))
     (:span :class "text-nowrap" 
            (:i :class "bi bi-calendar3 me-1 text-muted small")
            (%format-date val)))

    ;; 4. Email (Lien mailto)
    ((eq type :email)
     (:a :href (format nil "mailto:~A" val) 
         :class "text-decoration-none" 
         val))

    ;; 5. URL (Lien externe)
    ((eq type :url)
     (:a :href val :target "_blank" :class "text-decoration-none"
         (:i :class "bi bi-box-arrow-up-right me-1") "Lien"))

    ;; 6. UUID (Affichage compact)
    ((eq type :uuid)
     (:code :class "text-muted small user-select-all" 
            (let ((s (format nil "~A" val)))
              (if (> (length s) 8) (subseq s 0 8) s))))

    ;; 7. Prix / Monétaire
    ((eq type :price)
     (:span :class "font-monospace fw-bold" 
            (format nil "~,2F €" val)))
    
    ;; 8. Texte long (Troncature)
    ((and (eq type :text) (stringp val) (> (length val) 50))
     (:span :title val 
            (format nil "~A..." (subseq val 0 47))))

    ;; 9. Défaut
    (t (format nil "~A" val)))))

(defun %fetch-prop (item key)
  "Récupère une valeur dans une Alist ou Plist de manière tolérante (snake/kebab)."
  (let ((kebab-key (lumen.utils:keyword-to-kebab key))
        (is-alist (and (consp item) (consp (first item)))))
    
    (if is-alist
        ;; Cas ALIST
        (let ((cell (or (assoc key item :test #'eq)        ;; Essai 1 : :project_id
                        (assoc kebab-key item :test #'eq)))) ;; Essai 2 : :project-id
          (cdr cell))
        
        ;; Cas PLIST
        (let ((val (getf item key '%%not-found%%)))
          (if (eq val '%%not-found%%)
              (getf item kebab-key) ;; Essai 2
              val)))))

(defun %get-display-value (item field)
  "Orchestre l'affichage d'une cellule : Résolution FK ou Formatage standard."
  (let* ((col      (getf field :col))
         ;; Supporte item sous forme de Plist (:key val) ou Alist ((:key . val))
         (val      (%fetch-prop item col))                 ;; Plist
         (type     (getf field :type))
         (ref      (getf field :references)) ;; Nom de table (ex: "projects")
         (ref-col  (getf field :ref-col)))   ;; Colonne label (ex: "name")
    (with-html    
    (cond
      ;; CAS 1 : C'est une Clé Étrangère définie
      ((and val ref ref-col)
       ;; On affiche le label résolu (ex: "Projet Alpha")
       ;; On pourrait ajouter un lien vers l'entité parente ici si on voulait être très complet
       (:span :class "fw-medium text-dark"
              (%resolve-foreign-key val ref ref-col)))

      ;; CAS 2 : C'est un champ Enum (Choices)
      ;; Si choices est ((1 . "Haut") ...), on veut afficher "Haut", pas 1.
      ((and val (getf field :choices))
       (let* ((choices (getf field :choices))
              (found   (find val choices :key (lambda (x) (if (consp x) (car x) x)) :test #'equalp)))
         (if (consp found)
             ;; On a trouvé le label correspondant
             (cdr found)
             ;; Pas trouvé, on affiche la valeur brute
             (%format-value val type))))

      ;; CAS 3 : Affichage Standard
      (t 
       (%format-value val type))))))

;;; ---------------------------------------------------------------------------
;;; 2. AUTO-DISCOVERY (INTELLIGENT)
;;; ---------------------------------------------------------------------------
(defun %derive-columns-from-entity (entity-sym)
  (let* ((fields (lumen.data.dao:entity-fields entity-sym))
         (cols '()))
    
    (dolist (f fields)
      (let* ((name     (getf f :col))
             (type     (getf f :type))
             (hidden?  (getf f :hidden?))
             (choices  (getf f :choices))     ;; Ex: (("todo" . "À faire") ...)
             (ref      (getf f :references))
             (ref-col  (getf f :ref-col "id"))
             (label    (or (getf f :label) (string-capitalize (string-downcase name)))))
        
        (unless hidden?
          (let ((fmt 
                 (cond
                   ;; CAS 1 : Clé Étrangère (FK) avec Cache résolu
                   ((and ref ref-col)
                    (lambda (val row)
                      (let* ((resolved-key (intern (string-upcase (format nil "~A-RESOLVED" name)) :keyword))
                             (pre-fetched  (lumen.utils:lookup row resolved-key)))
                        (with-html-string 
                          (:span :class "fw-medium text-dark"
                                 (or pre-fetched 
                                     (%resolve-foreign-key val ref ref-col)))))))

                   ;; CAS 2 : CHOICES (Correction Robuste pour Strings et Entiers)
                   (choices
                    (lambda (val row)
                      (declare (ignore row))
                      ;; On utilise ASSOC, qui est fait pour les listes ((clé . val) ...)
                      ;; Mais on lui passe un TEST personnalisé pour gérer String et Int
                      (let ((pair (assoc val choices :test 
                                         (lambda (v k)
                                           (cond
                                             ;; Si les deux sont des nombres (Priorité) -> comparaison numérique
                                             ((and (numberp v) (numberp k)) (= v k))
                                             ;; Sinon conversion en string et comparaison insensible à la casse
                                             ;; Gère "todo" vs "todo", :todo vs "todo", etc.
                                             (t (string-equal (string v) (string k))))))))
                        (if pair 
                            (cdr pair) ;; Retourne "À faire"
                            val))))    ;; Retourne "todo" si pas trouvé

                   ;; CAS 3 : Booléen
                   ((eq type :boolean) :badge)

                   ;; CAS 4 : Dates
                   ((member type '(:date :datetime :timestamp)) :date-fr)

                   ;; CAS 5 : Email
                   ((search "email" (string-downcase name)) 
                    (lambda (v r) (declare (ignore r)) 
                      (format nil "<a href='mailto:~A'>~A</a>" v v)))

                   (t :text))))

            (push (col name label :sortable t :format fmt) cols)))))
    
    (push (make-action-col '(:show :edit :delete)) cols)
    (nreverse cols)))

;;; ============================================================================
;;; 2. ELEMENTS D'INTERFACE (Header, Filters, Actions)
;;; ============================================================================
(defun %render-sortable-header (field current-sort current-dir base-url)
  "Génère un TH cliquable pour le tri."
  (let* ((col (string-downcase (getf field :col)))
         (label (or (getf field :label) (string-capitalize col)))
         (is-active (string-equal col current-sort))
         (new-dir (if (and is-active (eq current-dir :asc)) :desc :asc))
         (icon (cond ((not is-active) "bi-arrow-down-up text-muted")
                     ((eq current-dir :asc) "bi-arrow-down-short text-primary")
                     (t "bi-arrow-up-short text-primary"))))
    (with-html
    (:th :scope "col"
         ;; On utilise HTMX pour recharger juste la table quand on trie
         (:a :href "#"
             :class "text-decoration-none text-dark d-flex align-items-center gap-1"
             :hx-get (format nil "~A?sort=~A&dir=~A" base-url col new-dir)
             :hx-target "#entity-table-container" ;; Cible le wrapper de la table
             :hx-include "closest form"           ;; Inclut les filtres actuels !
             label
             (:i :class (format nil "bi ~A" icon)))))))

(defun render-row (row columns base-url)
  "Génère le HTML d'un TR complet."
  (spinneret:with-html
    (:tr
     (dolist (c columns)
       (let ((key (getf c :key)) (cls (getf c :class)))
         (:td :class cls
              (if (eq key :__actions__)
                  (render-row-actions row base-url (getf c :actions))
                  (:raw (render-cell (getf c :format) 
                                     (if (eq key :__custom-actions__) "%%DUMMY%%" (lumen.utils:lookup row key))
                                     row)))))))))

(defun render-row-actions (row base-url actions)
  (let* ((id (lumen.utils:lookup row :id))
         (url-show   (format nil "~A/~A" base-url id))
         (url-edit   (format nil "~A/~A/edit" base-url id))
         (url-delete (format nil "~A/~A" base-url id)))
    
    (with-html
      (:div :class "btn-group btn-group-sm"
        
            ;; BOUTON VOIR (Type Button, pas A)
            (when (member :show actions)
              (:button :type "button" 
                       :class "btn btn-outline-secondary"
                       :hx-get url-show 
                       :hx-target "#modal-container"
                       :hx-swap "innerHTML"
                       :title "Voir"
                       (:i :class "bi bi-eye")))

            ;; EDIT (Peut rester un lien car changement de page)
            (when (member :edit actions)
              (:a :href url-edit 
                  :class "btn btn-outline-primary"
                  :title "Modifier"
                  (:i :class "bi bi-pencil")))

            ;; DELETE (Bouton)
            (when (member :delete actions)
              (:button :class "btn btn-outline-danger"
                       :title "Supprimer"
                       :hx-delete url-delete
                       :hx-confirm "Voulez-vous vraiment supprimer cet élément ?"
                       :hx-target "closest tr"
                       :hx-swap "outerHTML swap:1s"
                       (:i :class "bi bi-trash")))))))

(defun render-header-col (col current-sort current-dir base-url target-id &optional (form-selector "closest form"))
  (let* ((key (getf col :key))
         (label (getf col :label))
         (sortable (getf col :sortable))
         (class (getf col :class))
         (key-str (format nil "~A" (or key "")))
         (sort-str (format nil "~A" (or current-sort "")))
         (is-sorted (and key current-sort (string-equal key-str sort-str)))
         (is-asc (string-equal (string current-dir) "ASC"))
         (new-dir (if (and is-sorted is-asc) "DESC" "ASC")))

    (spinneret:with-html
      (:th :scope "col" :class class
           (if sortable
               (:a :href "#" 
                   :class "text-decoration-none text-dark d-flex align-items-center gap-1"
                   :hx-get (format nil "~A?sort=~A&dir=~A" base-url key new-dir)
                   :hx-target (format nil "#~A" target-id)
                   ;; OVERRIDE DU BODY !
                   ;;:hx-select (format nil "#~A" target-id) 
                   ;; ON REMPLACE TOUT LE BLOC
                   ;;:hx-swap "outerHTML"
		   :hx-swap "innerHTML" 
                   :hx-include form-selector
                   
                   label
                   (cond 
                     ((and is-sorted is-asc)  (:i :class "bi bi-sort-down-alt text-primary"))
                     ((and is-sorted (not is-asc)) (:i :class "bi bi-sort-up text-primary"))
                     (t (:i :class "bi bi-arrow-down-up text-muted opacity-25 small"))))
               label)))))

(defun render-pagination (pg source-url target-id &optional (form-selector "closest form"))
  (let ((page (getf pg :page 1))
        (total (getf pg :total-pages 1))
        (sep (if (find #\? source-url) "&" "?")))
    
    (when (> total 1)
      (spinneret:with-html
        (:div :class "card-footer bg-white d-flex justify-content-between align-items-center py-2"
              (:small :class "text-muted" (format nil "Page ~D / ~D" page total))
              (:nav
               (:ul :class "pagination pagination-sm mb-0"
                    (flet ((render-link (target-page label disabled?)
                             (:li :class (format nil "page-item ~A" (if disabled? "disabled" ""))
                                  (:button :class "page-link"
                                           :hx-get (format nil "~A~Apage=~D" source-url sep target-page)
                                           :hx-target (format nil "#~A" target-id)
                                           ;; OVERRIDE DU BODY !
                                           ;;:hx-select (format nil "#~A" target-id)
                                           ;; ON REMPLACE TOUT LE BLOC
                                           ;;:hx-swap "outerHTML"
					   :hx-swap "innerHTML"
                                           :hx-include form-selector
                                           label))))
                      (render-link (1- page) "Précédent" (<= page 1))
                      (render-link (1+ page) "Suivant" (>= page total))))))))))

(defgeneric render-cell (format value row)
  ;; OBS 3: Gestion des NULLs globaux
  (:method :around (format value row)
    (if (or (null value) (equal value "NULL"))
        (with-html-string (:span :class "text-muted opacity-50" "-"))
        (call-next-method)))

  (:method ((format (eql nil)) value row) (format nil "~A" value))
  (:method ((format (eql :text)) value row) (format nil "~A" value))
  
  (:method ((format (eql :badge)) value row)
    (let ((truthy (and value (not (equal value "false"))))) ;; Gestion string "false" de la DB
      (with-html-string
        (:span :class (format nil "badge bg-~A" (if truthy "success" "secondary"))
               (if truthy "Oui" "Non")))))

  ;; OBS 6: Format Date Français
  (:method ((format (eql :date-fr)) value row)
    (with-html-string 
      (:span :class "text-nowrap" 
             (:i :class "bi bi-calendar3 me-1 text-muted small")
             (lumen.utils:%val->date-display value))))

  ;; OBS 4: Clé Étrangère
  (:method ((format (eql :foreign-key)) value row)
    ;; Amélioration possible : Si value est un ID, on peut le raccourcir
    (if (and (stringp value) (> (length value) 8))
        (with-html-string 
          (:span :class "font-monospace small text-muted" 
                 :title value ;; Tooltip avec l'ID complet
                 (subseq value 0 8) "..."))
        (format nil "~A" value)))

  (:method ((format function) value row)
    (funcall format value row)))

;;; ============================================================================
;;; 3. COMPOSANT PRINCIPAL : TABLE
;;; ============================================================================
(defun render-datagrid (items columns &key (id "datagrid") (source-url "") 
                                           (mode :default) 
                                           (current-sort nil) (current-dir "DESC") 
                                        (pagination nil) (empty-message "Aucune donnée.")
					(filter-selector "closest form"))
  
  (let ((tbody-id (format nil "~A-body" id))         ;; ID calculé pour le corps
        (pagination-id (format nil "~A-pagination" id))) ;; ID calculé pour le footer
    
    (spinneret:with-html
      (:div :id id :class "card shadow-sm"
            (:div :class "table-responsive"
                  (:table :class "table table-hover table-striped align-middle mb-0"
                          ;; THEAD
                          (:thead :class "table-light"
                                  (:tr
                                   (dolist (c columns)
                                     (render-header-col c current-sort current-dir source-url tbody-id))))
                          
                          ;; TBODY (Cible des mises à jour)
                          (:tbody :id tbody-id
                                  :hx-get (if (eq mode :remote) source-url "")
                                  :hx-trigger (if (eq mode :remote) "load" "")
                                  :hx-target "this"
                                  :hx-include filter-selector
				  :hx-swap (if (eq mode :remote) "outerHTML" "")
				  ;;:hx-select (if (eq mode :remote) (format nil "#~A" tbody-id) "")
                                  (if (eq mode :remote)
                                      ;; Loader initial
                                      (:tr (:td :colspan (length columns) :class "text-center py-5"
                                                (:div :class "spinner-border text-primary")))
                                      ;; Rendu direct
                                      (if (null items)
                                          (:tr (:td :colspan (length columns) :class "text-center py-5 text-muted" empty-message))
                                          (dolist (row items)
                                            (render-row row columns source-url)))))))
            
            ;; FOOTER (Pagination)
            (:div :id pagination-id
                  (when (and pagination (not (eq mode :remote)))
                    ;; On passe tbody-id pour que les boutons sachent quoi mettre à jour
                    (render-pagination pagination source-url tbody-id filter-selector)))))))

#|
(defun render-datagrid (items columns &key (id "datagrid") (source-url "") 
                                           (current-sort nil) (current-dir "DESC") 
                                        (pagination nil) (empty-message "Aucune donnée."))
  (with-html
    (:div :id id :class "card shadow-sm"
      (:div :class "table-responsive"
        (:table :class "table table-hover table-striped align-middle mb-0"
          
          ;; --- HEADER ---
          (:thead :class "table-light"
            (:tr
             (dolist (c columns)
               (let ((key (getf c :key)) 
                     (label (getf c :label)) 
                     (sortable (getf c :sortable)) 
                     (cls (getf c :class)))
                 (:th :scope "col" :class cls
                      (if sortable
                          ;; OBS 7: Logique de Tri Robuste (String-Equal)
                          (let* ((is-sorted (string-equal (string key) (string current-sort)))
                                 (is-asc    (string-equal (string current-dir) "ASC"))
                                 (new-dir   (if (and is-sorted is-asc) "DESC" "ASC")))
                            
                            (:a :href "#" :class "text-decoration-none text-dark d-flex align-items-center gap-1"
                                :hx-get (format nil "~A?sort=~A&dir=~A" source-url key new-dir)
                                :hx-target (format nil "#~A" id) :hx-include "closest form"
                                label
                                ;; Affichage de l'icône
                                (cond
                                  ((and is-sorted is-asc)  (:i :class "bi bi-sort-down-alt text-primary"))
                                  ((and is-sorted (not is-asc)) (:i :class "bi bi-sort-up text-primary"))
                                  (t (:i :class "bi bi-arrow-down-up text-muted opacity-25 small")))))
                          
                          ;; Non triable
                          label))))))
          
          ;; --- BODY ---
          (:tbody
           (if (null items)
               (:tr (:td :colspan (length columns) :class "text-center py-5 text-muted" empty-message))
               
               (dolist (row items)
                 (:tr
                  (dolist (c columns)
                    (let ((key (getf c :key)) (cls (getf c :class)))
                      (:td :class cls
                           (if (eq key :__actions__)
                               (render-row-actions row source-url (getf c :actions))
                               (:raw (render-cell (getf c :format) 
                                                  (if (eq key :__custom-actions__)
						      "%%DUMMY%%"
						      (lumen.utils:lookup row key))
                                                  row))))))))))))
      
      ;; FOOTER
      (when pagination (render-pagination pagination source-url id)))))
|#

;;; ============================================================================
;;; 4. COMPOSANT MODALE (VIEW / SHOW)
;;; ============================================================================
(defun render-entity-details-modal (entity-sym item &key (title "Détails"))
  "Génère le HTML d'une modale Bootstrap 5 ouverte."
  (let ((fields (entity-fields entity-sym)))
    (with-html
      (:div :class "modal fade show" :id "detailModal" :tabindex "-1" 
            :style "display: block; background: rgba(0,0,0,0.5);"
            :role "dialog"
            ;; Clic sur le fond ferme la modale (vide le container)
            :onclick "if(event.target === this) document.getElementById('modal-container').innerHTML = '';"
        
            (:div :class "modal-dialog modal-dialog-centered"
		  (:div :class "modal-content"
			(:div :class "modal-header"
			      (:h5 :class "modal-title" title)
			      (:button :type "button" :class "btn-close" 
				       :onclick "document.getElementById('modal-container').innerHTML = '';"))
            
			(:div :class "modal-body"
			      (:table :class "table table-sm"
				      (:tbody
				       (dolist (field fields)
					 (unless (getf field :hidden?)
					   (:tr
					    (:th :class "w-25 text-muted fw-normal" (or (getf field :label) (getf field :col)))
					    (:td :class "fw-medium" 
						 (%get-display-value item field))))))))
            
			(:div :class "modal-footer"
			      (:button :type "button" :class "btn btn-secondary" 
				       :onclick "document.getElementById('modal-container').innerHTML = '';"
				       "Fermer"))))))))
