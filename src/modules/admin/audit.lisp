(defpackage :lumen.admin.audit
  (:use :common-lisp :spinneret :lumen.utils)
  (:export :render-audit-view :render-diff-detail))

(in-package :lumen.admin.audit)

;; --- 1. RENDU DU DIFF (Key / Before / After) ---

(defun %parse-diff (json-diff)
  "Transforme le JSONB diff en structure exploitable."
  ;; On suppose un format { "col": [old, new] } ou { "col": new } (create)
  (if (stringp json-diff)
      (cl-json:decode-json-from-string json-diff)
      json-diff))

(defun render-diff-detail (diff-json)
  "Génère un tableau HTML comparatif."
  (let ((diff-alist (%parse-diff diff-json)))
    (with-html-string
      (if diff-alist
          (:table :class "table table-sm table-bordered mb-0"
            (:thead :class "table-light"
              (:tr (:th "Champ") (:th "Ancienne valeur") (:th "Nouvelle valeur")))
            (:tbody
              (dolist (item diff-alist)
                (let* ((key (car item))
                       (val (cdr item))
                       ;; Détection format [old, new] vs simple value
                       (is-list (and (listp val) (= (length val) 2)))
                       (old (if is-list (first val) "-"))
                       (new (if is-list (second val) val)))
                  
                  (:tr 
                    (:td :class "fw-bold font-monospace text-muted" key)
                    (:td :class "text-danger bg-danger bg-opacity-10 text-break" 
                         (spinneret::escape-string (princ-to-string old)))
                    (:td :class "text-success bg-success bg-opacity-10 text-break" 
                         (spinneret::escape-string (princ-to-string new))))))))
          
          (:div :class "text-muted font-italic" "Aucun changement enregistré (ou création brute).")))))

;; --- 2. LA LISTE DES LOGS (Spécialisée) ---

(defun render-audit-view (req logs)
  (lumen.admin.view:render-admin-layout req
    :title "Journal d'Audit"
    :content
    (with-html-string
      (:div :class "card shadow-sm"
        (:div :class "table-responsive"
          (:table :class "table table-hover align-middle"
            (:thead :class "table-light"
              (:tr (:th "Date") (:th "Utilisateur") (:th "Action") (:th "Cible") (:th "Détails")))
            (:tbody
              (dolist (log logs)
                (let ((id      (alist-get log :id))
                      (date    (alist-get log :created_at))
                      (user    (alist-get log :user_id)) ;; TODO: Résoudre nom
                      (action  (alist-get log :action))
                      (table   (alist-get log :table_name))
                      (rec-id  (alist-get log :record_id))
                      (diff    (alist-get log :diff)))
                  
                  (:tr
                    ;; Date
                    (:td (lumen.utils:format-timestamp date))
                    
                    ;; User (Badge + ID)
                    (:td (:span :class "badge bg-secondary rounded-pill" "User") 
                         (:small :class "ms-1 text-muted font-monospace" (subseq (string user) 0 8)))
                    
                    ;; Action (Couleur)
                    (:td (cond 
                           ((string= action "INSERT") (:span :class "badge bg-success" "CRÉATION"))
                           ((string= action "UPDATE") (:span :class "badge bg-warning text-dark" "MODIFICATION"))
                           ((string= action "DELETE") (:span :class "badge bg-danger" "SUPPRESSION"))
                           (t (:span :class "badge bg-secondary" action))))
                    
                    ;; Cible
                    (:td (:small :class "text-uppercase fw-bold text-muted" table)
                         (:span :class "mx-1" "#")
                         (:small :class "font-monospace" (subseq (string rec-id) 0 8)))
                    
                    ;; Bouton Détail (Modal HTMX)
                    (:td 
                     (:button :class "btn btn-sm btn-outline-primary"
                              :hx-get (lumen.app.app:app-path (format nil "/admin/audit/~A" id))
                              :hx-target "#audit-modal-body"
                              :data-bs-toggle "modal" :data-bs-target "#auditModal"
                              (:i :class "bi bi-eye"))))))))

      ;; MODAL BOOTSTRAP (Conteneur vide rempli par HTMX)
      (:div :class "modal fade" :id "auditModal" :tabindex "-1" :aria-hidden "true"
        (:div :class "modal-dialog modal-lg modal-dialog-centered"
          (:div :class "modal-content"
            (:div :class "modal-header"
              (:h5 :class "modal-title" "Détail du changement")
              (:button :type "button" :class "btn-close" :data-bs-dismiss "modal" :aria-label "Close"))
            (:div :class "modal-body" :id "audit-modal-body"
              (:div :class "text-center py-5" 
                    (:div :class "spinner-border text-primary" :role "status")))
            (:div :class "modal-footer"
              (:button :type "button" :class "btn btn-secondary" :data-bs-dismiss "modal" "Fermer"))))))))))
