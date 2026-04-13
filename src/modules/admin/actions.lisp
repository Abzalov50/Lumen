(defpackage :lumen.admin.actions
  (:use :common-lisp :lumen.utils)
  (:export :handle-batch-action))

(in-package :lumen.admin.actions)

(defun handle-batch-action (req entity-sym action ids)
  "Dispatche l'action demandée."
  (let ((ctx (lumen.core.http:ctx-from-req req)))
  (case action
    (:delete
     (lumen.data.db:run-in-transaction
      (lambda ()
        (dolist (id ids)
          (lumen.data.repo.core:repo-delete entity-sym ctx id))))
     ;; On renvoie la grille mise à jour (via redirection interne ou fetch-grid)
     (lumen.modules.auth.service:respond-success
      req nil (lumen.app.app:app-path (format nil "/admin/list/~A" entity-sym))
      :msg (format nil "~A éléments supprimés." (length ids))))
    
    (:export-csv
     (export-csv entity-sym ctx ids))
    
    ;; Extension future : Actions customs via defadmin
    (t (error "Action inconnue")))))

;; --- HELPER DE FORMATAGE ---
(defun format-csv-cell (val)
  "Nettoie la valeur pour l'export CSV."
  (typecase val
    ;; 1. NIL devient une chaîne vide (pas "NIL")
    (null "")
    
    ;; 2. T (Booléen) devient "Oui" ou "1"
    ((eql t) "1")
    
    ;; 3. Listes (JSON Arrays ou Listes Lisp) -> "a,b,c"
    (list (format nil "~{~A~^,~}" val))
    
    ;; 4. Strings : on remplace les ; par des espaces pour ne pas casser le CSV
    (string (cl-ppcre:regex-replace-all ";" val " "))
    
    ;; 5. Défaut
    (t (princ-to-string val))))

(defun export-csv (entity-sym ctx ids)
  "Génère le CSV et retourne une RÉPONSE HTTP RAW (sans HTML)."
  (let* ((fields   (lumen.admin.grid:get-display-columns entity-sym))
         (col-keys (mapcar #'first fields))
         ;; Récupération des données brutes
         (rows     (mapcar (lambda (id) (lumen.data.repo.core:repo-show entity-sym ctx id)) ids)))
    
    (let ((csv-content
           (with-output-to-string (s)
             ;; A. EN-TÊTES (Header)
             (format s "~{~A~^;~}~%" (mapcar #'string-capitalize col-keys))
             
             ;; B. LIGNES (Rows)
             (dolist (row rows)
               (format s "~{~A~^;~}~%" 
                       (mapcar (lambda (k) 
                                 (let ((raw-val (lumen.utils:lookup row k)))
                                   ;; On applique le formatage propre
                                   (format-csv-cell raw-val)))
                               col-keys))))))

      ;; C. CONSTRUCTION DE LA RÉPONSE
      ;; On utilise make-response (ou équivalent bas niveau) pour éviter tout wrapping HTML
      (let ((resp (lumen.core.http:respond-html csv-content))) 
        ;; On écrase le Content-Type mis par défaut par respond-html
        (setf (lumen.core.http:resp-headers resp)
              `(("Content-Type" . "text/csv; charset=utf-8")
                ("Content-Disposition" . ,(format nil "attachment; filename=~A-export.csv" 
                                                  (string-downcase entity-sym)))))
        ;; On s'assure que le body est bien la string brute
        (setf (lumen.core.http:resp-body resp) csv-content)
        
        resp))))
