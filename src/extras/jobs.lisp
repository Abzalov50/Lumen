(in-package :cl)

(defpackage :lumen.extras.jobs
  (:use :cl :lumen.core.scheduler :lumen.core.config))

(in-package :lumen.extras.jobs)

(defun %file-older-than-p (path seconds)
  "Vérifie si le fichier n'a pas été modifié depuis X secondes."
  (let ((mtime (file-write-date path))) ;; Retourne un universal-time
    (and mtime 
         (< mtime (- (get-universal-time) seconds)))))

(defun %system-cleanup-spool ()
  "GC: Supprime les fichiers temporaires vieux de plus de 1h."
  (let ((dir *tmp-dir*)
        (count 0))
    (when (uiop:directory-exists-p dir)
      (let ((patterns '("req_*.tmp" "upload_*"))) 
        (dolist (pat patterns)
          (let ((wildcard (merge-pathnames pat dir)))
            (dolist (file (directory wildcard))
              ;; SÉCURITÉ : On ne touche qu'aux fichiers > 3600s (1h)
              (when (%file-older-than-p file 3600)
                (handler-case
                    (progn
                      (delete-file file)
                      (incf count))
                  (error (e)
                    ;; On log juste, ce n'est pas critique
                    (format t "~&[Lumen/GC] Locked file ignored: ~A~%" file)))))))))
    
    (when (> count 0)
      (format t "~&[Lumen/GC] Cleaned ~D stale files.~%" count))))
