(in-package :cl)

(defpackage :lumen.core.trace
  (:use :cl)
  (:export :with-tracing :print-trace-waterfall :*trace-root*
	   :current-context :with-propagated-context))

(in-package :lumen.core.trace)

;; On utilise DEFPARAMETER pour forcer le reset de la table si on recharge le fichier
(defparameter *thread-contexts* (make-hash-table :test 'equal))
(defvar *context-lock* (bt:make-lock "trace-context-lock"))

(defclass trace-context ()
  ((root    :initform nil :accessor ctx-root)
   (current :initform nil :accessor ctx-current)))

;; --- IDENTIFIANT UNIQUE STABLE ---
(defun %get-thread-id ()
  ;; On utilise le nom + l'adresse mémoire pour être sûr et stable
  (format nil "~A-~A" (bt:thread-name (bt:current-thread)) (bt:current-thread)))

(defun %get-thread-ctx ()
  (let ((tid (%get-thread-id)))
    (bt:with-lock-held (*context-lock*)
      (or (gethash tid *thread-contexts*)
          (setf (gethash tid *thread-contexts*) (make-instance 'trace-context))))))

(defun %clear-thread-ctx ()
  (let ((tid (%get-thread-id)))
    (bt:with-lock-held (*context-lock*)
      (remhash tid *thread-contexts*))))

;; --- STRUCTURE ---
(defclass trace-node ()
  ((name :initarg :name :accessor trace-name)
   (tags :initarg :tags :initform nil :accessor trace-tags)
   (start-ts :initarg :start-ts :accessor trace-start)
   (end-ts :initarg :end-ts :initform nil :accessor trace-end)
   (children :initform (make-array 0 :adjustable t :fill-pointer 0) :accessor trace-children)
   (parent :initarg :parent :initform nil :accessor trace-parent)))

(defun %enter-trace (name tags)
  (let* ((ctx (%get-thread-ctx))
         (parent (ctx-current ctx))
         (now (get-internal-real-time))
         ;;(tid (%get-thread-id))
	 )
    
    ;; LOG DE DÉBOGAGE POUR VOUS
    ;;(format t "~&[TRACE-DEBUG] Thread[~A] Entrée: '~A'. Parent actuel? ~A~%" 
    ;;        tid name (if parent (trace-name parent) "NIL (Racine?)"))
    
    (let ((node (make-instance 'trace-node :name name :tags tags :start-ts now :parent parent)))
      (if parent
          (vector-push-extend node (trace-children parent))
          (unless (ctx-root ctx)
            (setf (ctx-root ctx) node)))
      (setf (ctx-current ctx) node)
      node)))

(defun %exit-trace (node)
  (let ((ctx (%get-thread-ctx)))
    (setf (trace-end node) (get-internal-real-time))
    (setf (ctx-current ctx) (trace-parent node))))

;; --- FONCTION & MACRO ---
(defun call-with-tracing (name tags thunk)
  (let ((node (%enter-trace name tags)))
    (unwind-protect
         (funcall thunk)
      (%exit-trace node))))

(defmacro with-tracing ((name &rest tags) &body body)
  ;; Si vous voyez ce log à la compilation, c'est que la macro est à jour
  ;;(format t "~&[MACRO-EXPAND] Expansion de with-tracing pour ~A~%" name)
  `(call-with-tracing ,name (list ,@tags) (lambda () ,@body)))

;; --- RENDU ---
(defun %ms (ticks)
  (float (/ (* ticks 1000) internal-time-units-per-second)))

(defvar *trace-root* nil) ;; Legacy

(defun print-trace-waterfall (&optional root (stream *standard-output*))
  (let* ((ctx (%get-thread-ctx))
         (r (or root (ctx-root ctx))))
    (if r
        (let ((base-start (trace-start r)))
          (labels ((rec (node depth)
                     (let* ((start-rel (%ms (- (trace-start node) base-start)))
                            (dur (if (trace-end node) (%ms (- (trace-end node) (trace-start node))) 0.0))
                            (indent (make-string (* depth 2) :initial-element #\Space)))
                       (format stream "~&~A[+~8,2Fms] ~A (~,2Fms) ~A~%" indent start-rel (trace-name node) dur (or (trace-tags node) ""))
                       (loop for child across (trace-children node) do (rec child (1+ depth))))))
            (format stream "~&--- TRACE WATERFALL (Thread: ~A) ---~%" (%get-thread-id))
            (rec r 0)
            (format stream "-----------------------~%")))
        (format stream "~&[TRACE] Pas de racine trouvée pour le thread ~A.~%" (%get-thread-id)))))

(defun current-context ()
  "Récupère l'objet contexte du thread courant (pour le passer à un autre thread)."
  (%get-thread-ctx))

(defmacro with-propagated-context (parent-ctx &body body)
  "Exécute BODY dans le thread courant MAIS en utilisant le contexte du PARENT."
  (let ((ctx-var (gensym "CTX"))
        (tid-var (gensym "TID")))
    `(let ((,ctx-var ,parent-ctx)
           (,tid-var (%get-thread-id)))
       ;; On injecte manuellement le contexte du parent dans la case du thread courant
       (bt:with-lock-held (*context-lock*)
         (setf (gethash ,tid-var *thread-contexts*) ,ctx-var))
       
       (unwind-protect
            (progn ,@body)
         ;; Nettoyage : on retire l'entrée car ce thread ne possède pas le contexte
         (bt:with-lock-held (*context-lock*)
           (remhash ,tid-var *thread-contexts*))))))
