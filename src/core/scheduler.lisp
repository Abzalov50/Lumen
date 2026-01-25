(in-package :cl)

(defpackage :lumen.core.scheduler
  (:use :cl)
  (:export :defjob :enqueue :schedule-cron :start-scheduler :stop-scheduler
           :*default-queue* :job-context))

(in-package :lumen.core.scheduler)

;;; ---------------------------------------------------------------------------
;;; 1. LE MODÈLE DE JOB (CLOS)
;;; ---------------------------------------------------------------------------

(defclass job ()
  ((id          :initform (uuid:make-v4-uuid) :accessor job-id)
   (name        :initarg :name      :accessor job-name)
   (payload     :initarg :payload   :initform nil :accessor job-payload)
   (created-at  :initform (get-universal-time) :accessor job-created-at)
   (max-retries :initarg :retries   :initform 3 :accessor job-max-retries)
   (attempts    :initform 0         :accessor job-attempts)
   (handler     :initarg :handler   :accessor job-handler)))

(defvar *job-registry* (make-hash-table :test 'equal) 
  "Mappe les noms de jobs (string) vers leurs fonctions handler.")

(defmacro defjob (name (payload-var) &body body)
  "Définit un type de job exécutable en arrière-plan."
  (let ((job-name (string-downcase (symbol-name name)))
        (fn-name  (intern (format nil "EXEC-JOB-~A" name))))
    `(progn
       ;; La fonction qui fait le travail
       (defun ,fn-name (,payload-var)
         ;; On enveloppe automatiquement dans le tracing
         (lumen.core.trace:with-tracing (,(format nil "Job:~A" job-name) :async t)
           ,@body))
       
       ;; Enregistrement
       (setf (gethash ,job-name *job-registry*) ',fn-name)
       (format t "~&[SCHEDULER] Job registered: ~A~%" ,job-name))))

;;; ---------------------------------------------------------------------------
;;; 2. LA QUEUE (Thread-Safe)
;;; ---------------------------------------------------------------------------

(defstruct job-queue
  (items (make-array 10 :adjustable t :fill-pointer 0))
  (lock  (bt:make-lock "job-queue-lock"))
  (sem   (bt:make-semaphore :count 0)))

(defvar *default-queue* (make-job-queue))

(defun enqueue (job-sym payload &key (retries 3))
  "Pousse un job dans la file d'attente."
  (let* ((name (string-downcase (symbol-name job-sym)))
         (job  (make-instance 'job 
                              :name name 
                              :payload payload 
                              :handler (gethash name *job-registry*)
                              :retries retries)))
    
    (unless (job-handler job)
      (error "Job inconnu: ~A. Avez-vous utilisé DEFJOB ?" name))
    
    (bt:with-lock-held ((job-queue-lock *default-queue*))
      (vector-push-extend job (job-queue-items *default-queue*)))
    
    (bt:signal-semaphore (job-queue-sem *default-queue*))
    (job-id job)))

;;; ---------------------------------------------------------------------------
;;; 3. LE WORKER (Consommateur)
;;; ---------------------------------------------------------------------------

(defvar *worker-thread* nil)
(defvar *running* nil)

(defun %worker-loop ()
  (format t "~&[SCHEDULER] Worker started.~%")
  (loop while *running* do
    ;; Attente bloquante (sans CPU spin) jusqu'à ce qu'un job arrive
    (bt:wait-on-semaphore (job-queue-sem *default-queue*))
    
    (when *running* ;; Double check après le réveil
      (let ((job nil))
        ;; Récupération thread-safe
        (bt:with-lock-held ((job-queue-lock *default-queue*))
          (when (> (length (job-queue-items *default-queue*)) 0)
            (setf job (vector-pop (job-queue-items *default-queue*)))))
        
        (when job
          (handler-case
              (let ((t0 (get-internal-real-time)))
                (incf (job-attempts job))
                ;; Exécution du Handler
                (funcall (job-handler job) (job-payload job))
                
                (format t "~&[SCHEDULER] Job ~A OK (~,2Fms)~%" 
                        (job-name job)
                        (/ (- (get-internal-real-time) t0) 1000.0)))
            
            (error (e)
              (format t "~&[SCHEDULER] Error on job ~A: ~A~%" (job-name job) e)
              ;; Retry Logic basique
              (if (< (job-attempts job) (job-max-retries job))
                  (progn
                    (format t "~&[SCHEDULER] Retrying job ~A...~%" (job-id job))
                    (sleep 1) ;; Backoff simple
                    ;; On remet dans la queue
                    (bt:with-lock-held ((job-queue-lock *default-queue*))
                      (vector-push-extend job (job-queue-items *default-queue*)))
                    (bt:signal-semaphore (job-queue-sem *default-queue*)))
                  (format t "~&[SCHEDULER] Job ~A abandoned.~%" (job-id job))))))))))

;;; ---------------------------------------------------------------------------
;;; 4. LE CRON (Planificateur)
;;; ---------------------------------------------------------------------------

(defvar *cron-entries* nil)
(defvar *cron-thread* nil)

(defstruct cron-entry name interval-sec last-run payload)

(defun schedule-cron (job-sym interval-sec &optional payload)
  (push (make-cron-entry :name (string-downcase (symbol-name job-sym))
                         :interval-sec interval-sec
                         :last-run (get-universal-time)
                         :payload payload)
        *cron-entries*))

(defun %cron-loop ()
  (format t "~&[SCHEDULER] Cron started.~%")
  (loop while *running* do
    (let ((now (get-universal-time)))
      (dolist (entry *cron-entries*)
        (when (>= (- now (cron-entry-last-run entry)) (cron-entry-interval-sec entry))
          ;; C'est l'heure ! On délègue au worker via enqueue
          (enqueue (intern (string-upcase (cron-entry-name entry)) :keyword) 
                   (cron-entry-payload entry))
          (setf (cron-entry-last-run entry) now))))
    (sleep 1)))

;;; ---------------------------------------------------------------------------
;;; 5. API PUBLIQUE
;;; ---------------------------------------------------------------------------

(defun start-scheduler ()
  (unless *running*
    (setf *running* t)
    (setf *worker-thread* (bt:make-thread #'%worker-loop :name "lumen-worker"))
    (setf *cron-thread* (bt:make-thread #'%cron-loop   :name "lumen-cron"))))

(defun stop-scheduler ()
  (setf *running* nil)
  (bt:signal-semaphore (job-queue-sem *default-queue*)) ;; Réveil du worker pour qu'il s'arrête
  (when *worker-thread* (bt:join-thread *worker-thread*))
  (when *cron-thread* (bt:join-thread *cron-thread*))
  (format t "~&[SCHEDULER] Stopped.~%"))
