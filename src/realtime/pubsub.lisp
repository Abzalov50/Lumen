(in-package :cl)

(defpackage :lumen.realtime.pubsub
  (:use :cl :alexandria)
  (:import-from :bordeaux-threads
                :make-lock :with-lock-held
                :make-condition-variable :condition-wait :condition-notify)
  (:export
   ;; mailbox
   :make-mailbox :mb-push :mb-pop :mb-close
   ;; bus
   :subscribe :unsubscribe :publish :subscribers :subscribers-count
   ;; (facultatif) états internes
   :*topics* :*topics-lock*))

(in-package :lumen.realtime.pubsub)

(defun %norm-topic (topic)
  (etypecase topic
    (string topic)
    (symbol (string-downcase (symbol-name topic)))))

(defvar *topics*
  (if (boundp '*topics*) *topics* (make-hash-table :test 'equal)))
(defvar *topics-lock*
  (if (boundp '*topics-lock*)
      *topics-lock*
      (bordeaux-threads:make-lock "topics")))

(defun subscribers (topic)
  (let ((tkey (%norm-topic topic)))
    (bordeaux-threads:with-lock-held (*topics-lock*)
      (copy-list (gethash tkey *topics*)))))

(defun subscribers-count (topic)
  (length (subscribers topic)))

(defun subscribe (topic)
  (let* ((tkey (%norm-topic topic))
         (mb (make-mailbox)))
    (bordeaux-threads:with-lock-held (*topics-lock*)
      (setf (gethash tkey *topics*)
            (cons mb (gethash tkey *topics*))))
    mb))

(defun unsubscribe (topic mb)
  (let ((tkey (%norm-topic topic)))
    (bordeaux-threads:with-lock-held (*topics-lock*)
      (setf (gethash tkey *topics*)
            (remove mb (gethash tkey *topics*))))))

(defun publish (topic payload)
  "Retourne le nombre d’abonnés notifiés."
  (let* ((tkey (%norm-topic topic))
         (subs (subscribers tkey)))
    (dolist (mb subs) (mb-push mb payload))
    (length subs)))

(defstruct (mailbox (:constructor %make-mailbox))
  (queue '() :type list)
  (lock  (bordeaux-threads:make-lock "mbox"))
  (cv    (bordeaux-threads:make-condition-variable :name "mbox-cv"))
  (closed-p nil))

(defun make-mailbox () (%make-mailbox))

(defun mb-push (mb msg)
  (bordeaux-threads:with-lock-held ((mailbox-lock mb))
    (when (mailbox-closed-p mb) (return-from mb-push nil))
    (setf (mailbox-queue mb) (nconc (mailbox-queue mb) (list msg)))
    (bordeaux-threads:condition-notify (mailbox-cv mb)))
  t)

(defun mb-pop (mb &key timeout)
  "Bloque jusqu’à un message. Retourne (values msg t).
   Si timeout (secondes) expire: (values nil nil). Si mailbox close: (nil nil)."
  (labels ((pop-one ()
             (when (mailbox-queue mb)
               (let ((msg (first (mailbox-queue mb))))
                 (setf (mailbox-queue mb) (rest (mailbox-queue mb)))
                 (return-from mb-pop (values msg t))))
             (when (mailbox-closed-p mb)
               (return-from mb-pop (values nil nil)))))
    (bordeaux-threads:with-lock-held ((mailbox-lock mb))
      (loop
        (pop-one)
        (if timeout
            (unless (bordeaux-threads:condition-wait (mailbox-cv mb) (mailbox-lock mb) :timeout timeout)
              (return (values nil nil)))
            (bordeaux-threads:condition-wait (mailbox-cv mb) (mailbox-lock mb)))))))

(defun mb-close (mb)
  (bordeaux-threads:with-lock-held ((mailbox-lock mb))
    (setf (mailbox-closed-p mb) t)
    (bordeaux-threads:condition-notify (mailbox-cv mb)))
  t)
