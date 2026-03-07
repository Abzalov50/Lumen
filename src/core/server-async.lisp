(in-package :cl)

(defpackage :lumen.core.server-async
  (:use :cl :alexandria)
  (:export :start-async :stop-async))

(in-package :lumen.core.server-async)

(defvar *async-thread* nil
  "Le thread unique qui fera tourner la boucle d'événements.")

(defvar *async-server* nil
  "L'objet serveur TCP asynchrone.")

(defun %write-async-response (socket response)
  "Convertit l'objet lumen response en octets et l'envoie de manière non-bloquante."
  (let* ((status (lumen.core.http:resp-status response))
         (body (lumen.core.http:resp-body response))
         (headers (lumen.core.http:resp-headers response))
         ;; (Simplification : on suppose ici que le body est une string ou un byte-vector)
         (body-bytes (if (stringp body) (trivial-utf-8:string-to-utf-8-bytes body) body))
         (len (length body-bytes)))
    
    (with-output-to-string (s)
      (format s "HTTP/1.1 ~A OK~C~C" status #\Return #\Newline)
      (dolist (h headers)
        (format s "~A: ~A~C~C" (car h) (cdr h) #\Return #\Newline))
      (format s "Content-Length: ~A~C~C" len #\Return #\Newline)
      (format s "Connection: keep-alive~C~C~C~C" #\Return #\Newline #\Return #\Newline))
    
    ;; 1. Envoi des en-têtes (non-bloquant)
    (as:write-socket-data socket (trivial-utf-8:string-to-utf-8-bytes (get-output-stream-string s)))
    ;; 2. Envoi du corps (non-bloquant)
    (as:write-socket-data socket body-bytes)))

(defun start-async (&key (port 8080) handler)
  "Démarre le serveur Lumen en mode Event Loop pure avec fast-http."
  (format t "~&[ASYNC SERVER] Démarrage sur le port ~A...~%" port)
  
  (setf *async-thread*
        (bt:make-thread
         (lambda ()
           (as:start-event-loop
            (lambda ()
              (setf *async-server*
                    (as:tcp-server "0.0.0.0" port
                     
				   ;; CALLBACK 1 : DONNÉES REÇUES SUR LE SOCKET
				   (lambda (socket data)
				     (let ((http-state (as:socket-data socket)))
                         
				       ;; Si c'est le tout premier paquet de la requête, on initialise le parseur
				       (unless http-state
					 (let* ((http (fast-http:make-http-request))
						(body-buffer (make-array 0 :element-type '(unsigned-byte 8)
									   :adjustable t :fill-pointer 0))
                                  
						;; INITIALISATION DU PARSEUR AVEC SES CALLBACKS C-STYLE
						(parser (fast-http:make-parser http
									       ;; Callback pour capturer le corps s'il y en a un (ex: POST)
									       :body-callback (lambda (chunk start end)
												(loop for i from start below end
												      do (vector-push-extend (aref chunk i) body-buffer)))
                                            
									       ;; CALLBACK DE FIN DE REQUÊTE (Remplace le handler-case)
									       :message-complete-callback 
									       (lambda ()
										 ;; 1. La requête est entièrement reçue, on construit l'objet Lumen
										 (let* ((headers-alist (loop for (k v) on (fast-http:http-headers http) by #'cddr
													     collect (cons (string-downcase (string k)) v)))
											(req (make-instance 'lumen.core.http:request
													    :method (string (fast-http:http-method http))
													    :path (fast-http:http-resource http)
													    :headers headers-alist
													    :body-stream body-buffer)))
                                                
										   ;; 2. On appelle votre proxy/routeur
										   (let ((response (funcall handler req)))
                                                  
										     ;; 3. On envoie la réponse
										     (%write-async-response socket response)))))))
                             
					   ;; On sauvegarde l'état dans la mémoire du socket
					   (setf http-state (list :http http :parser parser))
					   (setf (as:socket-data socket) http-state)))
                         
				       ;; On "nourrit" le parseur avec les octets bruts. 
				       ;; C'est lui qui déclenchera les callbacks tout seul quand il aura fini.
				       (funcall (getf http-state :parser) data)))
                     
				   ;; CALLBACK 2 : ERREUR RÉSEAU
				   (lambda (err)
				     (format t "~&[ASYNC NETWORK ERROR] ~A~%" err)))))))
         :name "lumen-event-loop"))
  t)

(defun stop-async ()
  "Arrête la boucle d'événements."
  (when *async-server*
    (as:close-tcp-server *async-server*)
    (setf *async-server* nil))
  ;; Arrêt brutal de la boucle
  (as:exit-event-loop)
  (format t "~&[ASYNC SERVER] Arrêté.~%"))
