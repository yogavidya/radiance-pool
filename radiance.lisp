(defpackage :radiance
  (:use :cl  :cl-postgres :fiveam :BORDEAUX-THREADS)
  (:export :with-pooled-query :pool :reset :test-package))

(in-package :radiance)
(fiveam:in-suite* radiance-tests)

;;
;; CLASS: connection-data
;;
(defun postgresql-connector (conn-data)
"success => connection \
 error => (values nil condition)"
  (handler-case
      (cl-postgres:open-database (name conn-data)
                                 (user conn-data)
                                 (password conn-data)
                                 (host conn-data)
                                 (port conn-data))
    (condition (c) (values nil c))))

(defun postgresql-disconnector (conn)
"success => T \
 error => (values nil condition)"
  (handler-case
      (cl-postgres:close-database conn)
    (condition (c) (values nil c))
    (:no-error () t)))

(defun postgresql-executor (conn query-txt)
  (labels 
      ((executor ()
         (handler-case
             (exec-query conn query-txt 'alist-row-reader)
           (database-connection-lost () 
             (cl-postgres:reopen-database conn)
             (executor))
           (error (c) c))))
    (let ((result (executor)))
      (disconnect (connection-data (make-instance 'pool)) conn)
      (if (typep result 'condition)
          (progn 
            (values nil result))
        result))))


(defclass connection-data ()
  ((name :type string :initarg :name :reader name)
   (host :type string :initarg :host :initform "localhost" :reader host)
   (port :type fixnum :initarg :port :initform 5432 :reader port)
   (user :type string :initarg :user :reader user)
   (password :type string :initarg :password :reader password)
   (connect-fn :type function :initarg :connect-fn :initform #'postgresql-connector :reader connect-fn) 
   (disconnect-fn :type function :initarg :disconnect-fn :initform #'postgresql-disconnector :reader disconnect-fn)
   (execute-query-fn :type function :initarg :execute-query-fn :initform #'postgresql-executor :reader execute-query-fn)))


(defmethod connect ((this connection-data))
  (funcall (connect-fn this) this))

(defmethod disconnect ((this connection-data) conn)
  (funcall (disconnect-fn this) conn))

(defmethod execute-query ((this connection-data)  conn (query string))
  (funcall (execute-query-fn this) conn query))

(defparameter *default-connection-data* 
  (make-instance 'connection-data 
                 :name "scratch"
                 :user "scratch-owner"
                 :password "none"))

;;-------------------------------------------
(def-test connection-data ()
  (let* ((bad-connection-data
          (make-instance 'connection-data :name "lkajshd" :user "sdsd" :password "dfsdf"))
         (result-0
          (multiple-value-list
           (connect bad-connection-data)))
         (result-1
          (multiple-value-list
           (connect *default-connection-data*)))
         (result-2
          (and (first result-1)
               (multiple-value-list
                (execute-query *default-connection-data* 
                               (first result-1)
                               "select * from test"))))
         (result-3
          (and (first result-1)
               (multiple-value-list
                (disconnect *default-connection-data*
                            (first result-1))))))
    (is-false (first result-0))
    (is-true (second result-0))
    (and (second result-0) (format t "~%=> Bad (connect) returns: ~A~%" (second result-0)))
    (is-true (first result-1))
    (is-false (second result-1))
    (is-true (first result-2))
    (is-false (second result-2))
    (is-true (first result-3))
    (is-false (second result-3))
    (and (first result-2) (format t "~%=> Successful query returns: ~A~%" (first result-2)))))



;;
;; CLASS: pool
;;
(defclass pool-mc (standard-class)
  ((the-pool :initform nil :accessor the-pool)))

(defmethod c2mop:validate-superclass ((class pool-mc)(superclass standard-class)) T)

(defmethod make-instance :around ((c pool-mc) &key)
  (if (the-pool c) (the-pool c)
    (setf (the-pool c) (call-next-method))))

(defparameter *default-max-connections* 2)

(defclass pool ()
  ((connection-data :type connection-data 
                    :initarg :connection-data 
                    :initform *default-connection-data*
                    :reader connection-data)
   (connections :type (cons cl-postgres:database-connection) 
		:initform nil 
		:accessor connections)
   (used-connections :type (cons cl-postgres:database-connection) 
                     :initform nil 
                     :accessor used-connections)
   (lock :initform (bt:make-lock) 
         :reader lock)
   (max-connections :type fixnum 
                    :initform *default-max-connections*
                    :initarg :max-connections 
                    :reader max-connections))
  (:metaclass pool-mc))


(defmethod overflow-p ((this pool))
  (>= (+ (length (connections this))(length (used-connections this))) (max-connections this)))

(define-condition pool-overflow (condition) ()
  (:report (lambda(condition stream)
             (declare (ignore condition))
             (write-string 
              (format nil "Pool won't open more than ~D connections" 
                      (max-connections (make-instance 'pool))) stream))))

(define-condition pool-null-connection (condition) ()
  (:report (lambda(condition stream)
             (declare (ignore condition))
             (write-string 
              "Attempted pool operation on null connection"
              stream))))


(defmethod connect ((this pool))
  (bt:acquire-lock (lock this) T)
  (if (overflow-p this)
      (progn (bt:release-lock (lock this))
        (values nil (make-condition 'pool-overflow)))
    (let ((conn nil))
      (if (connections this)
          (progn
            (setq conn (pop (connections this)))
            (push conn (used-connections this)))
        (progn
          (setq conn (connect (connection-data this)))
          (push conn (used-connections this))))
      (bt:release-lock (lock this))
      conn)))

(defmethod disconnect ((this pool) conn)
  (if (null conn)
      (values nil (make-condition 'pool-null-connection))
    (progn
      (bt:acquire-lock (lock this) T)
      (setf (used-connections this) 
            (remove conn (used-connections this)))
      (push conn (connections this))
      (bt:release-lock (lock this))
      T)))

(defmethod execute-query ((this pool) conn (query string))
  (execute-query (connection-data this) conn query))

(defmethod reset ((this pool) &key force timeout)
  (when (null timeout) (setf timeout .1))
  (let ((time-count 0))
    (loop until (or force (>= time-count timeout)(used-connections this))
          do
          (sleep .01)
          (incf time-count .01)))
  (when force (setf (slot-value this 'lock) (make-lock)))
  (bt:acquire-lock (lock this) T)
  (loop while (connections this) do
        (disconnect (connection-data this) (pop (connections this))))
  (when force
    (loop while (used-connections this) do
          (disconnect (connection-data this) (pop (used-connections this)))))
  (bt:release-lock (lock this))
  T)
 
(defun force-new-pool (&key max-connections)
  (reset (make-instance 'pool) :force T)
  (setf (slot-value (find-class 'pool) 'the-pool) nil)
  (make-instance 'pool 
                 :max-connections 
                 (if max-connections max-connections *default-max-connections*)))

(defun pool-report ()
  (format t "~%---------------~%pool singleton description:")
  (describe (make-instance 'pool))
  (format t "~%Connections in pool: idle ~S, in use ~D~%---------------~%"
          (length (connections (make-instance 'pool)))
          (length (used-connections (make-instance 'pool)))))

(def-test pool ()
  (let*
      ((result-0 (force-new-pool))
       (result-1 (make-instance 'pool))
       (p (force-new-pool))
       ;create a set of connection results; last one is invalid
       (result-2 (loop for n from 1 to (1+ (max-connections p))
                       collect (multiple-value-bind 
                                   (r c)
                                   (connect p)
                                 (list r c))))
       ;create a set of query results; last one is invalid
       (result-3 (loop for n from 1 to (1+ (max-connections p))
                       collect (multiple-value-bind 
                                   (r c)
                                   (execute-query p (nth (1- n)(used-connections p))
                                                  "select * from test")
                                 (list r c))))
       ;create a set of disconnection results; last one is invalid
       (result-4 (loop for n from 1 to (1+ (max-connections p))
                       collect (multiple-value-bind 
                                   (r c)
                                   (disconnect p (first (used-connections p)))
                                 (list r c)))))
    ; pool singleton
    (is-true (eq result-0 result-1))
    (is-false (eq p result-0))
    ; all but last in result-2 are valid connections
    (is-true (= (length result-2) (1+ (max-connections p))))
    (dolist (connection-result (subseq result-2 0 (1- (max-connections p))))
      (is-true (and (first connection-result) (null (second connection-result)))))
    (is-true (and (null (first (car (last result-2))))
                  (second (car (last result-2)))))
    ; all but last in result-3 are valid query results
    (is-true (= (length result-3) (1+ (max-connections p))))
    (dolist (connection-result (subseq result-3 0 (1- (max-connections p))))
      (is-true (and (first connection-result) (null (second connection-result)))))
    (is-true (and (null (first (car (last result-3))))
                  (second (car (last result-3)))))
    ;all disconnects in result-4 successful  but last
    (is-true (= (length result-4) (1+ (max-connections p))))
    (dolist (connection-result (subseq result-4 0 (1- (max-connections p))))
      (is-true (and (first connection-result) (null (second connection-result)))))
    (is-true (and (null (first (car (last result-4))))
                  (second (car (last result-4)))))
    ;reset successfully empties pool
    (is-false (used-connections p))
    (reset p)
    (is-false (connections p))))
 
(defparameter *concurrent-test-results* nil)
(defparameter *concurrent-test-lock* (make-lock))
(defparameter *concurrent-test-start* nil)
(defparameter *test-output-file-pattern*
  (if (string= (software-type) "Linux") "/srv/pool/pooled.query.~D.txt"
    "d:/pool/pooled.query.~D.txt"))
(defparameter *concurrent-test-index* 0)
(defparameter *concurrent-test-done* nil)
(defparameter *concurrent-tests* 20)

(def-test concurrent-operations ()
  ; ensure pool empty
  (force-new-pool :max-connections 50)
  (loop while (< *concurrent-test-index* *concurrent-tests*) do
    (make-thread 
     (lambda() 
       (loop while (null *concurrent-test-start*) do (sleep .01))
       (let ((start-time (get-internal-real-time))
             (conn (connect (make-instance 'pool))))
         (multiple-value-bind (r c)
             (execute-query (make-instance 'pool) conn  "select * from test")
           (disconnect (make-instance 'pool) conn)
           (acquire-lock *concurrent-test-lock* T)
           (push  `(,$n ,r ,c 
                        (time ,(float (/ (- (get-internal-real-time) start-time) internal-time-units-per-second ))))
                  *concurrent-test-results*)
           (release-lock *concurrent-test-lock*)))
       (when (= $n (1- *concurrent-tests*)) (setf *concurrent-test-done* T)))
     :name (format nil "concurrency thread ~D" *concurrent-test-index*) 
     :initial-bindings `(($n . ,*concurrent-test-index*)))
    (incf *concurrent-test-index*))
  (setf *concurrent-test-start* T)
  (loop while (null *concurrent-test-done*) do
        (sleep .1))
  (acquire-lock *concurrent-test-lock* T)
  (is-true (= *concurrent-tests* (length *concurrent-test-results*)))
  (format t "~%RESULT =>~% ~{~T~A~%~}" *concurrent-test-results*)
  (release-lock *concurrent-test-lock*)
  (pool-report))



(fiveam:debug!)
