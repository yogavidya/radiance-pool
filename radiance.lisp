(defpackage :radiance
  (:use :cl :cl-postgres :fiveam :BORDEAUX-THREADS)
  (:export :with-pooled-query :pool :reset))

(in-package :radiance)

;;
;; CLASS: connection-data
;;
(defun postgresql-connector (conn-data)
  (cl-postgres:open-database (name conn-data)
                             (user conn-data)
                             (password conn-data)
                             (host conn-data)
                             (port conn-data)))

(defun postgresql-disconnector (conn)
  (cl-postgres:close-database conn))

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

;;
;; CLASS: pool
;;
(defclass pool-mc (standard-class)
  ((the-pool :initform nil :accessor the-pool)))

(defmethod validate-superclass ((c1 pool-mc)(c2 standard-class)) T)

(defmethod make-instance :around ((c pool-mc) &key)
  (if (the-pool c) (the-pool c)
    (setf (the-pool c) (call-next-method))))

(defclass pool ()
  ((connection-data :type connection-data :initarg :connection-data :reader connection-data)
   (connections :type (cons cl-postgres:database-connection) :initform nil :accessor connections)
   (used-connections :type (cons cl-postgres:database-connection) :initform nil :accessor used-connections)
   (lock :initform (bt:make-lock) :reader lock))
  (:metaclass pool-mc))

(defmethod validate-superclass ((c1 pool)(c2 pool-mc)) T)

(defmethod connect ((this pool))
  (let ((conn nil))
    (bt:acquire-lock (lock this) T)
    (if (connections this)
        (progn
          (setq conn (pop (connections this)))
          (push conn (used-connections this)))
      (progn
        (setq conn (connect (connection-data this)))
        (push conn (used-connections this))))
    (bt:release-lock (lock this))
    conn))

(defmethod disconnect ((this pool) conn)
  (bt:acquire-lock (lock this) T)
  (setf (used-connections this) 
        (remove conn (used-connections this)))
  (push conn (connections this))
  (bt:release-lock (lock this)))

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
 
(defmacro with-pooled-connection (pool conn-sym &body code)
  `(let ((,conn-sym (connect ,pool)))
     (unwind-protect (progn ,@code)
       (disconnect ,pool ,conn-sym))))

(defmacro with-pooled-query (query-txt result-sym &body code)
  `(with-pooled-connection 
       (make-instance (quote pool)) conn
     (multiple-value-bind (result condition)
         (funcall (execute-query-fn 
                   (connection-data (make-instance (quote pool))))
                  conn ,query-txt)
       (cond 
        (condition (values nil condition))
        ((and ,result-sym (quote ,code))
         (let ((,result-sym result))
           ,@code))
        (T result)))))
                            

(defun make-connection-data ()
  (make-instance 'connection-data 
                 :name "scratch"
                 :user "scratch-owner"
                 :password "none"))

;;
;; TESTS
;;
(fiveam:in-suite* radiance-tests)

(fiveam:def-test creation ()
  (fiveam:finishes 
    (reset (make-instance 'pool 
                          :connection-data (make-connection-data)))))
(fiveam:def-test connection ()
  (fiveam:finishes
    (let ((conn nil)
          (conn-data (make-connection-data)))
      (setq conn (connect conn-data))
      (fiveam:is-true conn)
      (disconnect conn-data conn))))

(fiveam:def-test pooled-connection ()
  (fiveam:finishes
    (let ((conn nil)
          (pool (make-instance 'pool :connection-data (make-connection-data))))
      (setq conn (connect pool))
      (fiveam:is-true conn)
      (pprint (cl-postgres:exec-query conn "select * from test" 'cl-postgres:alist-row-reader))
      (disconnect pool conn)
      (reset pool))))

(fiveam:def-test concurrent-pool-access ()
  (fiveam:finishes
    (let
        ((pool (make-instance 'pool :connection-data (make-connection-data)))
         (backup-special-bindings *default-special-bindings*))
      (loop for accessor-threads from 1 to 10 do
            (setf bt:*default-special-bindings* `(($thread-index . ,accessor-threads)($pool . ,pool)))
            (bt:make-thread
             (lambda ()
               (labels ((query-db (conn)
                          (handler-case
                              (cl-postgres:exec-query conn "select * from test" 'cl-postgres:alist-row-reader)
                            (CL-POSTGRES:DATABASE-CONNECTION-LOST (c) 
                              (cl-postgres:reopen-database conn)
                              (query-db conn)
                              (disconnect $pool conn))
                            (ERROR (c) (invoke-debugger c)))))
                 (let ((conn (connect $pool)))
                   (with-open-file 
                       (s (format nil "d:/pooled.query.~D.txt" $thread-index) 
                          :direction :output :if-exists :supersede :if-does-not-exist :create)
                     (format s "~D> ~A~%"
                             $thread-index
                             (query-db conn))
                     (format s "connections in pool: ~A~%" (connections $pool))
                     (format s "used connections in pool: ~A~%" (used-connections $pool))
                     (format s "current connection: ~A~%" conn))
                   (disconnect $pool conn))))))
      (setf *default-special-bindings* backup-special-bindings)
      (reset pool))))

(def-test with-pooled-query-macro ()
  (multiple-value-bind (result condition)
      (with-pooled-query "select * from test" result (format T "RESULT: ~A~%" result))
    (is-false condition)
    (is-false (used-connections (make-instance 'pool))))
  (reset (make-instance 'pool)))



(fiveam:debug!)
