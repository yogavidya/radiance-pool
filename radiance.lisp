(defpackage :radiance
  (:use :cl  :cl-postgres :fiveam :BORDEAUX-THREADS)
  (:export :initialize :define-connector :define-disconnector :define-executor 
   :connection-data :*default-connection-data*
   :pool :connect :disconnect :execute-query :reset :force-new-pool :pool-report
   :pooled-query
   :test-package)
  (:documentation "
The RADIANCE package: pooled access to a single database. Designed to be used inside a backend,
it's supposed to never invoke the debugger - to run unattended - and safely handles multi-threaded calls.
Usage:
* general: customizazion
  The POOL object is a singleton. It is created by the first call to any of the following:
  - (MAKE-INSTANCE 'RADIANCE:POOL) => create or access the singleton
  - (RADIANCE:POOL-REPORT) => print information about the singleton; implicitly creates one
  - (RADIANCE:POOLED-QUERY) => returns the result of a query on an implicit pooled connection
  - (RADIANCE:TEST-PACKAGE) => run the test suite, creating/recreating pools as necessary.
  And it is (re)created by
  - (force-new-pool) => dangerous: forcibly closes all pooled connections and recreates the singleton
  Default pool parameters are as follows: local postgresql instance, user 'scratch-owner',
  database 'scratch', password 'none', maximum number of connections: 10.
  Pool parameters must thus be customized before first use by a call to 
  (RADIANCE:INITIALIZE 
   (&KEY 
    (DB-NAME "scratch") ; => database for pooled connections
    (DB-USER "scratch-owner")(db-password "none") ; => credentials for pooled connections
    (DB-HOST "localhost") (db-port 5432) ; => the database host and port for pooled connections
    (DB-CONNECTOR #'postgresql-connector) ; => connection hook
    (DB-DISCONNECTOR #'postgresql-disconnector) ; => disconnection hook
    (DB-EXECUTOR #'postgresql-executor) ; => query execution hook
    (MAX-CONNECTIONS *default-max-connections*) ; => maximum number of pooled connections
    (TEST-QUERY *test-query*)) ; => text of query to be used in test suite
  If the backend for your use case is a PostgreSQL server, you can ignore db-connectior,
  db-disconnector and db-executor.
  If you use a different database engine - or you want to customize default behaviour - you must
  provide your own hook functions. Best way to do this is with the three macros
  DEFINE-CONNECTOR, DEFINE-DISCONNECTOR and DEFINE-EXECUTOR: see below how they are used to generate
  default PostgreSQL hooks. 
  Ça va sans dire: generate your hooks BEFORE your call to (INITIALIZE).
* case 1: tests
  Customize your POOL as described above, then (RADIANCE:TEST-PACKAGE). 
* case 2: production
  Customize your POOL as described above, then (RADIANCE:POOLED-QUERY <query-text>) wherever you need
  to read from or write to your database. Successful operations will return the query result in the format
  defined in your :DB-EXECUTOR (list of rows as alists is the default).
  If a condition is signaled along the chain of implicit connect-execute-disconnect operations,
  POOLED-QUERY will return (VALUES NIL CONDITION)
"))
(in-package :radiance)
(in-suite* 'radiance-tests)


;;
;; CLASS: connection-data
;;
(defmacro define-connector ((name connection-data-var) &body connection-forms)
"Creates a DB-CONNECTOR hook for (INITIALIZE).
IN: NAME: hook's name
    CONNECTION-DATA-VAR: the variable for CONNECTION-DATA
    CONNECTION-FORMS: implicit PROGN using data in CONNECTION-DATA-VAR to create
      and return a connection
OUT: a function named NAME to be used as a :DB-CONNECTOR argument for (INITIALIZE)"
  `(defun ,name (,connection-data-var)
     (handler-case
         (progn ,@connection-forms)
       (condition (c) (values nil c)))))

(defmacro define-disconnector ((name connection-var) &body disconnection-forms)
"Creates a DB-DISCONNECTOR hook for (INITIALIZE).
IN: NAME: hook's name
    CONNECTION-VAR: the connection variable
    DISCONNECTION-FORMS: implicit PROGN to disconnect CONNECTION-VAR
OUT: a function named NAME to be used as a :DB-DISCONNECTOR argument for (INITIALIZE)"  
`(defun ,name (,connection-var)
     (handler-case
         (progn ,@disconnection-forms)
       (condition (c) (values nil c))
       (:no-error () t))))


(defmacro define-executor ((name connection-var query-var error-restarts) &body execute-forms)
    `(defun ,name (,connection-var ,query-var)
       (labels 
           ((executor ()
              (handler-case
                  (progn ,@execute-forms)
                ,@error-restarts
                (error (c) c))))
         (let ((result (executor)))
           (disconnect (connection-data (make-instance 'pool)) ,connection-var)
           (if (typep result 'condition)
               (progn 
                 (values nil result))
             result)))))

(define-connector (postgresql-connector conn-data)
                  (cl-postgres:open-database (name conn-data)
                                             (user conn-data)
                                             (password conn-data)
                                             (host conn-data)
                                             (port conn-data)))

(define-disconnector (postgresql-disconnector conn)
      (cl-postgres:close-database conn))

(define-executor (postgresql-executor conn query-txt ((database-connection-lost () 
             (cl-postgres:reopen-database conn)
             (executor))))
  (exec-query conn query-txt 'alist-row-reader))
#|
(defun postgresql-connector (conn-data)
"Connects using CONN-DATA. 
success => connection 
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
"success => query rows \
 error => (values nil condition)"
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
|#

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
;;
;; connection-data tests
;;
(def-test connection-data ()
  (print "Tests: connection-data")
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
    (is-true (first result-1))
    (is-false (second result-1))
    (is-true (first result-2))
    (is-false (second result-2))
    (is-true (first result-3))
    (is-false (second result-3))))



;;
;; CLASS: pool
;;
(defclass pool-mc (standard-class)
  ((the-pool :initform nil :accessor the-pool)))

(defmethod c2mop:validate-superclass ((class pool-mc)(superclass standard-class)) T)

(defmethod make-instance :around ((c pool-mc) &key)
  (if (the-pool c) (the-pool c)
    (setf (the-pool c) (call-next-method))))

(defparameter *default-max-connections* 10)

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

;;
;; pool tests
;;

(defparameter *test-query* "select * from test")

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
                                                  *test-query*)
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
(defparameter *concurrent-test-index* 0)
(defparameter *concurrent-tests* 50)
(defparameter *concurrent-test-start* nil)
(defparameter *concurrent-tests-done* 0)

(def-test concurrent-operations ()
  ; ensure pool empty
  (force-new-pool :max-connections 90)
  (loop while (< *concurrent-test-index* *concurrent-tests*) do
    (make-thread 
     (lambda() 
       (loop while (null *concurrent-test-start*) do (sleep (/ (random  1000) 1000)))
       (let ((start-time (get-internal-real-time))
             (conn (connect (make-instance 'pool))))
         (multiple-value-bind (r c)
             (execute-query (make-instance 'pool) conn *test-query*)
           (disconnect (make-instance 'pool) conn)
           (with-lock-held (*concurrent-test-lock*)
             (push  `((thread ,$n) (result ,r) (condition ,c) 
                          (time ,(float (/ (- (get-internal-real-time) start-time) internal-time-units-per-second ))))
                    *concurrent-test-results*))))
       (incf *concurrent-tests-done*))
     :name (format nil "concurrency thread ~D" *concurrent-test-index*) 
     :initial-bindings `(($n . ,*concurrent-test-index*)))
    (incf *concurrent-test-index*))
  (setf *concurrent-test-start* T)
  ;wait until all threads done
  (loop while (< *concurrent-tests-done* *concurrent-tests*) do (sleep .1))
  (acquire-lock *concurrent-test-lock* T)
  (dolist (current-result *concurrent-test-results*)
    (is-true (first (assoc 'result current-result))))
  (is-true (= *concurrent-tests* (length *concurrent-test-results*)))
  (release-lock *concurrent-test-lock*))

(defun pooled-query (query)
  (let* ((pool (make-instance 'pool))
         (conn (connect pool)))
    (unwind-protect
        (execute-query pool conn query)
      (disconnect pool conn))))

(defun test-package ()
  (explain! (run 'connection-data))
  (explain! (run 'pool))
  (explain! (run 'concurrent-operations)))

(defun initialize  
       (&key (db-name "scratch") (db-user "scratch-owner") 
             (db-password "none") (db-host "localhost") (db-port 5432)
             (db-connector #'postgresql-connector)
             (db-disconnector #'postgresql-disconnector)
             (db-executor #'postgresql-executor)
             (max-connections *default-max-connections*)
             (test-query *test-query*))
  (setf *default-connection-data* 
        (make-instance 'connection-data
                       :name db-name :host db-host
                       :port db-port :user db-user
                       :password db-password
                       :connect-fn db-connector
                       :disconnect-fn db-disconnector
                       :execute-query-fn db-executor))
  (force-new-pool :max-connections max-connections)
  (setf *test-query* test-query)
T)
