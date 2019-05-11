(in-package :radiance)

;;
;; CLASS: pool
;;
(defclass pool-mc (standard-class)
  ((the-pool :initform nil :accessor the-pool))
  (:documentation "The metaclass for POOL that implements singleton behaviour"))

(defmethod c2mop:validate-superclass ((class pool-mc)(superclass standard-class)) T)

(defmethod make-instance :around ((c pool-mc) &key)
"Creates a POOL instance if necessary, else returns existing instance" 
  (if (the-pool c) (the-pool c)
    (setf (the-pool c) (call-next-method))))

(defparameter *default-max-connections* 10)

(defclass pool ()
  ((connection-data :type connection-data 
                    :initarg :connection-data 
                    :initform *default-connection-data*
                    :reader connection-data
                    :documentation "CONNECTION-DATA for the POOL")
   (connections :type (cons cl-postgres:database-connection) 
		:initform nil 
		:accessor connections
                :documentation "List of idle connections")
   (used-connections :type (cons cl-postgres:database-connection) 
                     :initform nil 
                     :accessor used-connections
                     :documentation "List of busy connections")
   (lock :initform (bt:make-lock) 
         :reader lock
         :documentation "Used to ensure safe multi-threaded operation")
   (max-connections :type fixnum 
                    :initform *default-max-connections*
                    :initarg :max-connections 
                    :reader max-connections
                    :documentation "Total number (idle+busy) of connections allowed"))
  (:metaclass pool-mc))


(defmethod overflow-p ((this pool))
  "Checks if total connections >= MAX-CONNECTIONS"
  (>= (+ (length (connections this))(length (used-connections this))) (max-connections this)))

(define-condition pool-overflow (condition) ()
  (:report (lambda(condition stream)
             (declare (ignore condition))
             (write-string 
              (format nil "Pool won't open more than ~D connections" 
                      (max-connections (make-instance 'pool))) stream)))
  (:documentation "Returned by operations that require more than MAX-CONNECTIONS"))

(define-condition pool-null-connection (condition) ()
  (:report (lambda(condition stream)
             (declare (ignore condition))
             (write-string 
              "Attempted pool operation on null connection"
              stream)))
  (:documentation "Returned by operations on a null connection"))


(defmethod connect ((this pool))
  "Multi-threaded safe acquisition of a pooled connection"
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
  "Multi-threaded safe release of a pooled connection"
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
  "Returns query rows or (VALUES NIL CONDITION)"
  (execute-query (connection-data this) conn query))

(defmethod reset ((this pool) &key force timeout)
"Closes idle connections and empties idle list. Will wait TIMEOUT seconds
 (default .1) for busy list to empty if FORCEd. 
  IN: FORCE: Forces closing busy connextions and emptying busy list. Dangerous.
    TIMEOUT: Seconds to wait for busy list to become empty if FORCE, default .1"
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
  "(RESET :FORCE T) on the pool, then create a new one with :MAX-CONNECTIONS (default: 10)"
  (reset (make-instance 'pool) :force T)
  (setf (slot-value (find-class 'pool) 'the-pool) nil)
  (make-instance 'pool 
                 :max-connections 
                 (if max-connections max-connections *default-max-connections*)))

(defun pool-report ()
  "Outputs a description of the POOL to *standard-output*"
  (format t "~%---------------~%pool singleton description:")
  (describe (make-instance 'pool))
  (format t "~%Connections in pool: idle ~S, in use ~D~%---------------~%"
          (length (connections (make-instance 'pool)))
          (length (used-connections (make-instance 'pool)))))
