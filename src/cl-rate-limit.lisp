;;;; cl-rate-limit.lisp

(in-package #:cl-rate-limit)

(defparameter *rate-limit-table* (make-hash-table :test #'equal))

(defclass bucket-limit ()
  ((q
    :initarg :q
    :accessor q
    :type lparallel.vector-queue:vector-queue)
   (unlocking-thread
    :initarg :unlocking-thread
    :accessor unlocking-thread)
   (stop-thread-p
    :initform nil
    :accessor stop-thread-p
    :type boolean)
   (rate-per-second
    :initarg :rate-per-second
    :accessor rate-per-second
    :type number)
   (stop-thread-lock
    :initform (bt:make-lock)
    :accessor stop-thread-lock
    :type bt:lock
    :documentation "is grabbed to then stop the thread safely")))

(defun make-bucket (rate-per-second &optional (max-len 10000))
  "Creates an instance of the class bucket-limit where max-len is the length of the vector-queue used
to store each limiter. This also starts up the thread that unlocks the limiters put on the queue"
  (let ((b (make-instance 'bucket-limit :rate-per-second rate-per-second
                                        :q (lparallel.vector-queue:make-vector-queue max-len))))
    (setf (unlocking-thread b) (create-unlock-thread rate-per-second b))
    b))

(defun make-limiter (id)
  (list (list :lock (bt:make-lock))(list :id id)(list :lockedp t)))

(defparameter *limiter* (make-limiter "oof2"))

(defmacro access-limiter (limiter &body body)
  `(let ((lock (cadr (assoc :lock ,limiter :test #'eq))))
     (unwind-protect
          (prog2 (bt:acquire-lock lock)
              ,@body
            (bt:release-lock lock))
       (bt:release-lock lock))))

(defun limiterp (list)
  "Takes in a list and checks whether it is a limiter"
  (when (= 3 (length list))
    (destructuring-bind (fi se th)
        list
      (and (eq (car fi) :lock)
           (not (null (cadr fi)))
           (eq (car se) :id)
           (not (null (cadr se)))
           (eq (car th) :lockedp)
           (or (eq (cadr th) nil)
               (eq (cadr th) t))))))

(deftype limiter () `(satisfies limiterp))

(defun vpop (q &optional (timeout 0.5))
  (check-type q lparallel.vector-queue:vector-queue)
  (check-type timeout (float 0 *))
  (lparallel.vector-queue:try-pop-vector-queue q timeout))

(defun vpush (ele q)
  (check-type q lparallel.vector-queue:vector-queue)
  (lparallel.vector-queue:push-vector-queue ele q))

(defun vq-full-p (q)
  (check-type q lparallel.vector-queue:vector-queue)
  (lparallel.vector-queue:vector-queue-full-p q))

(defun lassoc (limiter key)
  (access-limiter limiter
    (assoc key limiter :test #'eq)))

(defun get-id (limiter)
  (check-type limiter limiter)
  (cadr (lassoc limiter :id)))

(defun get-locked (limiter)
  (check-type limiter limiter)
  (cadr (lassoc limiter :lockedp)))

(defun lockedp (limiter)
  (get-locked limiter))

(defun lock (limiter)
  (check-type limiter limiter)
  (rplacd (lassoc limiter :lockedp) (list t)))

(defun unlock (limiter)
  (check-type limiter limiter)
  (rplacd (lassoc limiter :lockedp) (list nil)))

(defun queue-limiter (id bucket)
  "Given an a string/symbol as an ID and a BUCKET, attempts to create a new limiter and push it into
the buckets queue, if successful the limiter is returned, if it fails because the buckets queue has
reached maximum capacity then a condition of type BUCKET-IS-FULL is signalled."
  ;;(check-type bucket bucket-limit)
  (let ((limiter (make-limiter id)))
    (if (vq-full-p (q bucket))
        (signal-bucket-is-full bucket (lparallel.vector-queue:vector-queue-count (q bucket))
                               limiter "Failed to add new limiter to bucket")
        ;;this can be handled higher up by say returning a 429 or something
        (vpush limiter (q bucket)))
    limiter))

(defun execute-func-when-limiter-free (limiter func)
  (loop :if (lockedp limiter)
          :do (sleep 0.001)
        :else
          :return (funcall func)))

(defmacro bucket-execution (bucket &body body)
  `(execute-func-when-limiter-free (queue-limiter ',(gensym) ,bucket)
                                   (lambda () ,@body)))

(defun pop-and-unlock (bucket &optional (timeout 0.5))
  "Pops from BUCKETs queue and unlocks the limiter. If the queue within BUCKET is empty and 
the TIMEOUT is reached then a condition of type BUCKET-Q-IS-EMPTY is signalled"
  (check-type bucket bucket-limit)
  (let ((popped (vpop (q bucket) timeout)))
    (if (limiterp popped)
        (unlock popped)
        (signal-bucket-q-is-empty bucket "Attempted to pop from BUCKET and timed out"))))

(declaim (inline rate-per-second-to-sleep-time))
(defun rate-per-second-to-sleep-time (rate)
  (/ 1 rate))

(defun unlock-thread (bucket)
  (loop :if (stop-thread-p bucket)
          :do (return :STOPPED-SAFE)
        :else 
          :do (handler-case (prog1 (pop-and-unlock bucket)
                              (format t "popping~%")
                              (force-output));;maybe we could make a restart
                (bucket-q-is-empty ()
                  (format t "mt")
                  (force-output t)))
              (sleep (rate-per-second-to-sleep-time (rate-per-second bucket)))))

(defun emergency-stop-unlock-thread (bucket)
  "Destructively kills the thread used to unlock limiters. Only for use in the worst case."
  (if (bt:thread-alive-p (unlocking-thread bucket))
      (bt:destroy-thread (unlocking-thread bucket))
      t))

(defun create-unlock-thread (rate-per-second bucket)
  (bt:make-thread (lambda ()
                    (unlock-thread rate-per-second bucket))))

(defun stop-unlock (bucket)
  "Sets (stop-thread-p BUCKET) to t, in an attempt to get the thread to kill itself. However if the
thread does not kill itself within 1 second, perhaps it is sleeping or there is nothing on the queue
so it is blocking then this will destructively kill the thread using EMERGENCY-STOP-UNLOCK-THREAD"
  (bt:with-lock-held ((stop-thread-lock bucket))
    (setf (stop-thread-p bucket) t))
  (loop :for n :from 1 :upto 1000
        :for alivep := (bt:thread-alive-p (unlocking-thread bucket))
          :then (bt:thread-alive-p (unlocking-thread bucket))
        :if alivep
          :do (sleep 0.001)
        :else 
          :do (return t)
        :finally (emergency-stop-unlock-thread bucket)))
;;;maybe ^ wants to add a force option that simply kills the bucket, and a default option that
;;;stops you from adding to the queue and shuts down the unlocker when the queue has emptied
(defun test-limiter ()
  (let ((limiter (make-limiter :test)))
    (bt:make-thread (lambda () (execute-func-when-limiter-free limiter (lambda ()
                                                                    (format t "executed")
                                                                    (force-output t)))))
    (sleep 5)
    (unlock limiter)))


