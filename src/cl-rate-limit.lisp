;;;; cl-rate-limit.lisp

(in-package #:cl-rate-limit)

(defparameter *rate-limit-table* (make-hash-table :test #'equal))

(defclass bucket-limit ()
  ((%q
    :initarg :q
    :accessor q
    :type lparallel.vector-queue:vector-queue)
   (%q-capacity
    :initarg :q-capacity
    :reader q-capacity
    :type number)
   (%lock-queue-p
    :accessor lock-queue-p
    :initform nil)
   (%queue-lock-p-lock
    :reader queue-lock-p-lock
    :initform (bt:make-lock))
   (%unlocking-thread
    :initarg :unlocking-thread
    :accessor unlocking-thread)
   (%purge-p
    :accessor purgep
    :initform nil
    :documentation "Setting this flag to T causes the bucket to lock its queue and unlock all
limiters")
   (%purge-p-lock
    :reader purgep-lock
    :initform (bt:make-lock))
   (%stop-thread-p
    :initform nil
    :accessor stop-thread-p
    :type boolean)
   (%stop-thread-lock
    :initform (bt:make-lock)
    :reader stop-thread-lock
    :type bt:lock
    :documentation "is grabbed to then stop the thread safely")
   (%rate-per-second
    :initarg :rate-per-second
    :accessor rate-per-second
    :type number)
   (%rate-per-second-lock
    :reader rate-per-second-lock
    :initform (bt:make-lock))
   ))

(defmethod (setf purgep) (val (bu bucket-limit))
  (check-type val boolean)
  (bt:with-lock-held ((purgep-lock bu))
    ;;(format t "lock grabbed~%")
    (setf (slot-value bu '%purge-p) val)))

(defmethod (setf rate-per-second) (val (bu bucket-limit))
  (bt:with-lock-held ((rate-per-second-lock bu))
    ;;(format t "lock grabbed~%")
    (setf (slot-value bu '%rate-per-second) val)))

(defmethod (setf stop-thread-p) (val (bu bucket-limit))
  (bt:with-lock-held ((stop-thread-lock bu))
    (setf (slot-value bu '%stop-thread-p) val)))

(defmethod (setf lock-queue-p) (val (bu bucket-limit))
  (check-type val boolean)
  (bt:with-lock-held ((queue-lock-p-lock bu))
    (setf (slot-value bu '%lock-queue-p) val)))

(defstruct limiter
  (lock (bt:make-lock))
  id
  (lockedp t))

(defun make-bucket (rate-per-second &optional (max-len 10000))
  "Creates an instance of the class bucket-limit where max-len is the length of the vector-queue used
to store each limiter. This also starts up the thread that unlocks the limiters put on the queue"
  (let ((b (make-instance 'bucket-limit :rate-per-second rate-per-second
                                        :q (lparallel.vector-queue:make-vector-queue max-len)
                                        :q-capacity max-len)))
    (setf (unlocking-thread b) (create-unlock-thread b))
    b))



(defun vpop (q &optional (timeout 0.5))
  (check-type q lparallel.vector-queue:vector-queue)
  (check-type timeout number)
  (lparallel.vector-queue:try-pop-vector-queue q timeout))

(defun vpush (ele q)
  (check-type q lparallel.vector-queue:vector-queue)
  (lparallel.vector-queue:push-vector-queue ele q))

(defun vq-full-p (q)
  (check-type q lparallel.vector-queue:vector-queue)
  (lparallel.vector-queue:vector-queue-full-p q))

(defun vq-empty-p (q)
  (check-type q lparallel.vector-queue:vector-queue)
  (zerop (lparallel.vector-queue:vector-queue-count q)))

(defun get-id (limiter)
  (bt:with-lock-held ((lock limiter))
    (limiter-id limiter)))

(defun lockedp (limiter)
  (bt:with-lock-held ((lock limiter))
    (get-locked limiter)))

(defmethod (setf lockedp) (val (li limiter))
  (check-type val boolean)
  (bt:with-lock-held ((limiter-lock li))
    (setf (slot-value li 'lockedp) val)))

(defmethod (setf id) (val (li limiter))
  (error "ID shouldn't be modified"))

(defun lock-bucket (bucket)
  (setf (lock-queue-p bucket) t))
(defun unlock-bucket (bucket)
  (setf (lock-queue-p bucket) nil))

(defun lock-limiter (limiter)
  (setf (lockedp limiter) t))
(defun unlock-limiter (limiter)
  (setf (lockedp limiter) nil))

(defun queue-limiter (id bucket)
  "Given an a string/symbol as an ID and a BUCKET, attempts to create a new limiter and push it into
the buckets queue, if successful the limiter is returned, if it fails because the buckets queue has
reached maximum capacity then a condition of type BUCKET-IS-FULL is signalled."
  ;;(check-type bucket bucket-limit)
  (let ((limiter (make-limiter :id id)))
    (if (vq-full-p (q bucket))
        (signal-bucket-is-full bucket (lparallel.vector-queue:vector-queue-count (q bucket))
                               limiter "Failed to add new limiter to bucket")
        ;;this can be handled higher up by say returning a 429 or something
        (if (lock-queue-p bucket)
            (signal-bucket-q-is-locked bucket "The bucket Q is locked. Perhaps it is shutting down?")
            (vpush limiter (q bucket))))
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
    (if (eq (type-of popped) 'limiter)
        (unlock-limiter popped)
        (signal-bucket-q-is-empty bucket "Attempted to pop from BUCKET and timed out"))))

(declaim (inline rate-per-second-to-sleep-time))
(defun rate-per-second-to-sleep-time (rate)
  (/ 1 rate))

(defun adjust-rate-per-second (bucket new-rate)
  (setf (rate-per-second bucket) new-rate))

(defun unlock-thread (bucket)
  (loop :if (stop-thread-p bucket)
          :do (return :STOPPED-SAFE)
        :else 
          :do (handler-case (pop-and-unlock bucket)
                (bucket-q-is-empty ()))
              ;;(format t "mt")
              ;;(force-output t)))
              (if (purgep bucket)
                  (sleep 0)
                  (sleep (rate-per-second-to-sleep-time (rate-per-second bucket))))))

(defun emergency-stop-unlock-thread (bucket)
  "Destructively kills the thread used to unlock limiters. Only for use in the worst case."
  (if (bt:thread-alive-p (unlocking-thread bucket))
      (bt:destroy-thread (unlocking-thread bucket))
      t))

(defun create-unlock-thread (bucket)
  (bt:make-thread (lambda ()
                    (unlock-thread bucket))))
;;;need to add a mode where when shutting down the bucket, the queue is locked so that no one can
;;;add to it again, make a condition like "bucket is shutting down, queue shut"
(defun stop-unlock (bucket)
  "Sets (stop-thread-p BUCKET) to t, in an attempt to get the thread to kill itself. However if the
thread does not kill itself within 1 second, perhaps it is sleeping or there is nothing on the queue
so it is blocking then this will destructively kill the thread using EMERGENCY-STOP-UNLOCK-THREAD"
  (setf (stop-thread-p bucket) t)
  (loop :for n :from 1 :upto 1000
        :for alivep := (bt:thread-alive-p (unlocking-thread bucket))
          :then (bt:thread-alive-p (unlocking-thread bucket))
        :if alivep
          :do (sleep 0.001)
        :else 
          :do (return t)
        :finally (emergency-stop-unlock-thread bucket)))
;;;maybe ^ wants to add a force option that simply kills the bucket, and a default option that
;;;stops you from adding to the queue and shuts down the unlocked when the queue has emptied
;;(defun shutdown-bucket (bucket &optional (clear-queue-first t)))

(defun purge-bucket (bucket &optional (unlock t))
  "Given a bucket-limit (BUCKET) this function locks the Q contained within the bucket and PURGEP is 
set to T, meaning the unlock thread will process as fast as it can. Once the Q is empty,
PURGEP is set back to nil and the Q is unlocked once again. If you set UNLOCK to nil then
the bucket is kept locked when the function returns. This function makes no modification to the 
rate-per-second slot in the bucket"
  (lock-bucket bucket)
  (setf (purgep bucket) t)
  (loop :if (vq-empty-p (q bucket))
          :do (when unlock (unlock-bucket bucket))
              (setf (purgep bucket) nil)
              (return t)
        :else
          :do (sleep 0.001)))

(defun shutdown-bucket (bucket &key (purge-first nil)(wait-until-empty t)(empty-speed :default))
  "When passed a bucket-limit (BUCKET) this function attempts to shutdown the bucket"
  (check-type bucket bucket-limit)
  (check-type purge-first boolean)
  (check-type wait-until-empty boolean)
  (check-type empty-speed (or keyword number))
  (let ((current-speed (rate-per-second bucket)))
    (lock-bucket bucket)
    (when purge-first
      (purge-bucket bucket nil)
      (stop-unlock bucket))
    (when wait-until-empty
      (unless (eq empty-speed :default)
        (adjust-rate-per-second bucket empty-speed))
      (loop :if (vq-empty-p (q bucket))
              :do (stop-unlock bucket)
                  (return t)
            :else
              :do (sleep 0.01)))
    (and (not purge-first)
         (not wait-until-empty)
         (stop-unlock bucket))
    (adjust-rate-per-second bucket current-speed)
    :shutdown))

(defun make-full-bucket (rps length)
  (let ((b (make-bucket rps length)))
    ;;(unwind-protect
    (dotimes (i length b)
      (queue-limiter (gensym) b))))
;;    (stop-unlock b))))

(defun fill-bucket-from-mt (bucket)
  (dotimes (i (q-capacity bucket))
    (sleep 0.1)
    (queue-limiter (gensym) bucket)))
  
