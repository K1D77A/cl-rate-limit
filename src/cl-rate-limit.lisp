;;;; cl-rate-limit.lisp

(in-package #:cl-rate-limit)

(defparameter *rate-limit-table* (make-hash-table :test #'equal))

(defclass bucket-limit ()
  ((q
    ;;:initform (lparallel.vector-queue:make-vector-queue )
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
   (stop-thread-lock
    :initform (bt:make-lock)
    :accessor stop-thread-lock
    :type bt:lock
    :documentation "is grabbed to then stop the thread safely")))

(defun make-bucket (&optional (max-len 10000))
  (make-instance 'bucket-limit :q (lparallel.vector-queue:make-vector-queue max-len)))


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

(defun lassoc (limiter key)
  (access-limiter limiter
    (assoc key limiter :test #'eq)))

(defun get-id (limiter)
  (cadr (lassoc limiter :id)))

(defun get-locked (limiter)
  (cadr (lassoc limiter :lockedp)))

(defun lockedp (limiter)
  (get-locked limiter))

(defun lock (limiter)
  (rplacd (lassoc limiter :lockedp) (list t)))

(defun unlock (limiter)
  (rplacd (lassoc limiter :lockedp) (list nil)))

(defun queue-limiter (id bucket)
  (check-type bucket bucket-limit)
  (let ((limiter (make-limiter id)))
    (if (lparallel.queue:queue-full-p (q bucket))
        (error "QUEUE FULL") ;;need to replace with a condition saying "limiter queue full"
        ;;this can be handled higher up by say returning a 429 or something
        (lparallel.vector-queue:push-vector-queue limiter (q bucket)))
    limiter))

(defun execute-func-when-limiter-free (limiter func)
  (loop :if (lockedp limiter)
          :do (sleep 0.001)
        :else
          :return (funcall func)))

(defmacro bucket-execution (&body body)
  `(execute-func-when-limiter-free (make-limiter ,(gensym))
                                   (lambda () ,@body)))

(defun pop-and-unlock (bucket)
  "Pops from *bucket* and unlocks the limiter. If the queue is empty this function blocks"
  (check-type bucket bucket-limit)
  (unlock
   (lparallel.vector-queue:try-pop-vector-queue (q bucket) :timeout 0.5)));;idk why this isn't external

(declaim (inline rate-per-second-to-sleep-time))
(defun rate-per-second-to-sleep-time (rate)
  (/ 1 rate))

(defun unlock-thread (rate-per-second bucket)
  (let ((time (rate-per-second-to-sleep-time rate-per-second)))
    (loop :do (pop-and-unlock bucket)
              (sleep time)
          :if (stop-thread-p bucket)
            :do (return :STOPPED-SAFE))))
              
            

(defun emergency-stop-unlock-thread (bucket)
  "Destructively kills the thread used to unlock limiters. Only for use in the worst case."
  (if (bt:thread-alive-p (unlocking-thread bucket))
      (bt:destroy-thread (unlocking-thread bucket))
      t))

(defun start-unlock (rate-per-second bucket)
  (unwind-protect (setf (unlocking-thread bucket)
                        (bt:make-thread (lambda () (unlock-thread rate-per-second bucket))))
    (emergency-stop-unlock-thread bucket)))

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

(defun test-limiter ()
  (let ((limiter (make-limiter :test)))
    (bt:make-thread (lambda () (execute-func-when-limiter-free limiter (lambda ()
                                                                    (format t "executed")
                                                                    (force-output t)))))
    (sleep 5)
    (unlock limiter)))


