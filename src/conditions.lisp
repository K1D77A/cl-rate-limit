(in-package :cl-rate-limit)

;;;;this file contains the conditions used in the rate limiting system

(define-condition bucket-is-full (error)
  ((bucket-is-full-bucket
    :initarg :bucket-is-full-bucket
    :accessor bucket-is-full-bucket) 
   (bucket-is-full-count
    :initarg :bucket-is-full-count 
    :accessor bucket-is-full-count)
   (bucket-is-full-attempted
    :initarg :bucket-is-full-attempted
    :accessor bucket-is-full-attempted)
   (bucket-is-full-message
    :initarg :bucket-is-full-message
    :accessor bucket-is-full-message
    :documentation "Message indicating what when wrong")))


(defun signal-bucket-is-full (bucket count attempted message)
  (error 'bucket-is-full
         :bucket-is-full-bucket bucket
         :bucket-is-full-count count
         :bucket-is-full-attempted attempted
         :bucket-is-full-message message))

(defmethod print-object ((object bucket-is-full) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~&Bucket is full: ~S~%Bucket count: ~S~%Attempted: ~S~%Message: ~A~%"
            (bucket-is-full-bucket object)
            (bucket-is-full-count object)
            (bucket-is-full-attempted object)
            (bucket-is-full-message object))))
