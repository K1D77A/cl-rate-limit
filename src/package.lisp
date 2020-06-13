;;;; package.lisp

(defpackage #:cl-rate-limit
  (:use #:cl)
  (:export #:make-bucket
           #:shutdown-bucket
           #:purge-bucket
           #:stop-unlock
           #:emergency-stop-unlock-thread
           #:adjust-rate-per-second
           #:bucket-execution
           #:execute-func-when-limiter-free
           #:bucket-is-full
           #:bucket-is-full-bucket
           #:bucket-is-full-count
           #:bucket-is-full-attempted
           #:bucket-is-full-message
           #:bucket-q-is-locked
           #:bucket-q-is-locked-bucket
           #:bucket-q-is-locked-message
           #:queue-limiter
           ))
