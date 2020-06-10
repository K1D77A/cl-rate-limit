;;;; cl-rate-limit.asd

(asdf:defsystem #:cl-rate-limit
  :description "A library for limiting the rate at which functions can be executed based on a 
unique id"
  :author "K1D77A"
  :license  "MIT"
  :version "0.0.1"
  :depends-on (#:bordeaux-threads
               #:lparallel)
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "conditions")
               (:file "cl-rate-limit")))
