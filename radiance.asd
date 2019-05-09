(defsystem "radiance"
  :depends-on ("usocket" "closer-mop" "cl-postgres" "fiveam" "bordeaux-threads")
  :components ((:file "radiance")))
