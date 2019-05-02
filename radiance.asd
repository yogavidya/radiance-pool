(defsystem "radiance"
  :depends-on ("usocket" "cl-postgres" "fiveam" "bordeaux-threads")
  :components ((:file "radiance")))
