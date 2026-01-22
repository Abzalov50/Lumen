(in-package :cl)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (pushnew :postmodern *features*)
  (pushnew :cl-json *features*)
  (pushnew :uuid *features*)
  (pushnew :local-time *features*)
  (pushnew :cl+ssl *features*)
  )
