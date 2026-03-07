(in-package :cl)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (pushnew :postmodern *features*)
  (pushnew :cl-json *features*)
  (pushnew :uuid *features*)
  (pushnew :local-time *features*)
  (pushnew :cl+ssl *features*)

  (pushnew "hx-" spinneret:*unvalidated-attribute-prefixes* :test #'string-equal)
  (pushnew "accept" spinneret:*unvalidated-attribute-prefixes* :test #'string-equal)
  (pushnew "minlength" spinneret:*unvalidated-attribute-prefixes* :test #'string-equal)
  (pushnew "width" spinneret:*unvalidated-attribute-prefixes* :test #'string-equal)
  )
