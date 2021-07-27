(ns sparq-yoots.sql.col
  (:gen-class))



(defn add-alias
  [col alias]
  (if alias
    (format "%s.%s" alias col) col))

(defn add-prefix
  [col prefix]
  (if prefix
    (format "%s_%s" prefix col) col))

(defn add-suffix
  [col suffix]
  (if suffix
    (format "%s_%s" col suffix) col))

(defn add-index
  [col index]
  (if index
    (format "%s[%d]" col index) col))

(defn add-field
  [col field]
  (if field
    (format "%s.%s" col field) col))

(defn add-function
  [col func]
  (if func
    (format "%s(%s)" (first func) (clojure.string/join ", " (concat [col] (rest func))))
    col))

(defn add-cast
  [col _type]
  (if _type
    (format "CAST(%s AS %s)" col _type) col))

(defn add-as
  [col as]
  (if as
    (format "%s AS %s" col as) col))

(defn _col
  [col & {:keys [prefix suffix index field alias func cast as]
          :or {prefix  nil
               suffix  nil
               index   nil
               field   nil
               alias   nil
               func    nil
               cast    nil
               as      nil}}]
  (-> col
      (add-prefix prefix)
      (add-suffix suffix)
      (add-index index)
      (add-field field)
      (add-alias alias)
      (add-function func)
      (add-cast cast)
      (add-as as)))

(defmacro gen-col
  [fn-name col-name]
  `(def ~(symbol fn-name) (partial _col ~col-name)))
