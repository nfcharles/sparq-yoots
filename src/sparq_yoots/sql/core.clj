(ns sparq-yoots.sql.core
  (:import [org.apache.spark.sql functions
                                 SQLContext
                                 Dataset
                                 RelationalGroupedDataset
                                 RowFactory
                                 Row
                                 Column]
           [org.apache.spark.sql.types DataType]))



;; ===================
;; -  Col Functions  -
;; ===================

(defn col-array
  [xs]
  (into-array Column xs))

(defn ^Column -col
  [^String name func as]
  (let [c (func name)]
    (if as (.as c as) c)))

#_(defn ^Column col
  [^String name & {:keys [as]
                   :or {as nil}}]
  (let [c (Column. name)]
    (if as (.as c as) c)))

#_(defn ^Column lit
  [^String name & {:keys [as]
                   :or {as nil}}]
  (let [c (functions/lit name)]
    (if as (.as c as) c)))

(defn ^Column col
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(Column. %) :as as))

(defn ^Column lit
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(functions/lit %) :as as))



(defn ^Dataset filter
  [^Dataset df ^Column condition]
  (.filter df condition))

(defn ^Dataset select
  [^Dataset df & cols]
  "Select expressions"
  (let [#^Column xs (col-array cols)]
    (.select df xs)))

(defn ^RelationalGroupedDataset groupby
  [^Dataset df & cols]
  (.groupBy df (col-array cols)))

(defn ^Dataset aggregate
  [^RelationalGroupedDataset df ^Column col & cols]
  (.agg df col (col-array cols)))

(defn ^Dataset sort
  [^Dataset df & cols]
  (.sort df (col-array cols)))


;; ==================
;; -  Registration  -
;; ==================

;; ---
;; Macro
;; ---

(defn gen-arg-vector
  "Generates function argument vector
  Arity assumed to be not greater than UDF limit."
  [arity & {:keys [init]
            :or {init []}}]
  (loop [xs (take arity (range 97 123))
         acc init]
    (if-let [x (first xs)]
      (recur (rest xs) (conj acc (symbol (str (char x)))))
      acc)))

(defmacro gen-udf
  "Dynamically builds UDF of arity `arity`
  UDF method body is `func` with return type `ret-type`."
  [func arity ret-type]
  (let [arg-vector (with-meta (gen-arg-vector arity :init [(symbol "_")]) {:tag (symbol ret-type)})]
    `(reify ~(symbol (str "org.apache.spark.sql.api.java.UDF" arity))
      (~(symbol "call") ~arg-vector
        (~func ~@(gen-arg-vector arity))))))


;; ---
;; - Reg function
;; ---

(defn register-function
  "Registers UDF function.
  `function` should include argument type hints."
  [^SQLContext sql-context ^String name ^java.io.Serializable func ^DataType ret-type]
  (-> sql-context
      (.udf)
      (.register name func ret-type)))
