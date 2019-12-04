(ns sparq-yoots.sql.core
  (:require [taoensso.timbre :as timbre :refer [info infof]])
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

(defn #^Column col-array
  [xs]
  (into-array Column xs))

(defn ^Column -col
  [^String name func as]
  (let [c (func name)]
    (if as (.as c as) c)))

(defn ^Column col
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(Column. %) as))

(defn ^Column lit
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(functions/lit %) as))

(defn ^Column count
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(functions/count (col %)) as))

(defn ^Column size
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(functions/size (col %)) as))

(defn ^Column date_add
  [^String name days & {:keys [as]
                        :or {as nil}}]
  (-col name #(functions/date_add (col %) days) as))

(defn ^Column date_sub
  [^String name days & {:keys [as]
                        :or {as nil}}]
  (-col name #(functions/date_sub (col %) days) as))

(defn ^Column date_format
  [^String name fmt & {:keys [as]
                       :or {as nil}}]
  (-col name #(functions/date_format (col %) fmt) as))

(defn ^Column and
  [& cols]
  (reduce (fn [a x] (.and a x)) cols))

(defn ^Column or
  [& cols]
  (reduce (fn [a x] (.or a x)) cols))

(defn ^Column expr
  [^String expr & {:keys [as]
                   :or {as nil}}]
  (-col expr #(functions/expr %) as))


;; ---
;; - Generic caller: UDF and built-in functions
;; ---

(defn ^Column udf
  [^String name as & cols]
  (.as (functions/callUDF name (col-array cols)) as))

(defn ^Column call
  [^Column c func as & args]
  #_(infof "COL=%s, ARGS=%s" c args)
  (.as (apply func c args) as))


;; =======================
;; -  Dataset Functions  -
;; =======================

(defn ^Dataset filter
  [^Dataset df ^Column condition]
  (.filter df condition))

(defn ^Dataset select
  [^Dataset df & cols]
  "Select expressions"
  (let [#^Column xs (col-array cols)]
    (.select df xs)))

(defn collect
  [^Dataset df]
  (.collect df))

(defn ^RelationalGroupedDataset groupby
  [^Dataset df & cols]
  (.groupBy df (col-array cols)))

(defn ^Dataset aggregate
  [^RelationalGroupedDataset df ^Column col & cols]
  (.agg df col (col-array cols)))

(defn ^Dataset sort
  [^Dataset df & cols]
  (.sort df (col-array cols)))

(defn ^Dataset left-join
  [^Dataset df ^Column join-cond]
  (.join df join-cond "left"))

(defn ^Dataset join
  [^Dataset df ^Column join-cond]
  (.join df join-cond "inner"))



;; ===================
;; -  UDF Functions  -
;; ===================

;; ---
;; Macro
;; ---

;;
;; TODO: need to implement proper function serialization
;;
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
  (infof "Registering <%s>UDF[%s][%s]" ret-type name func)
  (-> sql-context
      (.udf)
      (.register name func ret-type)))
