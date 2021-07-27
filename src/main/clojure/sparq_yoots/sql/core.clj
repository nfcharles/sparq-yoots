(ns sparq-yoots.sql.core
  (:require [taoensso.timbre :as timbre :refer [info infof]])
  (:import [org.apache.spark.sql functions
                                 SQLContext
                                 Dataset
                                 RelationalGroupedDataset
                                 RowFactory
                                 Row
                                 Column]
           [org.apache.spark.sql.catalyst.expressions GenericRowWithSchema]
           [org.apache.spark.sql.types DataType StructType]
           [org.apache.spark.sql.expressions Window]))




(defn invalid-column-type-exception
  [x]
  (java.lang.Exception. (format "Invalid column type: %s" (type x))))


;; ===================
;; -  Col Functions  -
;; ===================

(defn ^Column -col
  [^String name func as]
  (let [c (func name)]
    (if as (.as c as) c)))

(defn ^Column col
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(Column. %) as))

(defn #^Column col-array
  [xs]
  (into-array Column xs))

(defn #^Column column-array
  [c & cols]
  (loop [xs (concat [c] cols)
         acc []]
    (if-let [x (first xs)]
      (recur (rest xs) (conj acc (col x)))
      (col-array acc))))

(defn #^String string-array
  [xs]
  (into-array String xs))

(defn ^Column lit
  [^String name & {:keys [cast as]
                   :or {cast nil
                        as nil}}]
  (let [x (-col name #(functions/lit %) as)]
    (if cast (.cast x cast) x)))

(defn ^Column count
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(functions/count (col %)) as))

(defn ^Column size
  [^String name & {:keys [as]
                   :or {as nil}}]
  (-col name #(functions/size (col %)) as))

(defn ^Column array
  [col & cols]
  (infof "array.COL_TYPE=%s" (type col))
  (condp = (type col)
    String (let [#^String xs (string-array cols)]
             (functions/array col xs))
    Column (let [#^Column xs (col-array (concat [col] cols))]
             (functions/array xs))
    :else (throw (invalid-column-type-exception col))))

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

(defn ^Column over
  [^String _expr ^Window window & {:keys [as]
                                :or {as nil}}]
  (let [ret (.over (expr _expr) window)]
    (if as
      (.alias ret as)
      ret)))

(defn ^Column struct
  [& cols]
  (functions/struct (col-array cols)))


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


;; ===================
;; -  Row Functions  -
;; ===================

(defn ^GenericRowWithSchema row
  [xs]
  (RowFactory/create
    (object-array xs)))


(defn bool-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getBoolean row index))
    (.getBoolean row index)))

(defn byte-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getByte row index))
    (.getByte row index)))

(defn int-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getInt row index))
    (.getInt row index)))

(defn long-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getLong row index))
    (.getLong row index)))

(defn float-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getFloat row index))
    (.getFloat row index)))

(defn double-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getDouble row index))
    (.getDouble row index)))

(defn timestamp-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getTimestamp row index))
    (.getTimestamp row index)))

(defn date-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getDate row index))
    (.getDate row index)))

(defn list-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getList row index))
    (.getList row index)))

(defn map-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getMap row index))
    (.getMap row index)))

(defn struct-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getStruct row index))
    (.getStruct row index)))

(defn string-col
  [index ^StructType row & {:keys [nullable]
                            :or {nullable true}}]
  (if nullable
    (if (not (.isNullAt row index))
      (.getString row index))
    (.getString row index)))



;; =======================
;; -  Dataset Functions  -
;; =======================

(defn ^Dataset filter
  [^Dataset df ^Column condition]
  (.filter df condition))

(defn ^Dataset aggregate
  [^RelationalGroupedDataset df ^Column col & cols]
  (.agg df col (col-array cols)))

(defn collect
  [^Dataset df]
  (.collect df))

(defn ^Dataset drops
  [^Dataset df & cols]
  (loop [xs cols
         acc df]
    (if-let [col (first xs)]
      (recur (rest xs) (.drop acc col))
      acc)))

(defn ^Dataset renames
  [^Dataset df cols]
  (loop [xs cols
         acc df]
    (if-let [col (first xs)]
      (let [[from to] col]
       (infof "RENAME: %s -> %s" from to)
       (recur (rest xs) (.withColumnRenamed acc from to)))
      acc)))

(defn drop-duplicates
  [^Dataset df & cols]
  (.dropDuplicates df (string-array cols)))

(defn ^Dataset cache
  [^Dataset df]
  (.cache df))

;; --------------------
;; *** multimethods ***
;; --------------------

;; ---
;; - SELECT
;; ---

(defn ^Dataset select
  [^Dataset df col & cols]
  (infof "select.COL_TYPE=%s" (type col))
  (condp = (type col)
    String (let [#^String xs (string-array cols)]
             (.select df col xs))
    Column (let [#^Column xs (col-array (concat [col] cols))]
             (.select df xs))
    :else (throw (invalid-column-type-exception col))))

;; ---
;; - GROUP BY
;; ---

(defn ^RelationalGroupedDataset groupby
  [^Dataset df col & cols]
  (infof "groupby.COL_TYPE=%s" (type col))
  (condp = (type col)
    String (let [#^String xs (string-array cols)]
             (.groupBy df col xs))
    Column (let [#^Column xs (col-array (concat [col] cols))]
             (.groupBy df xs))
    :else (throw (invalid-column-type-exception col))))

;; ---
;; - SORT
;; ---

(defn ^Dataset sort
  [^Dataset df col & cols]
  (infof "sort.COL_TYPE=%s" (type col))
  (condp = (type col)
    String (let [#^String xs (string-array cols)]
             (.sort df col xs))
    Column (let [#^Column xs (col-array (concat [col] cols))]
             (.sort df xs))
    :else (throw (invalid-column-type-exception col))))

(defn sort-within-partitions
  [^Dataset df col & cols]
  (infof "sorth-within-partitions.COL_TYPE=%s" (type col))
  (condp = (type col)
    String (let [#^String xs (string-array cols)]
             (.sortWithinPartitions df col xs))
    Column (let [#^Column xs (col-array (concat [col] cols))]
             (.sortWithinPartitions df xs))
    :else (throw (invalid-column-type-exception col))))


;; ===================
;; -  UDF Functions  -
;; ===================

(defn register-function
  "Registers UDF function.
  `function` should include argument type hints."
  [^SQLContext sql-context ^String name ^java.io.Serializable func ^DataType ret-type]
  (infof "Registering <%s>UDF[%s][%s]" ret-type name func)
  (-> sql-context
      (.udf)
      (.register name func ret-type)))
