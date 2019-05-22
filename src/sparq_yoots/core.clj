(ns sparq-yoots.core
  "Main utility functions"
  (:require [sparq-yoots.constants :as sparq.const]
            [taoensso.timbre :as timbre :refer [infof debugf]])
  (:import [org.apache.spark.sql Column Dataset RelationalGroupedDataset]
           [org.apache.spark.sql functions]
           [org.apache.spark.sql SQLContext RowFactory Row]
           [org.apache.spark.sql.types StructType
                                       StructField
                                       DataTypes
                                       DataType])
  (:gen-class))



;;;; TODO: We can probably refactor this into smaller modules;  exercise for later.


(defn invalid-type
  (^String [_type]
    (format "Typespec parse error. '%s' unexpected." _type))
  (^String [_type msg]
    (format "Typespec parse error. '%s' unexpected. %s." _type msg)))


;; ===========
;; -  Types  -
;; ===========

(def types
  (hash-map
    :bin       DataTypes/BinaryType
    :bool      DataTypes/BooleanType
    :byte      DataTypes/ByteType
    :cal       DataTypes/CalendarIntervalType
    :date      DataTypes/DateType
    :double    DataTypes/DoubleType
    :float     DataTypes/FloatType
    :int       DataTypes/IntegerType
    :long      DataTypes/LongType
    :null      DataTypes/NullType
    :short     DataTypes/ShortType
    :string    DataTypes/StringType
    :timestamp DataTypes/TimestampType))


;; ===================
;; - Config Parsers  -
;; ===================

(defn nullable?
  [nullable]
  "Defaults to true if nullable is not specified in configuration"
  (or (nil? nullable) nullable))

(defn ^DataType parse-complex
  [spec]
  (if (vector? spec)
    (let [[_type _] spec]
      (condp = _type
        :array
          (let [[_ v null] spec]
            (DataTypes/createArrayType (parse-complex v) (nullable? null)))
        :map
          (let [[_ k v null] spec]
            (if-let [datatype (k types)]
              (DataTypes/createMapType (k types) (parse-complex v) (nullable? null))
              (throw (java.lang.Exception. (invalid-type k "Unsupported type")))))
        :decimal
         (let [[_ prec scale] spec]
            (if (not-any? nil? [prec scale])
              (DataTypes/createDecimalType prec scale)
              (DataTypes/createDecimalType)))
        (throw (java.lang.Exception. (invalid-type _type "Unsupported complex type")))))
    (if-let [v (spec types)]
      v
      (throw (java.lang.Exception. (invalid-type spec "Unsupported type"))))))

(defn ^StructField parse
  "Parse column type specification"
  [colspec]
  (let [{:keys [name typespec null]} colspec]
    (cond
      (vector? typespec)
        (DataTypes/createStructField name (parse-complex typespec) (nullable? null))
      (keyword? typespec)
        (if-let [datatype (typespec types)]
          (DataTypes/createStructField name datatype (nullable? null))
          (throw (java.lang.Exception. (invalid-type typespec "Unsupported type"))))
      :else (throw (java.lang.Exception. (invalid-type typespec "keyword or vector expected"))))))

(defn ^StructType struct-type
  "Builds StructType from columns names and types."
  [colspecs]
  (let [xform (fn [xs]
                (into-array StructField xs))]
    (loop [xs colspecs
           acc []]
      (if-let [cs (first xs)]
        (let [{:keys [name typespec _]} cs]
          (infof "Adding <%s>%s to struct" typespec name)
          (recur (rest xs) (conj acc (parse cs))))
        (DataTypes/createStructType (xform acc))))))


;; =============
;; -  Loaders  -
;; =============

(defn load-dataframe-from-schema
  "Loads dataframe with defined schema."
  [^SQLContext sql-ctx ^String path ^StructType schema ^String fmt]
  (println (format "DATAFRAME_SOURCE=%s" path))
  (-> sql-ctx
      (.read)
      (.schema schema)
      (.format fmt)
      (.load path)))

(defn load-dataframe
  "Loads dataframe from specification."
  [sql-ctx path specs & {:keys [fmt]
                         :or {fmt (:default sparq.const/read)}}]
  (load-dataframe-from-schema sql-ctx path (struct-type specs) fmt))

(defn load-row-rdd
  "Loads row rdd"
  [^SQLContext sql-ctx ^String path & {:keys [^String fmt]
                                       :or {^String fmt (:default sparq.const/read)}}]
  (-> sql-ctx
      (.read)
      (.format fmt)
      (.load path)))


;; ==============================
;; -  Statements & Expressions  -
;; ==============================


(defn col-array
  [xs]
  (into-array Column xs))

(defn ^Column col
  [^String name]
  (Column. name))

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

(comment
;; TODO: UDF generation inside function registration; macro errors, sort out later
(defn register-function
  "Registers UDF function.
  `function` should include argument type hints."
  [^SQLContext sql-context ^String name function arity ^DataType datatype]
  (let [udf (gen-udf function arity datatype)]
    (infof "Registered UDF[%s] -> (%s,%s)" name function udf)
    (-> sql-context
        (.udf)
        (.register name udf datatype))))
)

(defn register-function
  "Registers UDF function.
  `function` should include argument type hints."
  [^SQLContext sql-context ^String name function ^DataType datatype]
  (infof "TYPE=%s" function)
  (-> sql-context
      (.udf)
      (.register name function datatype)))
