(ns sparq-yoots.sql.types
  (:require [taoensso.timbre :as timbre :refer [infof debugf]])
  (:import  [org.apache.spark.sql.types StructType
                                        StructField
                                        DataTypes
                                        DataType]))



;; ---
;; - Util
;; ---

(defn invalid-type
  (^String [_type]
    (format "Typespec parse error. '%s' unexpected." _type))
  (^String [_type msg]
    (format "Typespec parse error. '%s' unexpected. %s." _type msg)))

(defn nullable?
  [nullable]
  "Defaults to true if nullable is not specified in configuration"
  (or (nil? nullable) nullable))


;; ---
;; - Type defs
;; ---

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


;; ---
;; - Type parsers
;; ---

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
