(ns sparq-yoots.sql.types
  (:require [taoensso.timbre :as timbre :refer [infof debugf]])
  (:import [org.apache.spark.sql.types DecimalType
                                       ArrayType
                                       MapType
                                       StructType
                                       StructField
                                       DataTypes]))



;; ============
;; -  Errors  -
;; ============

(defn notype
  [_type]
  (format "Unsupported type: %s" _type))

(defn unsupported-type-error
  [_type]
  (throw (java.lang.Exception. (notype _type))))

(defn parse-error
  [& msgs]
  (let [xs (cons "Parse Error" msgs)]
    (throw (java.lang.Exception. (clojure.string/join ".  " xs)))))


;; ======================
;; -  Type Definitions  -
;; ======================

;; ---
;; - Simple Types
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
;; - Complex Types
;; ---

(defn nullable?
  [nullable]
  "Defaults to true if nullable is not specified in configuration"
  (or (nil? nullable) nullable))

(defn parse-decimal-type
  [spec]
  (let [[_ prec scale] spec]
    (if (not-any? nil? [prec scale])
      (DataTypes/createDecimalType prec scale)
      (DataTypes/createDecimalType))))

(defn parse-array-type
  [spec parser]
  #_(debugf "<parse-array>[%s,%s]" spec parser)
  (let [[_ v null] spec]
    (DataTypes/createArrayType (parser v) (nullable? null))))

(defn parse-map-type
  [spec parser]
  (let [[_ k v null] spec
        t (k types)]
    (if t
      (DataTypes/createMapType t (parser v) (nullable? null))
      (parse-error (format "Map key[%s] must be one of: %s" t (keys types))))))

(defn parse-struct-field
  [name _type null]
  (DataTypes/createStructField name _type (nullable? null)))

(defn parse-struct-type
  [colspecs parser]
  (debugf "COLSPECS.COUNT=%d" (count colspecs))
  (loop [xs colspecs
         acc []]
    (if-let [colspec (first xs)]
      (let [{:keys [name typespec null]} colspec]
        (debugf "NAME=%s, TYPESPEC=%s, NULL=%s" name typespec null)
        (recur (rest xs) (conj acc (parse-struct-field name (parser typespec) null))))
      (DataTypes/createStructType (into-array StructField acc)))))


;; ---
;; - General Type Parser
;; ---

(defn parse-type
  [typespec]
  (cond
    ;; --- Parse simple type ---
    (keyword? typespec)
      (if-let [_type (typespec types)]
        _type
        (unsupported-type-error typespec))

    ;; --- Parse complex type ---
    (vector? typespec)
      (let [[_type _] typespec]
        (condp = _type
          :decimal (parse-decimal-type typespec)
          :array   (parse-array-type typespec parse-type)
          :map     (parse-map-type typespec parse-type)
          (parse-error (format "Unsupported complex type:%s" _type) "Must be :decimal, :array or :map")))

    ;; --- Parse struct type ---
    (list? typespec)
      ;; In this case, typespec is colspecs [{:name _ :typespec _ :null _} ...]
      (parse-struct-type typespec parse-type)
    :else (parse-error "Unexpected form")))


;; ---
;; - Top Level Parser
;; ---

(defn parse-colspecs
  [colspecs]
  (parse-struct-type colspecs parse-type))
