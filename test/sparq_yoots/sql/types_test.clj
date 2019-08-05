(ns sparq-yoots.sql.types-test
  (:require [clojure.test :refer :all]
            [sparq-yoots.sql.types :refer :all])
  (:import [org.apache.spark.sql.types DataTypes]))


(deftest datatype-test
  (testing "Simple types"
    (is (= (:string    types) DataTypes/StringType))
    (is (= (:short     types) DataTypes/ShortType))
    (is (= (:int       types) DataTypes/IntegerType))
    (is (= (:long      types) DataTypes/LongType))
    (is (= (:float     types) DataTypes/FloatType))
    (is (= (:double    types) DataTypes/DoubleType))
    (is (= (:bool      types) DataTypes/BooleanType))
    (is (= (:byte      types) DataTypes/ByteType))
    (is (= (:date      types) DataTypes/DateType))
    (is (= (:timestamp types) DataTypes/TimestampType))
    (is (= (:bin       types) DataTypes/BinaryType))
    (is (= (:cal       types) DataTypes/CalendarIntervalType))
    (is (= (:null      types) DataTypes/NullType)))
  (testing "Invalid types"
    (is (= (:foo types) nil))
    (is (= (:bar types) nil))))

(deftest parse-type-test
  (testing "Complex types"
    (is (= (.json (parse-array-type [:array :int] parse-type))
           "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}"))
    (is (= (.json (parse-array-type [:array :int false] parse-type))
           "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":false}"))
    (is (= (.json (parse-array-type [:array [:array :int]] parse-type))
           "{\"type\":\"array\",\"elementType\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"containsNull\":true}"))
    (is (= (.json (parse-map-type [:map :string :int] parse-type))
           "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}"))
    (is (= (.json (parse-map-type [:map :string :int false] parse-type))
           "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":false}"))
    (is (= (.json (parse-map-type [:map :string [:array :int] false] parse-type))
           "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"valueContainsNull\":false}"))
    (is (= (.json (parse-decimal-type [:decimal]))
           "\"decimal(10,0)\""))
    (is (= (.json (parse-decimal-type [:decimal 10 2]))
           "\"decimal(10,2)\""))))

(defn -parse-struct-field
  [colspec]
  (let [{name :name typespec :typespec null :null, :or {null true}} colspec]
    (parse-struct-field name (parse-type typespec) null)))

(deftest structs-test
  (let [colspec1 {:name "foo" :typespec :int :null false}
        colspec2 {:name "bar" :typespec :string}
        colspec3 {:name "baz" :typespec [:array :string]}
        colspec4 {:name "oof" :typespec [:map :string :int]}
        colspec5 {:name "rab" :typespec [:array [:array :string] false]}
        colspec6 {:name "zab" :typespec [:map :string [:array :string]]}]
    (testing "Simple"
      (is (= (.toString (-parse-struct-field colspec1))
             "StructField(foo,IntegerType,false)"))
      (is (= (.toString (-parse-struct-field colspec2))
             "StructField(bar,StringType,true)"))
      (is (= (.toString (-parse-struct-field colspec3))
             "StructField(baz,ArrayType(StringType,true),true)"))
      (is (= (.toString (-parse-struct-field colspec4))
             "StructField(oof,MapType(StringType,IntegerType,true),true)"))
      (is (= (.toString (-parse-struct-field colspec5))
             "StructField(rab,ArrayType(ArrayType(StringType,true),false),true)"))
      (is (= (.toString (-parse-struct-field colspec6))
             "StructField(zab,MapType(StringType,ArrayType(StringType,true),true),true)")))
    (testing "Complex"
      (let [colspecs (list colspec1 colspec2 colspec3 colspec4 colspec5 colspec6)]
        (is (= (.toString (parse-colspecs colspecs))
               "StructType(StructField(foo,IntegerType,false), StructField(bar,StringType,true), StructField(baz,ArrayType(StringType,true),true), StructField(oof,MapType(StringType,IntegerType,true),true), StructField(rab,ArrayType(ArrayType(StringType,true),false),true), StructField(zab,MapType(StringType,ArrayType(StringType,true),true),true))"))))))


;; TODO: test errors
