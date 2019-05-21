(ns sparq-yoots.core-test
  (:require [clojure.test :refer :all]
            [sparq-yoots.core :refer :all])
  (:import [org.apache.spark.sql.types DataTypes]))



(deftest data-type-test
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
    (is (= (:bar types) nil)))
  (testing "Complex types" ;; TODO: more tests
    (is (= (.json (parse-complex [:array :int]))
           "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}"))
    (is (= (.json (parse-complex [:array :int false]))
           "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":false}"))
    (is (= (.json (parse-complex [:array [:array :int]]))
           "{\"type\":\"array\",\"elementType\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"containsNull\":true}"))
    (is (= (.json (parse-complex [:map :string :int]))
           "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}"))
    (is (= (.json (parse-complex [:map :string :int false]))
           "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":false}"))
    (is (= (.json (parse-complex [:map :string [:array :int] false]))
           "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"valueContainsNull\":false}"))
    (is (= (.json (parse-complex [:decimal]))
           "\"decimal(10,0)\""))
    (is (= (.json (parse-complex [:decimal 10 2]))
           "\"decimal(10,2)\""))))


(deftest stucts-test
  (let [colspec1 {:name "foo" :typespec :int :null false}
        colspec2 {:name "bar" :typespec :string}
        colspec3 {:name "baz" :typespec [:array :string]}
        colspec4 {:name "oof" :typespec [:map :string :int]}
        colspec5 {:name "rab" :typespec [:array [:array :string] false]}
        colspec6 {:name "zab" :typespec [:map :string [:array :string]]}]
    (testing "Struct fields"
      (is (= (.toString (parse colspec1))
             "StructField(foo,IntegerType,false)"))
      (is (= (.toString (parse colspec2))
             "StructField(bar,StringType,true)"))
      (is (= (.toString (parse colspec3))
             "StructField(baz,ArrayType(StringType,true),true)"))
      (is (= (.toString (parse colspec4))
             "StructField(oof,MapType(StringType,IntegerType,true),true)"))
      (is (= (.toString (parse colspec5))
             "StructField(rab,ArrayType(ArrayType(StringType,true),false),true)"))
      (is (= (.toString (parse colspec6))
             "StructField(zab,MapType(StringType,ArrayType(StringType,true),true),true)")))
    (testing "Struct type"
      (let [colspecs [colspec1 colspec2 colspec3 colspec4 colspec5 colspec6]]
        (is (= (.toString (struct-type colspecs))
               "StructType(StructField(foo,IntegerType,false), StructField(bar,StringType,true), StructField(baz,ArrayType(StringType,true),true), StructField(oof,MapType(StringType,IntegerType,true),true), StructField(rab,ArrayType(ArrayType(StringType,true),false),true), StructField(zab,MapType(StringType,ArrayType(StringType,true),true),true))"))))))


(deftest error-msg-test
  (testing "Invalid type message"
    (is (= (invalid-type :string) "Typespec parse error. ':string' unexpected."))
    (is (= (invalid-type [:array :string]) "Typespec parse error. '[:array :string]' unexpected."))
    (is (= (invalid-type :string "foo") "Typespec parse error. ':string' unexpected. foo."))
    (is (= (invalid-type [:array :string] "bar") "Typespec parse error. '[:array :string]' unexpected. bar."))))



(defn build-msg
  [s]
  (re-pattern (format "Typespec parse error. '%s' unexpected. Unsupported type." s)))

(defn build-msg2
  [s]
  (re-pattern (format "Typespec parse error. '%s' unexpected. Unsupported complex type." s)))

(deftest errors
  (testing "Bad parse"
    (is (thrown? Exception (parse {:name "foo" :typespec :footype :null false})))
    (is (thrown-with-msg? Exception (build-msg :foo)     (parse {:name "foo" :typespec :foo :null false})))
    (is (thrown-with-msg? Exception (build-msg :bar)     (parse {:name "foo" :typespec :bar :null false})))
    (is (thrown-with-msg? Exception (build-msg :foo)     (parse {:name "foo" :typespec [:array :foo] :null false})))
    (is (thrown-with-msg? Exception (build-msg :foobar)  (parse {:name "foo" :typespec [:array [:array :foobar]] :null false})))
    (is (thrown-with-msg? Exception (build-msg :foo)     (parse {:name "foo" :typespec [:map :foo :string] :null false})))
    (is (thrown-with-msg? Exception (build-msg2 :foo)    (parse {:name "foo" :typespec [:foo :int :string] :null false})))
    (is (thrown-with-msg? Exception (build-msg2 :bar)    (parse {:name "foo" :typespec [:bar :string :string] :null false})))
    (is (thrown-with-msg? Exception (build-msg :bar)     (parse {:name "foo" :typespec [:map :string :bar] :null false})))
    (is (thrown-with-msg? Exception (build-msg :badtype) (parse {:name "foo" :typespec [:map :string [:array :badtype]] :null false})))))
