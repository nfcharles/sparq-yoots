(ns sparq-yoots.functions-test
  (:require [clojure.test :refer :all]
            [sparq-yoots.functions :refer :all])
  (:import [sparq_yoots.functions  UDF1  UDF2  UDF3  UDF4  UDF5
                                   UDF6  UDF7  UDF8  UDF9  UDF10
                                   UDF11 UDF12 UDF13 UDF14 UDF15
                                   UDF16 UDF17 UDF18 UDF19]))


;; ---
;; - Test Source Function
;; ---

(defn udf-source
  [& args]
  (apply * args))

;; ---
;; - Argument Builder
;; ---

(defn build-args
  [arity]
  (range 1 (inc arity)))

(defmacro call-udf
  [arity]
  (let [udf (symbol (format "UDF%d." arity))
        op  (symbol "%")]
    `(.call (~udf udf-source) ~@(build-args arity))))


;; ---
;; - Macro test
;; ---
(comment
(clojure.pprint/pprint (macroexpand '(call-udf 1)))
(clojure.pprint/pprint (macroexpand '(call-udf 5)))
(clojure.pprint/pprint (macroexpand '(call-udf 7)))
)



;; ===
;; - TEST!!
;; ===

;; TODO: Build macro for generation test suite for UDF1..19

(deftest udf-test
  (testing "Testing UDF1"
    (is (= (call-udf 1) 1)))
  (testing "Testing UDF2"
    (is (= (call-udf 2) 2)))
  (testing "Testing UDF3"
    (is (= (call-udf 3) 6)))
  (testing "Testing UDF5"
    (is (= (call-udf 5) 120)))
  (testing "Testing UDF8"
    (is (= (call-udf 8) 40320)))
  (testing "Testing UDF13"
    (is (= (call-udf 13) 6227020800))))
