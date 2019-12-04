(ns sparq-yoots.functions
  (:require [taoensso.timbre :as timbre :refer [info infof debug debugf errorf]])
  (:import [sparq_yoots.functions SerializableSparkFunctionContext]
           [clojure.lang RT Var]))



;; ===
;; - Utils
;; ===

(defn gen-arg-vector
  "Generates function argument vector
  Arity assumed to be not greater than UDF limit."
  [arity & {:keys [init with-type-hint]
            :or {init []
                 with-type-hint nil}}]
  (loop [xs (take arity (range 97 123))
         acc init]
    (if-let [x (first xs)]
      (if with-type-hint
        (recur (rest xs) (conj acc (with-meta (symbol (str (char x))) {:tag with-type-hint})))
        (recur (rest xs) (conj acc (symbol (str (char x))))))
      acc)))


;; ==================
;; - IMPLEMENTATION -
;; ==================



;; -----
;; - Macro Generation
;; ---

(defmacro gen-udf
  [arity]
  (let [fn           (gensym "udf-fn")
        fn-with-type (with-meta fn {:tag clojure.lang.IFn})]
    `(do
      (gen-class
        :name ~(format "sparq_yoots.functions.UDF%d" arity)
        :extends sparq_yoots.functions.SerializableSparkFunctionContext
        :implements [~(format "org.apache.spark.sql.api.java.UDF%d" arity)]
        :prefix ~(format "udf%d-" arity)
        :constructors {[clojure.lang.IFn] [clojure.lang.IFn]})
      (defn ~(symbol (format "udf%d-call" arity))
        ~(gen-arg-vector arity :init [(symbol "this")] :with-type-hint Object)
        (let [~fn-with-type (.getFunction ~(symbol "this"))]
          (~fn ~@(gen-arg-vector arity)))))))



;; ===
;; - UDF Template Test
;; ===

(comment
(set! *print-meta* true)
(clojure.pprint/pprint (macroexpand '(gen-udf 1)))
(clojure.pprint/pprint (macroexpand '(gen-udf 4)))
(clojure.pprint/pprint (macroexpand '(gen-udf 7)))
)


;; ===
;; - UDFs
;; ===

(gen-udf  1)
(gen-udf  2)
(gen-udf  3)
(gen-udf  4)
(gen-udf  5)
(gen-udf  6)
(gen-udf  7)
(gen-udf  8)
(gen-udf  9)
(gen-udf 10)
(gen-udf 11)
(gen-udf 12)
(gen-udf 13)
(gen-udf 14)
(gen-udf 15)
(gen-udf 16)
(gen-udf 17)
(gen-udf 18)
(gen-udf 19)


#_(defn ser-deser-test
  [udf]
  (write-output udf)
  (read-input))



;; ===
;; - Main
;; ===

(defn -main
  [& args]
  (let [test-fn (fn [& args] (apply * args))]
    (info (sparq_yoots.functions.UDF1. test-fn))
    (info (sparq_yoots.functions.UDF3. test-fn))
    (info (sparq_yoots.functions.UDF9. test-fn))
    (infof "RET=%d" (.call (sparq_yoots.functions.UDF4. test-fn) 1 2 3 4))
    #_(ser-deser-test udf)))
