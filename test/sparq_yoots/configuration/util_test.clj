(ns sparq-yoots.configuration.util-test
  (:require [clojure.test :refer :all]
            [sparq-yoots.configuration.util :refer :all]))


;; TODO: fix me: these tests fail when they should not.
(def msg #"^Spark configuration requires key/value pairs!\s.*$")

(deftest conf-test-errors
  (testing "Ensures key/value pair correct configuration"
    (let [conf nil]
      (is (thrown-with-msg? Exception msg (ensure-key-value-pairs ["foo"] identity)))
      (is (thrown-with-msg? Exception msg (ensure-key-value-pairs ["foo" "bar" "baz"] identity)))))
  (testing "Standard configuration"
    (let [conf nil]
      (is (thrown-with-msg? Exception msg (set-spark-conf conf ["foo"])))
      (is (thrown-with-msg? Exception msg (set-spark-conf conf ["foo" "bar" "baz"])))))
  (testing "Builder configuration"
    (let [builder nil]
      (is (thrown-with-msg? Exception msg (set-spark-conf-from-builder builder ["foo"])))
      (is (thrown-with-msg? Exception msg (set-spark-conf-from-builder builder ["foo" "bar" "baz"]))))))
