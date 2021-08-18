(ns sparq-yoots.sql.col-test
  (:require [clojure.test :refer :all]
            [sparq-yoots.sql.col :refer :all]))


; ==============
;; -  Fixtures  -
;; --------------


(gen-col "col-1" "col_1")
(gen-col "col-2" "col_2")

;; ===========
;; -  Tests  -
;; -----------

(deftest alias-test
  (testing "Add Alias"
    (is (= (add-alias "foo" "f") "f.foo"))
    (is (= (add-alias "foo" nil) "foo"))))

(deftest prefix-test
  (testing "Add Prefix"
    (is (= (add-prefix "bar" "foo") "foo_bar"))
    (is (= (add-prefix "bar" nil)   "bar"))))

(deftest function-test
  (testing "Add Function"
    (is (= (add-function "bar" ["foo_function"]) "foo_function(bar)"))
    (is (= (add-function "bar" ["baz_function" "foo"]) "baz_function(bar, foo)"))))

(deftest gencol-test
  (testing "Gen Col - Simple"
    (is (= (col-1) "col_1"))
    (is (= (col-2) "col_2")))

  (testing "Gen Col - Alias"
    (is (= (col-1 :alias "f") "f.col_1"))
    (is (= (col-2 :alias "b") "b.col_2")))

  (testing "Gen Col - Prefix"
    (is (= (col-1 :prefix "foo") "foo_col_1"))
    (is (= (col-2 :prefix "bar") "bar_col_2")))

  (testing "Gen Col - Empty Prefix"
    (is (= (col-1 :prefix "") "_col_1"))
    (is (= (col-2 :prefix "") "_col_2")))

  (testing "Gen Col - Function"
    (is (= (col-1 :func ["foo"]) "foo(col_1)"))
    (is (= (col-2 :func ["bar"]) "bar(col_2)")))

  (testing "Gen Col - Cast"
    (is (= (col-1 :cast "int")    "CAST(col_1 AS int)"))
    (is (= (col-1 :cast "date")   "CAST(col_1 AS date)"))
    (is (= (col-1 :cast "double") "CAST(col_1 AS double)")))

  (testing "Gen Col - Cast Alias Prefix"
    (is (= (col-1 :cast "int"    :alias "foo" :prefix "bar")  "CAST(foo.bar_col_1 AS int)"))
    (is (= (col-1 :cast "date"   :alias "bar" :prefix "foo")  "CAST(bar.foo_col_1 AS date)"))
    (is (= (col-1 :cast "double" :alias "meh" :prefix "")     "CAST(meh._col_1 AS double)")))

  (testing "Gen Col - Alias Prefix"
    (is (= (col-1 :alias "f" :prefix "foo") "f.foo_col_1"))
    (is (= (col-2 :alias "b" :prefix "bar") "b.bar_col_2")))

  (testing "Gen Col - Alias Empty Prefix"
    (is (= (col-1 :alias "f" :prefix "") "f._col_1"))
    (is (= (col-2 :alias "b" :prefix "") "b._col_2")))

  (testing "Gen Col - Alias Prefix Function"
    (is (= (col-1 :alias "f"   :prefix "foo" :func ["ruhroh"])     "ruhroh(f.foo_col_1)"))
    (is (= (col-2 :alias "b"   :prefix "bar" :func ["baz"])        "baz(b.bar_col_2)"))
    (is (= (col-1 :alias "bar" :prefix "foo" :func ["ruhroh" 1])   "ruhroh(bar.foo_col_1, 1)"))
    (is (= (col-2 :alias "foo" :prefix "bar" :func ["baz" 1 1.23]) "baz(foo.bar_col_2, 1, 1.23)")))

  (testing "Gen Col - Alias Function"
    (is (= (col-1 :alias "f" :func ["ruhroh"]) "ruhroh(f.col_1)"))
    (is (= (col-2 :alias "b" :func ["baz"])    "baz(b.col_2)")))

  (testing "Gen Col - Alias Suffix"
    (is (= (col-1 :alias "foo" :suffix "ruhroh") "foo.col_1_ruhroh")))

  (testing "Gen Col - All Options"
    (is (= (col-1 :alias "meh" :prefix "foo" :suffix "bar" :field "lol" :func ["helloworld"]) "helloworld(meh.foo_col_1_bar.lol)"))))
