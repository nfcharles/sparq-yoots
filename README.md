# sparq-yoots
[![Build Status](https://travis-ci.com/nfcharles/sparq-yoots.svg?branch=master)](https://travis-ci.com/nfcharles/sparq-yoots)
[![codecov](https://codecov.io/gh/nfcharles/sparq-yoots/branch/master/graph/badge.svg)](https://codecov.io/gh/nfcharles/sparq-yoots)
[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.nfcharles/sparq-yoots.svg)](https://clojars.org/org.clojars.nfcharles/sparq-yoots)

A simple Clojure library designed to facilitate easier integration w/ spark. Contains handy utilities, wrappers, functional patterns, etc.  This is NOT a fully featured clojure DSL.  Some features include easy schema configuration, data loading and UDF generation.


## Usage

### Schema Definition

```clojure
(ns example.schema
  (:gen-class))

;; Schema definition;  Use with dataframe loaders to set schema appropriately.
(def colspec
  (list
    {:name "foo" :type :int          }
    {:name "bar" :type :long         }
    {:name "baz" :type [:array :int] }))
```

### Spark Configuration

#### Spark Context
```clojure
(ns example.spark
  (:require [sparq-yoots.configuration.core :as sparq.conf])
  (:gen-class))


(defn run
  [spark-context ...]
  ...)


(defn -main
  [& args]
  (let [spark-context (sparq.conf/spark-context conf
                                                :app-name    (parse-app-name args)
                                                :master      (parse-master args)
                                                :spark-confs (parse-spark-confs args))]
    (run spark-context ...)))
```

#### Spark Session

```clojure

...

(defn -main
  [& args]
  (let [spark-session (sparq.conf/spark-session :app-name    (parse-app-name args)
                                                :master      (parse-master ags)
                                                :spark-confs (parse-spark-confs args)
                                                :with-hive   true)]
    (run spark-session ...)))

```


### S3 Configuration

```clojure
(ns example.s3
  (:require [sparq-yoots.configuration.s3 :as sparq.s3])
  (:import [com.amazonaws.auth DefaultAWSCredentialsProviderChain])
  (:gen-class))


(defn configure-s3
  [ctx]
  (let [creds (.getCredentials (DefaultAWSCredentialsProviderChain.))]
    (sparq.s3/configure ctx creds)))

(defn -main
  [& args]
  (let [spark-context (...)]
    (configure-s3 spark-context)
    (run ...)))
```

### Loaders

```clojure
(ns example.driver
  (:requre [sparq-yoots.core :as sparq.core]
           [example.schema :as schema]
           [example.spark :as spark.conf])
  (:gen-class))


(let [df (sparq.core/load-dataframe spark-ctx path schema/colspec)]
  (run df))
```

### UDFs

```clojure
(ns examples.functions
  (:import [sparq_yoots.functions UDF3 UDF5 UDF7])
  (:gen-class))


;; Create UDF3
(def foo (UDF3. (fn ^DoubleType [^DoubleType a ^DoubleType b ^DoubleType c] (* a b c))))
;;
```

#### Registration

```clojure
(ns examples.driver
  (:require [examples.functions :as func]
            [sparq-yoots.sql.core :as sparq.sql])
  (:gen-class))


(sparq.sql/register-function sql-ctx "foo" func/foo DataTypes/DoubleType)

...
```

### Functions

Use `gen-col` macro for creating named column functions.

#### `gen-col` macro

```clojure

(gen-col "col-1"    "col_1")
(gen-col "tmp-col"  "_temp_col")


(col-1)                           ;; "col_1"
(col-1 :field "foo")              ;; "col_1.foo"
(col-1 :alias :index 0 :as "foo") ;; "a.col_1[0] AS foo"
(col-1 :cast "int")               ;; "CAST(col_1 AS int)"
```

#### Column Readers

Convenience functions during UDF processing.

```clojure
(def FOO (partial sparq.sql/double-col 0))
(def BAR (partial sparq.sql/bool-col   1))
(def BAZ (partial sparq.sql/int-col    2))

(let [foo (FOO spark-row-obj)
      bar (BAR spark-row-obj)
      baz (BAZ spark-row-obj)]
  ...
  )
```

## License

Copyright © 2019 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
