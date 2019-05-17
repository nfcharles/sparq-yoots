# sparq-yoots
[![Build Status](https://travis-ci.org/nfcharles/sparq-yoots.svg?branch=master)](https://travis-ci.org/nfcharles/sparq-yoots)
[![codecov](https://codecov.io/gh/nfcharles/sparq-yoots/branch/master/graph/badge.svg)](https://codecov.io/gh/nfcharles/sparq-yoots)
[![Clojars Project](https://img.shields.io/clojars/v/sparq-yoots.svg)](https://clojars.org/sparq-yoots)

A simple Clojure library designed to facilitate easier integration w/ spark. Contains handy utilities, wrappers, functional patterns, etc.  This is NOT a fully featured clojure DSL.  Some features include easy schema configuration and data loading.

```clj
[sparq-yoots "0.1.0"]
```

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
  (import [com.amazonaws.auth DefaultAWSCredentialsProviderChain])
  (:gen-class))


(defn configure-s3
  [ctx]
  (let [creds (.getCredentials (DefaultAWSCredentialsProviderChain.))]
    (sparq.s3/configure ctx creds)))

(defn -main
  [& args]
  (let [spark-context (...)]
    (configure-s3 spark-context creds)
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



## License

Copyright © 2019 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
