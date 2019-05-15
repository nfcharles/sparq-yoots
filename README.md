# sparq-yoots
[![Build Status](https://travis-ci.org/nfcharles/sparq-yoots.svg?branch=master)](https://travis-ci.org/nfcharles/sparq-yoots)
[![codecov](https://codecov.io/gh/nfcharles/sparq-yoots/branch/master/graph/badge.svg)](https://codecov.io/gh/nfcharles/sparq-yoots)
[![Clojars Project](https://img.shields.io/clojars/v/sparq-yoots.svg)](https://clojars.org/sparq-yoots)

A simple Clojure library designed to facilitate easier integration w/ spark. Contains handy utilities, wrappers, functional patterns, etc.  This is NOT a fully featured clojure DSL.  Some features include easy schema configuration and data loading.

```clj
[sparq-yoots "0.1.0"]
```

## Usage

### Configuration

```clojure
(ns example.config
  (:gen-class))


(def colspec
  (list
    {:name "foo" :type :int          }
    {:name "bar" :type :long         }
    {:name "baz" :type [:array :int] }))
```

### Loaders

```clojure
(ns example.driver
  (:requre [sparq-yoots.core :as sparq.core])
           [example.config :as conf]
  (:gen-class))


(let [df (sparq.core/load-dataframe spark-ctx path conf/colspec)]
  (run df))
```


## License

Copyright © 2018 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
