(ns sparq-yoots.configuration.util
  (:require [taoensso.timbre :as timbre :refer [infof info]]))


(defn set-hadoop [ctx k v]
  (infof "setting key[%s]" k)
  (.set (.hadoopConfiguration ctx) k v))

(defn ensure-key-value-pairs
  "Ensures configuration list is even number of entries"
  [confs configure]
  (if (= 1 (mod (count confs) 2))
    (throw (java.lang.Exception. "Spark configuration requries key/value pairs! %s" confs))
    (do
      (info "Configuring spark...")
      (configure))))

(defn set-spark-conf
  "Set spark configuration"
  [^org.apache.spark.SparkConf conf spark-confs]
  (ensure-key-value-pairs spark-confs
    (fn []
      (doseq [[k v] (partition 2 spark-confs)]
        (infof "==> SET %s=%s" k v)
        (.set conf (str k) (str v)))
      conf)))

(defn set-spark-conf-from-builder
  [^org.apache.spark.sql.SparkSession$Builder builder spark-confs]
  (ensure-key-value-pairs spark-confs
    (fn []
      (loop [xs (partition 2 spark-confs)
             acc builder]
        (if-let [x (first xs)]
          (let [[k v] x]
            (infof "==> SET %s=%s" k v)
          (recur (rest xs) (.config acc k v)))
        acc)))))
