(ns sparq-yoots.configuration.core
  (:require [sparq-yoots.configuration.util :as conf.util]
            [sparq-yoots.constants :as sparq.consts]
            [taoensso.timbre :as timbre :refer [info warn error fatal infof warnf errorf debugf]])
  (:import [org.apache.spark.api.java JavaSparkContext]
           [org.apache.spark.sql SparkSession]))



(defn ^JavaSparkContext spark-context
  "Initializes spark context"
  [conf & {:keys [app-name master spark-confs]
           :or {app-name    "app.driver"
                master      "local[*]"
                spark-confs []}}]
  (infof "SPARK_MASTER=%s" master)
  (infof "SPARK_APP_NAME=%s" app-name)
  (-> (conf.util/set-spark-conf conf spark-confs)
      (.setAppName app-name)
      (.setMaster master)
      (JavaSparkContext.)))

(defn ^SparkSession spark-session
  "Initializes spark session"
  [& {:keys [app-name master spark-confs with-hive]
      :or {app-name    "app.driver"
           master      "local[*]"
           spark-confs []
           with-hive   false}}]
  (infof "SPARK_MASTER=%s" master)
  (infof "SPARK_APP_NAME=%s" app-name)
  (let [builder (-> (SparkSession/builder)
                    (.appName app-name)
                    (.master master)
                    (conf.util/set-spark-conf-from-builder spark-confs))]
    (if with-hive
      (-> builder
          (.enableHiveSupport)
          (.config (:dyn-partition-mode sparq.consts/hive) "nonstrict")
          (.config (:dyn-partition sparq.consts/hive)      "true")
          (.getOrCreate))
      (.getOrCreate builder))))
