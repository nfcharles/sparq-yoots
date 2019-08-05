(ns sparq-yoots.core
  "Main utility functions"
  (:require [sparq-yoots.constants :as sparq.const]
            [sparq-yoots.sql.types :as sparq.sql.types]
            [taoensso.timbre :as timbre :refer [infof debugf]])
  (:import [org.apache.spark.sql.types StructType]
           [org.apache.spark.sql SQLContext]))



;; =============
;; -  Loaders  -
;; =============

(defn load-dataframe-from-schema
  "Loads dataframe with defined schema."
  [^SQLContext sql-ctx ^String path ^StructType schema ^String fmt]
  (infof "DATAFRAME_SOURCE=%s" path)
  (-> sql-ctx
      (.read)
      (.schema schema)
      (.format fmt)
      (.load path)))

(defn load-dataframe
  "Loads dataframe from specification."
  [sql-ctx path colspecs & {:keys [fmt]
                            :or {fmt (:default sparq.const/read)}}]
  (load-dataframe-from-schema sql-ctx path (sparq.sql.types/parse-colspecs colspecs) fmt))

(defn load-row-rdd
  "Loads row rdd"
  [^SQLContext sql-ctx ^String path & {:keys [^String fmt]
                                       :or {^String fmt (:default sparq.const/read)}}]
  (-> sql-ctx
      (.read)
      (.format fmt)
      (.load path)))
