(ns sparq-yoots.core
  "Main utility functions"
  (:require [sparq-yoots.constants :as sparq.const]
            [sparq-yoots.sql.types :as sparq.sql.types]
            [taoensso.timbre :as timbre :refer [infof debugf]])
  (:import [org.apache.spark.api.java JavaRDD]
           [org.apache.spark.sql.types StructType]
           [org.apache.spark.sql SQLContext]))



;; =============
;; -  Loaders  -
;; =============

;; ---
;; - With schema
;; ---

(defn load-dataframe-from-schema
  "Loads dataframe with defined schema."
  [^SQLContext sql-ctx ^String path ^StructType schema ^String fmt]
  (infof "DATAFRAME_SOURCE=%s" path)
  (-> sql-ctx
      (.read)
      (.schema schema)
      (.format fmt)
      (.load path)))


;; ---
;; - Without schema
;; ---

(defn load-dataframe-ns
  "Loads dataframe with defined schema."
  [^SQLContext sql-ctx ^String path & {:keys [^String fmt]
                                       :or {^String fmt (:default sparq.const/read)}}]
  (infof "DATAFRAME_SOURCE=%s" path)
  (-> sql-ctx
      (.read)
      (.format fmt)
      (.load path)))

(defn load-dataframe
  "Loads dataframe from specification."
  [sql-ctx path colspecs & {:keys [fmt]
                            :or {fmt (:default sparq.const/read)}}]
  (if colspecs
    (load-dataframe-from-schema sql-ctx path (sparq.sql.types/parse-colspecs colspecs) fmt)
    (load-dataframe-ns sql-ctx path :fmt fmt)))


;; ---
;; - Variable path load - optimizes partition scan
;; ---

;; TODO: Consolidate this w/ above dataframe methods and unify interface, next version

(defn optional-conf
  [obj action predicate]
  (if predicate
    (action obj) obj))

(defn variable-conf
  [obj a1 a2 predicate]
  (if predicate
    (a1 obj) (a2 obj)))

(defn load-dataframe-sources
  "Loads dataframe with defined schema."
  [^SQLContext sql-ctx ^String path & {:keys [^StructType schema ^String fmt paths]
                                       :or {^StructType schema nil
                                            ^String fmt (:default sparq.const/read)
                                            paths nil}}]
  (infof "DATAFRAME_SOURCE=%s" path)
  (if paths
    (infof "PARTITIONS=%s" paths))
  (-> sql-ctx
      (.read)
      (optional-conf #(.schema %1 schema) schema)
      (.format fmt)
      (optional-conf #(.option %1 "basePath" path) paths)
      (variable-conf #(.load %1 (into-array String paths)) #(.load %1 path) paths)))


;; ---
;; - Row RDD
;; ---
(defn load-row-rdd
  "Loads row rdd"
  [^SQLContext sql-ctx ^String path & {:keys [^String fmt]
                                       :or {^String fmt (:default sparq.const/read)}}]
  (-> sql-ctx
      (.read)
      (.format fmt)
      (.load path)))

;; ---
;; - RDD to Dataframe
;; ---

(defn rdd->dataframe
  [^SQLContext sql-ctx ^JavaRDD rdd ^StructType struct-type]
  (let [r (JavaRDD/toRDD rdd)]
    (infof "Converted %s to DataFrame" r)
    (.createDataFrame sql-ctx r struct-type)))


;; ===
;; - Writer Utils
;; ===

(defn write
  "Persists dataframe in parquet format."
  [df output & {:keys [save-mode with-partitions]
                :or {save-mode nil
                     with-partitions nil}}]
  (-> df
      (.write)
      (optional-conf #(.mode %1 save-mode) save-mode)
      (optional-conf #(.partitionBy %1 (into-array String with-partitions)) with-partitions)
      (.parquet output)))
