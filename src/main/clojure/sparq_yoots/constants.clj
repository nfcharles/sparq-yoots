(ns sparq-yoots.constants)


;; ===================
;; -  configuration  -
;; ===================

;; ---
;; S3
;; ---

(def s3a
  (hash-map
    :access-key "fs.s3a.access.key"
    :secret-key "fs.s3a.secret.key"
    :provider   "fs.s3a.aws.credentials.provider"
    :token      "fs.s3a.session.token"))

(def temp-aws-provider "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")


;; ---
;; Hive
;; ---

(def hive
  (hash-map
    :dyn-partition      "hive.exec.dynamic.partition"
    :dyn-partition-mode "hive.exec.dynamic.partition.mode"))


;; ---
;; I/O
;; ---

(def read
  (hash-map
    :default "parquet"))

(def write
  (hash-map
    :default "parquet"))
