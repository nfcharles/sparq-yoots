(ns sparq-yoots.constants-test
  (:require [clojure.test :refer :all]
            [sparq-yoots.constants :refer :all]))


(deftest s3-keys-test
  (testing "Test s3a keys"
    (is (= (:access-key s3a) "fs.s3a.access.key"))
    (is (= (:secret-key s3a) "fs.s3a.secret.key"))
    (is (= (:provider s3a)   "fs.s3a.aws.credentials.provider"))
    (is (= (:token s3a)      "fs.s3a.session.token"))))


(deftest tmp-provider-test
  (testing "Test temp aws provider"
    (is (= temp-aws-provider "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"))))

(deftest hive-keys-test
  (testing "Test hive keys"
    (is (= (:dyn-partition hive) "hive.exec.dynamic.partition"))
    (is (= (:dyn-partition-mode hive) "hive.exec.dynamic.partition.mode"))))


(deftest io-test
  (testing "Test I/O"
    (is (= (:default read) "parquet"))
    (is (= (:default write) "parquet"))))
