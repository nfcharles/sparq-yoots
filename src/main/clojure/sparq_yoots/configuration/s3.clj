(ns sparq-yoots.configuration.s3
  (:require [sparq-yoots.constants :as sparq.const]
            [taoensso.timbre :as timbre :refer [infof]]))


(defn set-hadoop-config [ctx k v]
  (infof "setting key[%s]" k)
  (.set (.hadoopConfiguration ctx) k v))

(defn token?
  "Determines if session token is set"
  [creds]
  (try
    (.getSessionToken creds)
    (catch java.lang.IllegalArgumentException e)))

(defn configure-common
  "Sets access and secret keys"
  [ctx creds]
  (set-hadoop-config ctx (:access-key sparq.const/s3a) (.getAWSAccessKeyId creds))
  (set-hadoop-config ctx (:secret-key sparq.const/s3a) (.getAWSSecretKey creds)))

(defn configure-from-temp
  [ctx creds]
  "Sets ephemeral s3 credentials"
  (configure-common ctx creds)
  (set-hadoop-config ctx (:provider sparq.const/s3a) sparq.const/temp-aws-provider)
  (set-hadoop-config ctx (:token sparq.const/s3a) (.getSessionToken creds)))

(defn configure
  [ctx creds]
  "Sets s3 credentials"
  (if (token? creds)
    (configure-from-temp ctx creds)
    (configure-common ctx creds)))
