(ns sparq-yoots.configuration.s3
  (:require [sparq-yoots.constants :as sparq.const])


(defn token?
  "Determines if session token is set"
  [creds]
  (try
    (.getSessionToken creds)
    (catch java.lang.IllegalArgumentException e)))

(defn configure-common
  "Sets access and secret keys"
  [ctx creds]
  (set-hadoop ctx (:access sparq.const/s3a) (.getAWSAccessKeyId creds))
  (set-hadoop ctx (:secret sparq.const/s3a) (.getAWSSecretKey creds)))

(defn configure-from-temp
  [ctx creds]
  "Sets ephemeral s3 credentials"
  (configure-common ctx creds)
  (set-hadoop ctx (:provider sparq.const/s3a) sparq.const/temp-aws-provider)
  (set-hadoop ctx (:token sparq.const/s3a) (.getSessionToken creds)))

(defn configure
  [ctx creds]
  "Sets s3 credentials"
  (configure-common ctx creds))
