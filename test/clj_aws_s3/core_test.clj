(ns clj-aws-s3.core-test
  (:require [clojure.test :refer :all]
            [clj-aws-s3.core :as core])
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.auth.BasicSessionCredentials
           com.amazonaws.services.s3.AmazonS3Client
           com.amazonaws.AmazonServiceException
           com.amazonaws.ClientConfiguration
           com.amazonaws.services.s3.model.Bucket))

(def credentials
  {:conn-timeout 5000
   :socket-timeout 4000
   :max-retries 10
   :access-key "ACCESS KEY"
   :secret-key "SECRET KEY"
   :token "TOKEN"})

(deftest client-config-builder-test
  (let [client-config (core/client-config-builder credentials)]
    (is (instance? ClientConfiguration client-config))

    (is (= (:conn-timeout credentials)
           (.getConnectionTimeout client-config)))
    (is (= (:socket-timeout credentials)
           (.getSocketTimeout client-config)))
    (is (= (:max-retries credentials)
           (.getMaxErrorRetry client-config)))))

(deftest aws-credentials-builder-test
  (let [result (core/aws-credentials-builder credentials)]
    (is (instance? BasicSessionCredentials result))
    (is (= (:secret-key credentials)
           (.getAWSSecretKey result)))
    (is (= (:token credentials)
           (.getSessionToken result)))))

(deftest client-test
  (let [client (core/client credentials)]
    (is client)
    (is (instance? AmazonS3Client client))))

(deftest bucket-to-map-test
  (let [bucket-name "bucket name"
        bucket (Bucket. bucket-name)
        bucket-map (core/as-map bucket)]
    (is (:name bucket-map))))
