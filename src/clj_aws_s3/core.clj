(ns clj-aws-s3.core
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.auth.BasicSessionCredentials
           com.amazonaws.services.s3.AmazonS3Client
           com.amazonaws.AmazonServiceException
           com.amazonaws.ClientConfiguration
           com.amazonaws.services.s3.model.Bucket
           com.amazonaws.services.s3.model.Owner))

;;
;; CREDENTIALS
;;

(defn client-config-builder
  [credentials]
  (let [client-config (ClientConfiguration.)]
    (when-let [conn-timeout (:conn-timeout credentials)]
      (.setConnectionTimeout client-config conn-timeout))
    (when-let [socket-timeout (:socket-timeout credentials)]
      (.setSocketTimeout client-config socket-timeout))
    (when-let [max-retries (:max-retries credentials)]
      (.setMaxErrorRetry client-config max-retries))
    (when-let [max-conns (:max-conns credentials)]
      (.setMaxConnections client-config max-conns))
    (when-let [proxy-host (:proxy-host credentials)]
      (.setProxyHost client-config proxy-host))
    (when-let [proxy-port (:proxy-port credentials)]
      (.setProxyPort client-config proxy-port))
    (when-let [proxy-user (:proxy-user credentials)]
      (.setProxyUsername client-config proxy-user))
    (when-let [proxy-pass (:proxy-pass credentials)]
      (.setProxyPassword client-config proxy-pass))
    (when-let [proxy-domain (:proxy-domain credentials)]
      (.setProxyDomain client-config proxy-domain))
    (when-let [proxy-workstation (:proxy-workstation credentials)]
      (.setProxyWorkStation client-config proxy-workstation))

    client-config))

(defn aws-credentials-builder
  [credentials]
  (if (:token credentials)
    (BasicSessionCredentials. (:access-key credentials)
                              (:secret-key credentials)
                              (:token credentials))
    (BasicAWSCredentials. (:access-key credentials)
                          (:secret-key credentials))))

(defn client
  "Should return a valid S3 client given credentials hash"
  [credentials]
  (let [client-config (client-config-builder credentials)
        aws-credentials (aws-credentials-builder credentials)
        client (AmazonS3Client. aws-credentials client-config)]
    (when-let [endpoint (:endpoint credentials)]
      (.setEndpoint client endpoint))
    client))

;;
;; BUCKETS
;;

(defprotocol Mappable
  (as-map [value] "Convert value to map"))

(extend-protocol Mappable
  Bucket
  (as-map [bucket]
    {:name (.getName bucket)
     :creation-date (.getCreationDate bucket)
     :owner (as-map (.getOwner bucket))})
  Owner
  (as-map [owner]
    {:name (.getDisplayName owner)})
  nil
  (as-map [value] nil))

(defn create-bucket
  "Create new bucket given bucket-name"
  [credentials ^String bucket-name]
  (as-map (.createBucket (client credentials) bucket-name)))

(defn bucket-exist?
  "Returns true if bucket name exists"
  [credentials bucket-name]
  (.doesBucketExist (client credentials) bucket-name))

(defn delete-bucket
  "Delete bucket given by bucket-name"
  [credentials ^String bucket-name]
  (.deleteBucket (client credentials) bucket-name))

(defn list-buckets
  [credentials]
  (map (as-map (.listBuckets (client credentials)))))
