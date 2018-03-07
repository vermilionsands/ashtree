(ns vermilionsands.ashtree.ignite
  (:require [clojure.core.memoize :as memoize])
  (:import [java.util.concurrent ExecutorService]
           [org.apache.ignite Ignite IgniteCompute IgniteCluster]
           [org.apache.ignite.cluster ClusterGroup ClusterNode]))

(defn compute*
  "Like compute, but accepts options map."
  ([^Ignite instance]
   (compute* instance nil))
  ([^Ignite instance opts-map]
   (let [{:keys [async cluster executor]} opts-map
         compute (if cluster
                   (.compute instance cluster)
                   (.compute instance))]
     (cond-> compute
             async    (.withAsync)
             executor (.withExecutor executor)))))

(defn ^IgniteCompute compute
  "Get an instance of compute API.

  (compute ignite-instance :cluster cluster-group)

  Args:
  instance - Ignite instance
  opts     - options as subsequent keyword value

  Options:
  :async    - if true would enable async mode (deprecated)
  :cluster  - ClusterGroup instance group that would be associated with this compute instance
              All tasks using this compute instance would be executed on nodes from this cluster group.
  :executor - name of an executor to be used by this compute instance
              All tasks using this compute instance would be processed by this executor. If the executor
              does not exist 'public' pool would be used."
  [^Ignite instance & opts]
  (compute* instance (apply hash-map opts)))

(defn ^ExecutorService executor-service
  "Get a distributed executor service from Ignite instance

  Args:
  instance      - Ignite instance
  cluster-group - optional cluster group that would be used by the executor"
  [^Ignite instance & [cluster-group]]
  (if cluster-group
    (.executorService instance cluster-group)
    (.executorService instance)))

(defn cluster-group
  "Get a cluster group for a given ClusterGroup instance and options.

  (cluster-group some-cluster :remote :cache-nodes cache-name :oldest)

  Args:
  cluster - ClusterGroup instance
  k - key for getting cluster group
  args - optional arguments and further keys

  Keys and arguments:
  :attribute ^String name val - nodes containing attribute with given name and val
  :cache-nodes ^String cache-name - nodes with given cache
  :client-nodes ^String cache-name - nodes in client mode with given cache
  :clients - nodes in client mode
  :daemons - daemon nodes
  :data-nodes ^String cache-name - data nodes with given cache
  :host ^ClusterNode node or [^String host1 & hosts] - nodes with same hosts as node, or with specified hosts
  :local - local node (requires IgniteCluster instance as cluster)
  :nodes [^ClusterNode node & nodes] - group with given nodes
  :node-id [^UUID uuid & uuids] - nodes with specified ids
  :oldest - oldest node (one)
  :others ^ClusterGroup group or [^ClusterNode node & nodes] - nodes not included in specified group or nodes
  :predicate ^IgnitePredicate pred - nodes that pass predicate
  :random - random node (one)
  :remote - remote nodes
  :servers - nodes in server mode
  :youngest - youngest node (one)"
  [^ClusterGroup cluster [k & [arg & more :as args]]]
  (when-not k
    (throw (IllegalArgumentException. "Cluster group key cannot be null")))
  (let [[cluster-group' args']
        (condp = k
          :attribute    [(.forAttribute cluster arg (first more)) (rest more)]
          :cache-nodes  [(.forCacheNodes cluster arg) more]
          :client-nodes [(.forClientNodes cluster arg) more]
          :clients      [(.forClients cluster) args]
          :daemons      [(.forDaemons cluster) args]
          :data-nodes   [(.forDataNodes cluster arg) more]
          :host         [(if (instance? ClusterNode arg)
                          (.forHost cluster ^ClusterNode arg)
                          (.forHost cluster (first arg) (into-array String (rest arg))))
                         more]
          :local        [(.forLocal ^IgniteCluster cluster) more]
          :nodes        [(.forNodes cluster arg) more]
          :node-id      [(.forNodeIds cluster arg) more]
          :oldest       [(.forOldest cluster) more]
          :others       [(if (instance? ClusterGroup arg)
                          (.forOthers cluster arg)
                          (.forOthers cluster (first arg) (into-array ClusterNode (rest arg))))
                         more]
          :predicate    [(.forPredicate cluster arg) args]
          :random       [(.forRandom cluster) args]
          :remote       [(.forRemotes cluster) args]
          :servers      [(.forServers cluster) args]
          :youngest     [(.forYoungest cluster) args]
          (throw (IllegalArgumentException. (format "Unsupported key %s" k))))]
    (if (empty? args')
      cluster-group'
      (recur cluster-group' args'))))

(defn cluster
  "Get a ClusterGroup for a given Ignite instance. If k and optionally further arguments are specified
  cluster-group would be called to get a more specific cluster group.

  See cluster-group for possible options."
  ([^Ignite instance & [k :as cluster-opts]]
   (let [cluster (.cluster instance)]
     (if k
       (cluster-group cluster cluster-opts)
       cluster))))