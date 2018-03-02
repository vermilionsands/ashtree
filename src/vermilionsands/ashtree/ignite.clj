(ns vermilionsands.ashtree.ignite
  (:import [java.util.concurrent ExecutorService]
           [org.apache.ignite Ignite IgniteCompute]
           [org.apache.ignite.cluster ClusterGroup]))

(def ^:dynamic *compute* "Compute API instance to be used with with-compute" nil)

(defn ^IgniteCompute compute
  "Get an instance of compute API.

  Args:
  instance - Ignite instance
  opts     - options map

  Options:
  :async         - if true would enable async mode (deprecated)
  :cluster       - ClusterGroup instance group that would be associated with this compute instance
                   All tasks using this compute instance would be executed on nodes from this cluster group.
  :executor      - name of an executor to be used by this compute instance
                   All tasks using this compute instance would be processed by this executor. If the executor
                   does not exist 'public' pool would be used."
  [^Ignite instance & [opts]]
  (let [{:keys [async cluster executor]} opts
        compute (if cluster
                  (.compute instance cluster)
                  (.compute instance))]
    (cond-> compute
      async    (.withAsync)
      executor (.withExecutor executor))))

(defmacro with-compute
  "Evaluates body in a context in which *compute* is bound to a given compute API instance"
  [compute & body]
  `(binding [*compute* ~compute]
     ~@body))

(defn ^ExecutorService executor-service
  "Get a distributed executor service from Ignite instance

  Args:
  instance      - Ignite instance
  cluster-group - optional cluster group that would be used by the executor"
  [^Ignite instance & [cluster-group]]
  (if cluster-group
    (.executorService instance cluster-group)
    (.executorService instance)))