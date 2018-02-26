(ns vermilionsands.ashtree.ignite
  (:import [org.apache.ignite Ignite IgniteCompute]))

(def ^:dynamic *compute* "Compute API instance to be used with with-compute" nil)

(defn compute
  "Get an instance of compute API.

  Args:
  instance - Ignite instance
  opts     - options map

  Options:
  :async         - if true would enable async mode (deprecated)
  :cluster-group - ClusterGroup instance group that would be associated with this compute instance
                   All tasks using this compute instance would be executed on nodes from this cluster group.
  :executor      - name of an executor to be used by this compute instance
                   All tasks using this compute instance would be processed by this executor. If the executor
                   does not exist 'public' pool would be used."
  ^IgniteCompute [^Ignite instance & [opts]]
  (let [{:keys [async cluster-group executor]} opts
        compute (if cluster-group
                  (.compute instance cluster-group)
                  (.compute instance))]
    (cond-> compute
            async         (.withAsync)
            executor      (.withExecutor executor))))

(defmacro with-compute
  "Evaluates body in a context in which *compute* is bound to a given compute API instance"
  [compute & body]
  `(binding [*compute* ~compute]
     ~@body))