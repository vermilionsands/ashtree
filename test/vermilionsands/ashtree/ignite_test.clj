(ns vermilionsands.ashtree.ignite-test
  (:require [clojure.test :refer [are deftest is testing use-fixtures]]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.util.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.util.functions :as functions :refer [echo]])
  (:import [java.util.concurrent ExecutorService]
           [org.apache.ignite Ignition IgniteCompute Ignite IgniteCluster]
           [org.apache.ignite.cluster ClusterGroup ClusterNode]
           [org.apache.ignite.internal.executor GridExecutorService]
           [org.apache.ignite.lang IgniteAsyncSupport IgnitePredicate]))

(use-fixtures :once (fixtures/ignite-fixture 2 true))

(defn- compute-executor-name [instance]
  (fixtures/get-private-field instance "execName"))

(defn- get-cluster-from-executor [instance]
  (fixtures/get-private-field instance "prj"))

(deftest compute-instance-test
  (testing "Default instance"
    (let [compute (ignite/compute *ignite-instance*)]
      (is (instance? IgniteCompute compute))
      (is (false? (.isAsync ^IgniteAsyncSupport compute)))
      (is (nil? (compute-executor-name compute)))))
  (testing "Instance with async enabled"
    (let [compute (ignite/compute *ignite-instance* :async true)]
      (is (.isAsync compute))))
  (testing "Instance with non-default executor"
    (let [compute (ignite/compute *ignite-instance* :executor "test-pool")]
      (is (= "test-pool" (compute-executor-name compute)))))
  (testing "Instance with cluster group"
    (let [test-cluster (ignite/cluster *ignite-instance* :remote)
          compute (ignite/compute *ignite-instance* :cluster test-cluster)]
      (is (= test-cluster (.clusterGroup compute))))))

(deftest executor-test
  (testing "Submitting functions as callables"
    (let [exec ^ExecutorService (ignite/executor-service *ignite-instance*)]
      (is (= "echo" @(.submit exec ^Callable (partial echo "echo"))))
      (is (= "echo" @(.submit exec ^Callable (function/eval-fn (function/sfn [] "echo")))))
      (is (= "echo" @(.submit exec ^Callable (partial (function/symbol-fn 'identity) "echo"))))))
  (testing "Executor service uses correct cluster group"
    (let [test-cluster (ignite/cluster *ignite-instance* :remote)
          exec ^GridExecutorService (ignite/executor-service *ignite-instance* test-cluster)
          executor-cluster (get-cluster-from-executor exec)]
      (is (= test-cluster executor-cluster)))))

(deftest local-node-map-test
  (let [local (ignite/cluster *ignite-instance*)]
    (ignite/put-local! local :test 1)
    (is (= 1 (ignite/get-local local :test)))
    (is (= 1 (ignite/put-local! local :test 2 true)))
    (ignite/put-local! local :other 3 true)
    (is (= 3 (ignite/get-local local :other)))))

(deftest cluster-test
  (testing "IgniteCluster instance"
    (is (instance? IgniteCluster (ignite/cluster *ignite-instance*))))
  (testing "Cluster group instance"
    (is (instance? ClusterGroup (ignite/cluster *ignite-instance* :local :youngest)))))

(deftest cluster-group-test
  (let [c ^IgniteCluster (ignite/cluster *ignite-instance*)
        node (.localNode c)]
    (testing "Fail on no or unsupported option"
      (is (thrown? IllegalArgumentException (ignite/cluster-group c nil)))
      (is (thrown? IllegalArgumentException (ignite/cluster-group c :wrong-option))))
    (testing "Supported options should return cluster group"
      ;; just checking that options are processed, not validating *what* is returned
      (let [test #(and (some? %1) (instance? ClusterGroup %1))
            test-cluster-group (fn [& args] (test (apply ignite/cluster-group c args)))
            predicate (reify IgnitePredicate (apply [_ _] true))]
        (are [x] (true? x)
          (test-cluster-group :attribute "some" "attribute")
          (test-cluster-group :cache-nodes "test")
          (test-cluster-group :client-nodes "test")
          (test-cluster-group :clients)
          (test-cluster-group :daemons)
          (test-cluster-group :data-nodes "test")
          (test-cluster-group :host node)
          (test-cluster-group :host ["localhost"])
          (test-cluster-group :local)
          (test-cluster-group :nodes [node])
          (test-cluster-group :node-id [(.id ^ClusterNode node)])
          (test-cluster-group :others (ignite/cluster-group c :random))
          (test-cluster-group :others [node])
          (test-cluster-group :others [node])
          (test-cluster-group :predicate predicate)
          (test-cluster-group :oldest)
          (test-cluster-group :random)
          (test-cluster-group :remote)
          (test-cluster-group :servers)
          (test-cluster-group :youngest))))
    (testing "Local fails when not called on IgniteCluster"
      (is (thrown? IllegalArgumentException
                  (ignite/cluster-group c :random :local))))
    (testing "Multiple subsequent options return a cluster group"
      (let [cluster-group (ignite/cluster-group c :servers :attribute "some" "attribute" :youngest)]
        (is (some? cluster-group))
        (is (instance? ClusterGroup cluster-group))))))