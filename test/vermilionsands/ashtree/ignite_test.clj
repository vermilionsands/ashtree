(ns vermilionsands.ashtree.ignite-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.util.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.util.functions :as functions :refer [echo]])
  (:import [java.util.concurrent ExecutorService]
           [org.apache.ignite Ignition IgniteCompute Ignite]
           [org.apache.ignite.internal.executor GridExecutorService]
           [org.apache.ignite.lang IgniteAsyncSupport]))

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