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

(defn- get-private-field [instance ^String field]
  (let [f (.getDeclaredField (.getClass instance) field)]
    (try
      (.setAccessible f true)
      (.get f instance)
      (catch Exception _ nil)
      (finally
        (.setAccessible f false)))))

(defn- compute-executor-name [instance]
  (get-private-field instance "execName"))

(defn- get-cluster-from-executor [instance]
  (get-private-field instance "prj"))

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
    (let [test-cluster (.forRemotes (.cluster *ignite-instance*))
          compute (ignite/compute *ignite-instance* :cluster test-cluster)]
      (is (= test-cluster (.clusterGroup compute))))))

(deftest executor-test
  (testing "Submitting functions as callables"
    (let [exec ^ExecutorService (ignite/executor-service *ignite-instance*)]
      (is (= "echo" @(.submit exec ^Callable (partial echo "echo"))))
      ;; ugly, rework this at some point in time, eval-form shouldn't be needed
      (is (= "echo" @(.submit exec ^Callable (function/eval-fn (function/eval-form (function/sfn [] "echo"))))))
      (is (= "echo" @(.submit exec ^Callable (partial (function/symbol-fn 'identity) "echo"))))))
  (testing "Executor service uses correct cluster group"
    (let [test-cluster (.forRemotes (.cluster *ignite-instance*))
          exec ^GridExecutorService (ignite/executor-service *ignite-instance* test-cluster)
          executor-cluster (get-cluster-from-executor exec)]
      (is (= test-cluster executor-cluster)))))