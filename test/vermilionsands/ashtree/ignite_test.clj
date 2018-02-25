(ns vermilionsands.ashtree.ignite-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.fixtures :as fixtures :refer [*ignite-instance*]])
  (:import [org.apache.ignite Ignition IgniteCompute Ignite]
           [org.apache.ignite.lang IgniteAsyncSupport]))

(use-fixtures :once (fixtures/ignite-fixture))

(defn- compute-executor-name [instance]
  (let [f (.getDeclaredField (.getClass instance) "execName")]
    (try
      (.setAccessible f true)
      (.get f instance)
      (catch Exception _ nil)
      (finally
        (.setAccessible f false)))))

(deftest compute-instance-test
  (testing "Default instance"
    (let [compute (ignite/compute *ignite-instance*)]
      (is (instance? IgniteCompute compute))
      (is (false? (.isAsync ^IgniteAsyncSupport compute)))
      (is (nil? (compute-executor-name compute)))))
  (testing "Instance with async enabled"
    (let [compute (ignite/compute *ignite-instance* {:async true})]
      (is (.isAsync compute))))
  (testing "Instance with non-default executor"
    (let [compute (ignite/compute *ignite-instance* {:executor "test-pool"})]
      (is (= "test-pool" (compute-executor-name compute)))))
  (testing "Instance with cluster group"
    (let [test-cluster (.forRemotes (.cluster *ignite-instance*))
          compute (ignite/compute *ignite-instance* {:cluster-group test-cluster})]
      (is (= test-cluster (.clusterGroup compute))))))