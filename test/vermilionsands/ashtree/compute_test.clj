(ns vermilionsands.ashtree.compute-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.compute :as compute]
            [vermilionsands.ashtree.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.test-helpers :as test-helpers])
  (:import [java.util UUID]
           [org.apache.ignite.compute ComputeTaskTimeoutException]
           [org.apache.ignite.lang IgniteFuture]))

(use-fixtures :once (fixtures/ignite-fixture 2 true))

(defn- compute []
  (ignite/compute *ignite-instance*))

(defn test-fn [x] (partial test-helpers/to-upper-case x))

(deftest call-test
  (testing "Call using function"
    (is (= "ODIN" (compute/call (compute) (test-fn "Odin")))))
  (testing "Alternative input functions"
    (testing "Call using symbol"
      (is (= "ODIN"
             (compute/call (compute) (compute/symbol-fn 'vermilionsands.ashtree.test-helpers/to-upper-case "Odin")))))
    (testing "Call using serializable function"
      (is (= "ODIN" (compute/call (compute) (function/sfn [] (.toUpperCase "Odin")))))))
  (testing "Call with async flag"
    (let [result (compute/call (compute) (test-fn "future") {:async true})]
      (is (instance? IgniteFuture result))
      (is (= "FUTURE" @result))))
  (testing "Call with timeout"
    (is (thrown? ComputeTaskTimeoutException
                 (compute/call (compute) (function/sfn [] (Thread/sleep 100) :ok) {:timeout 1})))))

(deftest map-call-test
  (is (= #{"THOR" "ODIN" "TYR" "LOKI"}
         (set
           (compute/map-call
             (compute)
             [(test-fn "Odin") (test-fn "Thor") (test-fn "Loki") (test-fn "Tyr")])))))

(deftest broadcast-test
  (testing "Basic broadcast"
    (is (= ["ECHO" "ECHO"] (compute/broadcast (compute) (test-fn "Echo")))))
  (testing "Passing Ignite instance to function"
    (is (every? #(instance? UUID %)
                (compute/broadcast (compute) (partial test-helpers/get-node-id *ignite-instance*))))))

(deftest with-compute-test
  (ignite/with-compute (compute)
    (is (= "ODIN" (compute/call* (test-fn "Odin"))))
    (is (= #{"ODIN" "THOR"} (set (compute/map-call* [(test-fn "Odin") (test-fn "Thor")]))))
    (is (= ["ODIN" "ODIN"] (compute/broadcast* (test-fn "Odin"))))))

(deftest per-node-shared-state-test
  ;; call 3 times on 2 nodes - 1 would be called 2 times, one 1 time
  (is (= #{1 2}
         (set
           (compute/map-call
             (compute)
             (repeat 3 (partial test-helpers/inc-node-state *ignite-instance*)))))))