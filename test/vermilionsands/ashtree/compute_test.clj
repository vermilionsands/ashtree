(ns vermilionsands.ashtree.compute-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.compute :as compute]
            [vermilionsands.ashtree.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.test-helpers :as test-helpers])
  (:import [org.apache.ignite.compute ComputeTaskTimeoutException]
           [org.apache.ignite.lang IgniteFuture]))

(use-fixtures :once (fixtures/ignite-fixture 2 true))

(defn- compute []
  (ignite/compute *ignite-instance*))

(defn test-fn [x] (partial test-helpers/to-upper-case x))

(deftest call-test
  (testing "Call using function"
    (let [result (compute/call (compute) (test-fn "Odin"))]
      (is (= "ODIN" result))))
  (testing "Alternative input functions"
    (testing "Call using symbol"
      (let [result (compute/call (compute) (compute/symbol-fn 'vermilionsands.ashtree.test-helpers/to-upper-case "Odin"))]
        (is (= "ODIN" result))))
    (testing "Call using serializable function"
      (let [result (compute/call (compute) (function/sfn [] (.toUpperCase "Odin")))]
        (is (= "ODIN" result)))))
  (testing "Call with async flag"
    (let [result (compute/call (compute) (test-fn "future") {:async true})]
      (is (instance? IgniteFuture result))
      (is (= "FUTURE" @result))))
  (testing "Call with timeout"
    (is (thrown? ComputeTaskTimeoutException
                 (compute/call (compute) (function/sfn [] (Thread/sleep 100) :ok) {:timeout 1})))))

(deftest map-call-test
  (let [result (compute/map-call
                 (compute)
                 [(test-fn "Odin") (test-fn "Thor") (test-fn "Loki") (test-fn "Tyr")])]
    (is (= #{"THOR" "ODIN" "TYR" "LOKI"} (set result)))))

(deftest broadcast-test
  (let [result (compute/broadcast (compute) (test-fn "Echo"))]
    (is (= ["ECHO" "ECHO"] result))))

(deftest with-compute-test
  (ignite/with-compute (compute)
    (is (= "ODIN" (compute/call* (test-fn "Odin"))))
    (is (= #{"ODIN" "THOR"} (set (compute/map-call* [(test-fn "Odin") (test-fn "Thor")]))))
    (is (= ["ODIN" "ODIN"] (compute/broadcast* (test-fn "Odin"))))))