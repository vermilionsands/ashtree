(ns vermilionsands.ashtree.compute-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.compute :as compute]
            [vermilionsands.ashtree.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.test-helpers :as test-helpers :refer [to-upper-case]])
  (:import [java.util UUID]
           [org.apache.ignite.compute ComputeTaskTimeoutException]
           [org.apache.ignite.lang IgniteFuture]))

(use-fixtures :once (fixtures/ignite-fixture 2 true))

(defn- compute []
  (ignite/compute *ignite-instance*))

;; opts tests - do osobnego testu

(deftest invoke-test
  (testing "invoke using function"
    (is (= "ODIN" (compute/invoke (compute) to-upper-case "Odin"))))
  (testing "invoke using partial no-arg function"
    (is (= "ODIN" (compute/invoke (compute) (partial test-helpers/to-upper-case "Odin")))))
  (testing "Alternative input functions"
    (testing "invoke using symbol"
      (is (= "ODIN" (compute/invoke (compute) 'vermilionsands.ashtree.test-helpers/to-upper-case "Odin"))))
    (testing "invoke using serializable function"
      (is (= "ODIN" (compute/invoke (compute) (function/sfn [s] (.toUpperCase s)) "Odin")))))
  (testing "invoke with async flag"
    (let [result (compute/invoke (compute) (compute/with-opts to-upper-case :async true) "future")]
      (is (instance? IgniteFuture result))
      (is (= "FUTURE" @result))))
  (testing "invoke with timeout"
    (is (thrown? ComputeTaskTimeoutException
                 (compute/invoke (compute) (compute/with-opts (function/sfn [] (Thread/sleep 100) :ok) :timeout 1))))))

(deftest invoke-seq-test
  (testing "invoke-seq with no-arg functions"
    (is (= #{"THOR" "ODIN" "TYR" "LOKI"}
           (set
             (compute/invoke-seq
               (compute)
               (map #(partial to-upper-case %) ["Odin" "Thor" "Loki" "Tyr"]))))))
  (testing "invoke-seq with args"
    (is (= #{"THOR" "ODIN"}
           (set (compute/invoke-seq (compute) (repeat 2 to-upper-case) [["Odin"] ["Thor"]])))))
  (testing "invoke-seq with mixed no-arg and arity 1 functions"
    (is (= #{"THOR" "ODIN" "TYR"}
           (set
             (compute/invoke-seq
               (compute)
               [to-upper-case (partial to-upper-case "Thor") (partial to-upper-case "Tyr")]
               [["Odin"] nil []]))))))

(deftest broadcast-test
  (testing "Basic broadcast"
    (is (= ["ECHO" "ECHO"] (compute/broadcast (compute) to-upper-case "Echo"))))
  (testing "Passing Ignite instance to function"
    (is (every? #(instance? UUID %)
                (compute/broadcast (compute) (partial test-helpers/get-node-id *ignite-instance*))))))

(deftest with-compute-test
  (ignite/with-compute (compute)
    (is (= "ODIN" (compute/invoke* to-upper-case "Odin")))
    (is (= #{"ODIN" "THOR"} (set (compute/invoke-seq* [to-upper-case to-upper-case] [["Odin"] ["Thor"]]))))
    (is (= ["ODIN" "ODIN"] (compute/broadcast* to-upper-case "Odin")))))

(deftest per-node-shared-state-test
  ;; call 3 times on 2 nodes - 1 would be called 2 times, one 1 time
  ;; ??? flickers sometimes?
  (is (= #{1 2}
         (set
           (compute/invoke-seq
             (compute)
             (repeat 3 (partial test-helpers/inc-node-state *ignite-instance*)))))))