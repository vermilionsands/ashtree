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

(deftest with-compute-test
  (ignite/with-compute (compute)
    (is (= "ODIN" (compute/invoke to-upper-case "Odin")))
    (is (= #{"ODIN" "THOR"} (set (compute/invoke-seq [to-upper-case to-upper-case] [["Odin"] ["Thor"]]))))
    (is (= ["ODIN" "ODIN"] (compute/broadcast to-upper-case "Odin")))))

(deftest invoke-test
  (ignite/with-compute (compute)
    (testing "invoke using function"
      (is (= "ODIN" (compute/invoke to-upper-case "Odin"))))
    (testing "invoke using partial no-arg function"
      (is (= "ODIN" (compute/invoke (partial test-helpers/to-upper-case "Odin")))))
    (testing "Alternative input functions"
      (testing "invoke using symbol"
        (is (= "ODIN" (compute/invoke 'vermilionsands.ashtree.test-helpers/to-upper-case "Odin"))))
      (testing "invoke using serializable function"
        (is (= "ODIN" (compute/invoke (function/sfn [s] (.toUpperCase s)) "Odin")))))))

(deftest invoke-seq-test
  (ignite/with-compute (compute)
    (testing "invoke-seq with no-arg functions"
      (is (= #{"THOR" "ODIN" "TYR" "LOKI"}
             (set
               (compute/invoke-seq
                 (map #(partial to-upper-case %) ["Odin" "Thor" "Loki" "Tyr"]))))))
    (testing "invoke-seq with args"
      (is (= #{"THOR" "ODIN"}
             (set (compute/invoke-seq (repeat 2 to-upper-case) [["Odin"] ["Thor"]])))))
    (testing "invoke-seq with mixed no-arg and arity 1 functions"
      (is (= #{"THOR" "ODIN" "TYR"}
             (set
               (compute/invoke-seq
                 [to-upper-case (partial to-upper-case "Thor") (partial to-upper-case "Tyr")]
                 [["Odin"] nil []])))))))

(deftest broadcast-test
  (ignite/with-compute (compute)
    (testing "Basic broadcast"
      (is (= ["ECHO" "ECHO"] (compute/broadcast to-upper-case "Echo"))))
    (testing "Passing Ignite instance to function"
      (is (every? #(instance? UUID %) (compute/broadcast test-helpers/get-node-id *ignite-instance*))))
    (testing "Passing Ignite instance to serializable function"
      (is (every? #(instance? UUID %)
                  (compute/broadcast
                    (function/sfn [ignite] (.id (.localNode (.cluster ignite))))
                    *ignite-instance*))))))

(deftest with-opts-test
  (ignite/with-compute (compute)
    (testing "invoke with async flag and normal function"
      (let [result (compute/invoke (compute/with-opts to-upper-case :async true) "future")]
        (is (instance? IgniteFuture result))
        (is (= "FUTURE" @result))))
    (testing "invoke with async flag and serializable function"
      (let [result (compute/invoke (compute/with-opts (function/sfn [x] (.toUpperCase x)) :async true) "future")]
        (is (instance? IgniteFuture result))
        (is (= "FUTURE" @result))))
    (testing "invoke with async flag and symbol"
      (let [result (compute/invoke (compute/with-opts 'vermilionsands.ashtree.test-helpers/to-upper-case :async true) "future")]
        (is (instance? IgniteFuture result))
        (is (= "FUTURE" @result))))
    (testing "invoke with timeout"
      (is (thrown? ComputeTaskTimeoutException
                   (compute/invoke (compute/with-opts (function/sfn [] (Thread/sleep 100) :ok) :timeout 1)))))))

(deftest per-node-shared-state-test
  ;; call 3 times on 2 nodes - 1 would be called 2 times, one 1 time
  ;; ??? flickers sometimes?
  (is (= #{1 2}
         (set
           (compute/invoke-seq*
             (compute)
             (repeat 3 (partial test-helpers/inc-node-state *ignite-instance*)))))))

(deftest reducer-test
  (ignite/with-compute (compute)
    (testing "Reducer without init value"
      (is (#{"RAGNAROK" "ROKRAGNA"} ;; there is no ordering guarantee
            (compute/invoke-seq (repeat 2 to-upper-case) [["RAGNA"] ["ROK"]] (compute/reducer str)))))
    (testing "Reducer with init value"
      (is (= 7
             (compute/invoke-seq (repeat 2 (partial * 2)) [[1] [2]] (compute/reducer + 1)))))))