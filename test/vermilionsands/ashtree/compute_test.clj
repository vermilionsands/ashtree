(ns vermilionsands.ashtree.compute-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.compute :as compute :refer [invoke invoke-seq broadcast]]
            [vermilionsands.ashtree.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.function :as function :refer [sfn]]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.test-helpers :as test-helpers :refer [to-upper-case]])
  (:import [java.util UUID]
           [org.apache.ignite.compute ComputeTaskTimeoutException]
           [org.apache.ignite Ignite IgniteCache]
           [org.apache.ignite.lang IgniteFuture]))

(use-fixtures :once (fixtures/ignite-fixture 2 true))

(defn- compute []
  (ignite/compute *ignite-instance*))

(deftest with-compute-test
  (ignite/with-compute (compute)
    (is (= "ODIN" (invoke to-upper-case :args ["Odin"])))
    (is (= #{"ODIN" "THOR"} (set (invoke-seq [to-upper-case to-upper-case] :args [["Odin"] ["Thor"]]))))
    (is (= ["ODIN" "ODIN"] (broadcast to-upper-case :args ["Odin"])))))

(deftest invoke-test
  (ignite/with-compute (compute)
    (testing "invoke using function"
      (is (= "ODIN" (invoke to-upper-case :args ["Odin"]))))
    (testing "invoke using partial no-arg function"
      (is (= "ODIN" (invoke (partial test-helpers/to-upper-case "Odin")))))
    (testing "invoke using symbol"
      (is (= "ODIN" (invoke 'vermilionsands.ashtree.test-helpers/to-upper-case :args ["Odin"]))))
    ; works in practice, fails in test
    #_(testing "invoke using unqualified symbol"
        (is (= "ODIN" (invoke 'to-upper-case :args ["Odin"]))))
    (testing "invoke using serializable function"
      (is (= "ODIN" (invoke (sfn [s] (.toUpperCase s)) :args ["Odin"]))))
    (testing "invoke with async flag and normal function"
      (let [result (invoke to-upper-case :opts {:async true} :args ["future"])]
        (is (instance? IgniteFuture result))
        (is (= "FUTURE" @result))))
    (testing "invoke with timeout"
      (is (thrown? ComputeTaskTimeoutException
            (invoke (sfn [] (Thread/sleep 100) :ok) :opts {:timeout 1}))))))

(deftest invoke-seq-test
  (ignite/with-compute (compute)
    (testing "invoke-seq with no-arg functions"
      (is (= #{"THOR" "ODIN" "TYR" "LOKI"}
             (set (invoke-seq (map #(partial to-upper-case %) ["Odin" "Thor" "Loki" "Tyr"]))))))
    (testing "invoke-seq with args"
      (is (= #{"THOR" "ODIN"}
             (set (invoke-seq (repeat 2 to-upper-case) :args [["Odin"] ["Thor"]])))))
    (testing "invoke-seq with mixed no-arg and arity 1 functions"
      (is (= #{"THOR" "ODIN" "TYR"}
             (set
               (invoke-seq
                 [to-upper-case (partial to-upper-case "Thor") (partial to-upper-case "Tyr")]
                 :args [["Odin"] nil []])))))
    (testing "invoke-seq with async flag and normal function"
      (let [result (invoke-seq (repeat 2 to-upper-case) :opts {:async true} :args [["future"] ["future"]])]
        (is (instance? IgniteFuture result))
        (is (= ["FUTURE" "FUTURE"] @result))))))

(deftest broadcast-test
  (ignite/with-compute (compute)
    (testing "Basic broadcast"
      (is (= ["ECHO" "ECHO"] (broadcast to-upper-case :args ["Echo"]))))
    (testing "Passing Ignite instance to function"
      (is (every? #(instance? UUID %) (broadcast test-helpers/get-node-id :args [*ignite-instance*]))))
    (testing "Passing Ignite instance to serializable function"
      (is (every? #(instance? UUID %)
                  (broadcast
                    (sfn [ignite] (.id (.localNode (.cluster ignite))))
                    :args [*ignite-instance*]))))
    (testing "Broadcast with options"
      (let [result (broadcast to-upper-case :opts {:async true} :args ["future"])]
        (is (instance? IgniteFuture result))
        (is (= ["FUTURE" "FUTURE"] @result))))))

(deftest per-node-shared-state-test
  ;; call 3 times on 2 nodes - 1 would be called 2 times, one 1 time
  ;; flickers sometimes when other tests fail
  (is (= #{1 2}
         (set
           (invoke-seq
             (repeat 3 (partial test-helpers/inc-node-state *ignite-instance*))
             :compute (compute))))))

(deftest reducer-test
  (ignite/with-compute (compute)
    (testing "Reducer without init value"
      (is (#{"RAGNAROK" "ROKRAGNA"} ;; there is no ordering guarantee
            (invoke-seq (repeat 2 to-upper-case) :args [["RAGNA"] ["ROK"]] :opts {:reduce str}))))
    (testing "Reducer with init value"
      (is (= 7
             (invoke-seq (repeat 2 (partial * 2)) :args [[1] [2]] :opts {:reduce + :reduce-init 1}))))))

(deftest affinity-test
  (let [cache (.getOrCreateCache ^Ignite *ignite-instance* "affinity-test")]
    (.put ^IgniteCache cache :test-key "test-val")
    (ignite/with-compute (compute)
      (testing "Sync affinity call"
        (is (every? #(= % "test-val")
                    (for [_ (range 10)]
                      (invoke test-helpers/cache-peek
                        :args [cache :test-key]
                        :opts {:affinity-cache "affinity-test" :affinity-key :test-key})))))
      (testing "Async affinity call"
        (is (every? #(= % "test-val")
                    (map deref
                      (for [_ (range 10)]
                        (invoke test-helpers/cache-peek
                                :args [cache :test-key]
                                :opts {:async true :affinity-cache "affinity-test" :affinity-key :test-key})))))))))
