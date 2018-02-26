(ns vermilionsands.ashtree.compute-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.compute :as compute]
            [vermilionsands.ashtree.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.test-helpers :as test-helpers]))

(use-fixtures :once (fixtures/ignite-fixture))

(defn- compute []
  (ignite/compute *ignite-instance*))

(deftest call-function-test
  (testing "Call test"
    (let [result (compute/call (compute) (partial test-helpers/to-upper-case "Odin"))]
      (is (= "ODIN" result))))
  (testing "Alternative input functions"
    (testing "Call using symbol"
      (let [result (compute/call (compute) (compute/symbol-fn 'vermilionsands.ashtree.test-helpers/to-upper-case "Odin"))]
        (is (= "ODIN" result))))
    (testing "Call using serializable function"
      (let [result (compute/call (compute) (function/sfn [] (.toUpperCase "Odin")))]
        (is (= "ODIN" result))))))