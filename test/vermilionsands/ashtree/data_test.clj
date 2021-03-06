(ns vermilionsands.ashtree.data-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.data :as data]
            [vermilionsands.ashtree.util.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.util.functions :as functions])
  (:import [java.util.concurrent CountDownLatch]
           [org.apache.ignite Ignition IgniteMessaging IgniteAtomicReference]
           [vermilionsands.ashtree.data IgniteAtom]))

(use-fixtures :once (fixtures/ignite-fixture))

(defn- future-swap [^CountDownLatch start ^CountDownLatch done a f & args]
  (future
    (.await start)
    (apply swap! a f args)
    (.countDown done)))

(deftest init-test
  (let [a (data/distributed-atom *ignite-instance* "init-test" 1)
        b (data/distributed-atom *ignite-instance* "init-test" 2 :global-notifications true)
        c (data/distributed-atom *ignite-instance* "atom-with-topic" 2 :global-notifications true)]
    (testing "Atom should be initialized with init value"
      (is (= 1 @a)))
    (testing "Atom should not have messaging instance"
      (is (nil? (.-messaging ^IgniteAtom a))))
    (testing "Init should be skipped if atom already exists"
      (is (= 1 @b)))
    (testing "Current options should not be overridden"
      (is (nil? (.-messaging ^IgniteAtom b))))
    (testing "Atom has messaging instance"
      (is (instance? IgniteMessaging (.-messaging ^IgniteAtom c)))
      (is (= "ashtree-atom-atom-with-topic"
             (:notification-topic (.get ^IgniteAtomicReference  (.-shared_ctx ^IgniteAtom c))))))))

(deftest swap-test
  (let [a (data/distributed-atom *ignite-instance* "swap-test" 0)]
    (testing "Swap arities"
      (is (= 1 (swap! a inc)))
      (is (= 2 (swap! a + 1)))
      (is (= 4 (swap! a + 1 1)))
      (is (= 7 (swap! a + 1 1 1)))
      (is (= 11 (swap! a + 1 1 1 1))))
    (testing "Multiple swaps"
      (reset! a 0)
      (let [n 100
            start (CountDownLatch. 1)
            done (CountDownLatch. n)]
        (doseq [_ (range n)]
          (future-swap start done a inc))
        (.countDown start)
        (.await done)
        (is (= 100 @a))))))

(deftest compare-and-set-test
  (let [a (data/distributed-atom *ignite-instance* "compare-and-swap-test" 0)
        old-val 0]
    (is (true?  (compare-and-set! a old-val 1)))
    (is (false? (compare-and-set! a old-val 2)))))

(deftest reset-test
  (let [a (data/distributed-atom *ignite-instance* "reset-test" 0)]
    (is (= 0 @a))
    (reset! a 1)
    (is (= 1 @a))))

(deftest two-instances-test
  (let [a (data/distributed-atom *ignite-instance* "two-instances-test" 0)
        b (data/distributed-atom *ignite-instance* "two-instances-test" 0)]
    (is (zero? @a))
    (is (zero? @b))
    (swap! a inc)
    (is (= 1 @a))
    (is (= 1 @b))))

(deftest meta-test
  (let [a (data/distributed-atom *ignite-instance* "meta-test" 0)
        m {:test :meta}]
    (is (nil? (meta a)))
    (reset-meta! a m)
    (is (= m (meta a)))
    (alter-meta! a assoc :altered :key)
    (is (= {:test :meta :altered :key} (meta a)))))

(deftest meta-is-per-instance-test
  (let [a (data/distributed-atom *ignite-instance* "meta-test" 0)
        b (data/distributed-atom *ignite-instance* "meta-test" 0)]
    (reset-meta! a {:test :meta})
    (is (= {:test :meta} (meta a)))
    (is (nil? (meta b)))))

(deftest validator-test
  (testing "Local validator is not shared between instances"
    (let [a (data/distributed-atom *ignite-instance* "local-validator-test" 0)
          b (data/distributed-atom *ignite-instance* "local-validator-test" 0)]
      (set-validator! a functions/less-than-10)
      (is (= functions/less-than-10 (get-validator a)))
      (is (nil? (get-validator b)))
      (swap! a inc)
      (is (= 1 @a))
      (is (thrown? IllegalStateException (swap! a + 10)))
      (is (= 1 @a))
      (is (= 1 @b))
      (swap! b + 10)
      (is (= 11 @a))
      (is (= 11 @b))))
  (testing "Shared validator is shared between instances"
    (let [a (data/distributed-atom *ignite-instance* "shared-validator-test" 0)
          b (data/distributed-atom *ignite-instance* "shared-validator-test" 0)]
      (data/set-shared-validator! a functions/less-than-10)
      (swap! a inc)
      (is (= 1 @a))
      (is (thrown? IllegalStateException (swap! a + 10)))
      (is (thrown? IllegalStateException (swap! b + 10)))
      (is (= 1 @a))
      (is (= 1 @b))))
  (testing "Both shared and local validators are called"
    (let [a (data/distributed-atom *ignite-instance* "mixed-validator-test" 0)]
      (set-validator! a even?)
      (data/set-shared-validator! a functions/less-than-4)
      ;; shared validator kicks in
      (is (thrown? IllegalStateException (swap! a + 10)))
      ;; local validator kicks in
      (is (thrown? IllegalStateException (swap! a + 1))))))

(deftest local-watch-test
  (testing "Local watch test without notification test"
    (let [a (data/distributed-atom *ignite-instance* "watch-test" 0)
          b (data/distributed-atom *ignite-instance* "watch-test" 0)
          [watch-a state-a] (functions/watch-and-store)
          [watch-b state-b] (functions/watch-and-store)]
      (add-watch a :1 watch-a)
      (add-watch b :1 watch-b)
      (is (= {:1 watch-a} (.getWatches a)))
      (is (= {:1 watch-b} (.getWatches b)))
      (swap! a inc)
      (is (= [[0 1]] @state-a))
      (is (empty? @state-b))))
  (testing "Local watch with notification test"
    (let [a (data/distributed-atom *ignite-instance* "notification-test" 0 :global-notifications true)
          b (data/distributed-atom *ignite-instance* "notification-test" 0)
          [watch-a state-a] (functions/watch-and-store)
          [watch-b state-b] (functions/watch-and-store)]
      (add-watch a :local watch-a)
      (add-watch b :local watch-b)
      (swap! a inc)
      (Thread/sleep 200) ;; let notification topic do it's job
      (is (= [[0 1]] @state-a))
      (is (= @state-a @state-b))
      (testing "Closing and removing listener"
        (.close b)
        (swap! a inc)
        (is (= [[0 1] [1 2]] @state-a))
        ;; b should not listen for changes any more
        (is (= [[0 1]] @state-b))))))

(deftest shared-watch-test
  (let [a (data/distributed-atom *ignite-instance* "shared-watch-test" 0 :global-notifications true)
        b (data/distributed-atom *ignite-instance* "shared-watch-test" 0)
        [local-watch local-state] (functions/watch-and-store)]
    (add-watch a :local local-watch)
    (data/add-shared-watch a :shared functions/store-to-atom-watch)
    (is (some? (:shared (data/get-shared-watches a))))
    (is (some? (:shared (data/get-shared-watches b))))
    (swap! a inc)
    (Thread/sleep 200)
    (is (= [[0 1]] @local-state))
    (is (= [[0 1] [0 1]] @functions/watch-log))))

(deftest skip-identity-test
  (let [a (data/distributed-atom *ignite-instance* "skip-identity-test" 0 :skip-identity true)
        [local-watch local-state] (functions/watch-and-store)]
    (add-watch a :local local-watch)
    ;; should be ignored
    (swap! a identity)
    (reset! a 0)
    (is (= [] @local-state))
    ;; should be triggered
    (swap! a inc)
    (reset! a 0)
    (is (= [[0 1] [1 0] @local-state]))))

(deftest destroy-test
  (let [a (data/distributed-atom *ignite-instance* "destroy-test" 0)
        b (data/distributed-atom *ignite-instance* "destroy-test" 0)]
    (swap! a inc)
    (is (= 1 @a))
    (is (= 1 @b))
    (data/destroy! a)
    (is (thrown? IllegalStateException (= 1 @a)))
    (is (thrown? IllegalStateException (= 1 @b)))))