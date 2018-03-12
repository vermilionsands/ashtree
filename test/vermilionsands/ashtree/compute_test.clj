(ns vermilionsands.ashtree.compute-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [vermilionsands.ashtree.compute :as compute :refer [invoke with-compute]]
            [vermilionsands.ashtree.function :refer [sfn]]
            [vermilionsands.ashtree.ignite :as ignite]
            [vermilionsands.ashtree.util.fixtures :as fixtures :refer [*ignite-instance*]]
            [vermilionsands.ashtree.util.functions :as functions :refer [echo]]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.compute :as c])
  (:import [clojure.lang IDeref IPending IBlockingDeref]
           [java.util UUID]
           [java.util.concurrent TimeUnit]
           [org.apache.ignite Ignite IgniteCache]
           [org.apache.ignite.compute ComputeTaskTimeoutException ComputeTaskFuture ComputeTaskSession]
           [org.apache.ignite.internal GridKernalContext]
           [org.apache.ignite.internal.processors.task GridTaskThreadContextKey]
           [org.apache.ignite.lang IgniteFuture IgniteCallable IgniteReducer IgniteFutureCancelledException IgniteFutureTimeoutException IgniteClosure]
           [vermilionsands.ashtree.compute AshtreeFuture]))

(use-fixtures :once (fixtures/ignite-fixture 2 true))

(defn- compute []
  (ignite/compute *ignite-instance*))

(function/defsfn ^:private long-fn [] (Thread/sleep 1000) :ok)

(deftest callable-test
  (testing "returns the same IgniteCallable"
    (let [x (reify
              IgniteCallable
              (call [_] "test"))
          callable (compute/callable x)]
      (is (instance? IgniteCallable callable))
      (is (= x callable))
      (is (= "test" (.call callable)))))
  (testing "IgniteCallable for function"
    (let [callable (compute/callable echo ["fn-test"])]
      (is (instance? IgniteCallable callable))
      (is (= "fn-test" (.call callable)))))
  (testing "IgniteCallable for symbol"
    (let [callable (compute/callable 'vermilionsands.ashtree.util.functions/echo ["symbol-test"])]
      (is (instance? IgniteCallable callable))
      (is (= "symbol-test" (.call callable)))))
  (testing "IgniteCallable for sfn"
    (let [callable (compute/callable (sfn [x] x) ["sfn-test"])]
      (is (instance? IgniteCallable callable))
      (is (= "sfn-test" (.call callable)))))
  (testing "exception for unsupported type"
    (is (thrown? IllegalArgumentException (compute/callable "should fail")))))

(deftest reducer-test
  (testing "returns the same IgniteReducer"
    (let [reducer-state (atom 0)
          x (reify
              IgniteReducer
              (collect [_ x] (swap! reducer-state + x) true)
              (reduce [_] @reducer-state))
          reducer (compute/reducer x "this-should-be-ignored")]
      (doto reducer (.collect 1) (.collect 1))
      (is (instance? IgniteReducer reducer))
      (is (= x reducer))
      (is (= 2 (.reduce reducer)))))
  (testing "IgniteReducer for function"
    (let [reducer (compute/reducer + 0)]
      (doto reducer (.collect 1) (.collect 1))
      (is (instance? IgniteReducer reducer))
      (is (= 2 (.reduce reducer)))))
  (testing "IgniteReducer for symbol"
    (let [reducer (compute/reducer 'clojure.core/+ 0)]
      (doto reducer (.collect 1) (.collect 1))
      (is (instance? IgniteReducer reducer))
      (is (= 2 (.reduce reducer)))))
  (testing "IgniteReducer for sfn"
    (let [reducer (compute/reducer (sfn [acc x] (+ acc x)) 0)]
      (doto reducer (.collect 1) (.collect 1))
      (is (instance? IgniteReducer reducer))
      (is (= 2 (.reduce reducer)))))
  (testing "init value"
    (let [no-init   (compute/reducer conj)
          with-init (compute/reducer conj (cons 0 nil))]
      (.collect no-init 1)
      (.collect with-init 1)
      (is (= (list 1)   (.reduce no-init)))
      (is (= (list 1 0) (.reduce with-init)))))
  (testing "exception for unsupported type"
    (is (thrown? IllegalArgumentException (compute/reducer "should fail")))))

(deftest closure-test
  (testing "returns the same IgniteClosure"
    (let [x (reify
              IgniteClosure
              (apply [_ x] x))
          closure (compute/closure x)]
      (is (instance? IgniteClosure closure))
      (is (= x closure))
      (is (= "test" (.apply closure "test")))))
  (testing "IgniteClosure for function"
    (let [closure (compute/closure echo)]
      (is (instance? IgniteClosure closure))
      (is (= "fn-test" (.apply closure "fn-test")))))
  (testing "IgniteClosure for symbol"
    (let [closure (compute/closure 'vermilionsands.ashtree.util.functions/echo)]
      (is (instance? IgniteClosure closure))
      (is (= "symbol-test" (.apply closure "symbol-test")))))
  (testing "IgniteClosure for sfn"
    (let [closure (compute/closure (sfn [x] x))]
      (is (instance? IgniteClosure closure))
      (is (= "sfn-test" (.apply closure "sfn-test")))))
  (testing "adding coercion"
    (let [closure (compute/closure clojure.string/reverse str)]
      (is (= "321" (.apply closure 123)))))
  (testing "exception for unsupported type"
    (is (thrown? IllegalArgumentException (compute/closure "should fail")))))

(deftest future-test
  (with-compute (compute)
    (let [[^AshtreeFuture fut ^AshtreeFuture another] (into [] (repeatedly 2 (partial invoke long-fn :async true)))]
      (is (instance? AshtreeFuture fut))
      (is (instance? ComputeTaskFuture fut))
      (is (instance? IDeref fut))
      (is (instance? IPending fut))
      (is (instance? IBlockingDeref fut))
      (is (not (.isRealized fut)))
      (is (not (.isCancelled fut)))
      (is (not (.isDone fut)))
      (is (instance? ComputeTaskSession (.getTaskSession fut)))
      (.cancel another)
      (is (= :timeouted (deref fut 1 :timeouted)))
      (is (thrown? IgniteFutureTimeoutException (.get fut 1)))
      (is (thrown? IgniteFutureTimeoutException (.get fut 1 TimeUnit/MILLISECONDS)))
      (is (= :ok @fut))
      (is (= :ok (.get fut)))
      (is (.isRealized fut))
      (is (.isDone fut))
      (is (.isCancelled another))
      (is (thrown? IgniteFutureCancelledException @another)))))

(deftest future-chaining-test
  (with-compute (compute)
    (testing "chaining single function"
      (let [^AshtreeFuture fut (invoke echo :args ["Hello"] :async true)
            x (c/chain fut clojure.string/lower-case)
            y (c/chain x clojure.string/reverse)]
        (is (instance? AshtreeFuture x))
        (is (instance? AshtreeFuture y))
        (is (= "hello" @x))
        (is (= "olleh" @y))))
    (testing "chaining multiple functions"
      (let [^AshtreeFuture fut (invoke echo :args ["Hello"] :async true)]
        (is (= "olleh" @(c/chain fut clojure.string/lower-case clojure.string/reverse)))))))

(deftest with-compute-test
  (testing "use with-compute instance"
    (with-compute (compute)
      (is (= "echo" (invoke echo :args ["echo"])))
      (is (= ["echo" "echo"] (invoke echo :args ["echo"] :broadcast :true)))))
  (testing "override with-compute"
    (with-compute (compute)
      (let [cluster-group (ignite/cluster *ignite-instance* :remote)
            random-instance (ignite/compute *ignite-instance* :cluster cluster-group)]
        (is (= ["random"] (invoke echo :args ["random"] :broadcast true :compute random-instance)))))))

(deftest no-compute-fails-test
  (is (thrown? IllegalArgumentException (invoke identity :args "fails"))))

(deftest invoke-test
  (with-compute (compute)
    (testing "invoke with function"
      (is (= "echo" (invoke echo :args ["echo"])))
      (is (= "no-arg echo" (invoke (partial echo "no-arg echo")))))
    (testing "invoke with symbol"
      (is (= "echo" (invoke 'vermilionsands.ashtree.util.functions/echo :args ["echo"]))))
    (testing "invoke with sfn"
      (is (= "echo" (invoke (sfn [x] x) :args ["echo"]))))))

(deftest invoke-with-broadcast-test
  (with-compute (compute)
    (let [result (invoke functions/get-node-id :args [*ignite-instance*] :broadcast true)]
      (is (vector? result))
      (is (= 2 (count (set result)))))))

(deftest invoke-with-multiple-tasks-test
  (with-compute (compute)
    (testing "multiple tasks without args"
      (let [result (invoke (partial echo "task1") (partial echo "task2"))]
        (is (vector? result))
        (is (= #{"task1" "task2"} (set result)))))
    (testing "multiple tasks with args"
      (is (= #{"task1" "task2"} (set (invoke echo echo :args ["task1"] ["task2"])))))
    (testing "mixed multiple tasks"
      (is (= #{"task1" "task2"} (set (invoke echo (partial echo "task2") :args ["task1"] nil)))))))

(deftest async-invoke-option-test
  (with-compute (compute)
    (testing "async invoke"
      (let [result (invoke echo :async true :args ["future"])]
        (is (instance? AshtreeFuture result))
        (is (= "future" @result))))
    (testing "async broadcast"
      (let [result (invoke echo :async true :args ["future"] :broadcast true)]
        (is (instance? AshtreeFuture result))
        (is (vector? @result))
        (is (= ["future" "future"] @result))))
    (testing "async invoke with multiple tasks"
      (let [result (invoke echo echo :async true :args ["future"] ["future"])]
        (is (instance? AshtreeFuture result))
        (is (vector? @result))
        (is (= ["future" "future"] @result))))))

(deftest timeout-option-test
  (with-compute (compute)
    (testing "timeout without async"
      (is (thrown? ComputeTaskTimeoutException
                   (invoke long-fn :timeout 100))))
    (testing "timeout with async"
      (is (thrown? ComputeTaskTimeoutException
                   @(invoke long-fn :timeout 100 :async true))))
    (testing "timeout-val"
      (is (= :still-ok (invoke long-fn :timeout 100 :timeout-val :still-ok)))
      (is (= :still-ok @(invoke long-fn :timeout 100 :timeout-val :still-ok :async true))))
    (testing "nil timeout-val"
      (is (nil? (invoke long-fn :timeout 100 :timeout-val nil))))))

(deftest task-name-option-test
  (with-compute (compute)
    (let [result (invoke long-fn :async true :name "test-task")]
      (is (= "test-task"
             (.getTaskName (.getTaskSession ^ComputeTaskFuture (.future result)))))
      (.cancel result))))

(deftest reduce-option-test
  (with-compute (compute)
    (testing "sync reduce"
      (is (= "test-echoecho" (invoke echo echo :args ["echo"] ["echo"] :reduce str :reduce-init "test-")))
      (is (= "echoecho" (invoke echo echo :args ["echo"] ["echo"] :reduce str))))
    (testing "async reduce"
      (is (= "test-echoecho" @(invoke echo echo :args ["echo"] ["echo"] :reduce str :reduce-init "test-" :async true)))
      (is (= "echoecho" @(invoke echo echo :args ["echo"] ["echo"] :reduce str :async true))))))

(deftest failover-option-test
  (with-compute (compute)
    (testing "failover is set on compute"
      (let [f #'c/compute-instance
            compute (f c/*compute* nil nil true)
            ctx (fixtures/get-private-field compute "ctx")]
        (is (true? (.getThreadContext (.task ^GridKernalContext ctx) GridTaskThreadContextKey/TC_NO_FAILOVER)))))
    (testing "compute-for-task is called with proper flag"
      (let [state (atom nil)]
        (with-redefs [c/compute-instance (fn [c _ _ failover] (swap! state (fn [_] failover)) c)]
          (invoke identity :args [true])
          (is (nil? @state))
          (invoke identity :args [true] :no-failover true)
          (is (true? @state)))))))

(deftest affinity-option-test
  (with-compute (compute)
    (let [fst-name "fst-affinity-cache"
          snd-name "snd-affinity-cache"
          fst-key :test-key-1
          snd-key :test-key-2
          fst-val "test-val-1"
          snd-val "test-val-2"
          fst-cache (.getOrCreateCache ^Ignite *ignite-instance* fst-name)
          snd-cache (.getOrCreateCache ^Ignite *ignite-instance* snd-name)]
      (.put ^IgniteCache fst-cache fst-key fst-val)
      (.put ^IgniteCache snd-cache snd-key snd-val)
      (testing "Sync affinity call"
        (is (every? #(= % fst-val)
                    (for [_ (range 10)]
                      (invoke functions/cache-peek
                              :args [fst-cache fst-key]
                              :affinity-cache fst-name
                              :affinity-key fst-key))))
        (is (every? #(= % snd-val)
                    (for [_ (range 10)]
                      (invoke functions/cache-peek
                              :args [snd-cache snd-key]
                              :affinity-cache [fst-name snd-name]
                              :affinity-key snd-key)))))
      (testing "Async affinity call"
        (is (every? #(= % fst-val)
                    (map deref
                      (for [_ (range 10)]
                        (invoke functions/cache-peek
                                :args [fst-cache fst-key]
                                :affinity-cache fst-name
                                :affinity-key fst-key
                                :async true)))))
        (is (every? #(= % snd-val)
                    (map deref
                      (for [_ (range 10)]
                        (invoke functions/cache-peek
                                :args [snd-cache snd-key]
                                :affinity-cache [fst-name snd-name]
                                :affinity-key snd-key
                                :async true)))))))))

(deftest per-node-shared-state-test
  ;; call on youngest 2 times
  (with-compute (ignite/compute *ignite-instance* :cluster (ignite/cluster *ignite-instance* :youngest))
    (invoke functions/inc-node-state :args [*ignite-instance*])
    (is (= 2 (invoke functions/inc-node-state :args [*ignite-instance*]))))
  ;; call on oldest once
  (with-compute (ignite/compute *ignite-instance* :cluster (ignite/cluster *ignite-instance* :oldest))
    (is (= 1 (invoke functions/inc-node-state :args [*ignite-instance*])))))

(deftest incorrect-configuration-test
  (let [validate-opts #'compute/validate-opts]
    (is (thrown? IllegalArgumentException (validate-opts {})))
    (is (thrown? IllegalArgumentException (validate-opts {:tasks [+] :args [[1 2] [2 3]]})))
    (is (thrown? IllegalArgumentException (validate-opts {:tasks [+] :reduce +})))
    (is (thrown? IllegalArgumentException (validate-opts {:tasks [+ +] :broadcast true})))
    (is (thrown? IllegalArgumentException (validate-opts {:tasks [+] :affinity-cache "cache" :broadcast true})))
    (is (thrown? IllegalArgumentException (validate-opts {:tasks [+] :affinity-cache "cache"})))
    (is (thrown? IllegalArgumentException (validate-opts {:tasks [+] :affinity-key "key"})))))

(deftest finvoke-test
  (with-compute (compute)
    (let [result (compute/finvoke echo :args ["future"])]
      (is (instance? IgniteFuture result))
      (is (= "future" @result)))))

(deftest distribute-test
  (testing "distribute without compute"
    (let [f (compute/distribute echo)
          g (compute/distribute 'vermilionsands.ashtree.util.functions/echo)
          h (compute/distribute (sfn [x] x))]
      (is (thrown? IllegalArgumentException (f "echo")))
      (with-compute (compute)
        (is (= "echo" (f "echo")))
        (is (= "echo" (g "echo")))
        (is (= "echo" (h "echo"))))))
  (testing "distribute with with-compute retains compute instance"
    (let [f (with-compute (compute) (compute/distribute echo))]
      (is (= "echo" (f "echo")))))
  (testing "distribute with :compute"
    (let [f (compute/distribute echo :compute (compute))]
      (is (= "echo" (f "echo"))))))

(deftest fdistribute-test
  (let [f (compute/fdistribute echo :compute (compute))
        result (f "future")]
    (is (instance? IgniteFuture result))
    (is (= "future" @result))))

(deftest invoke*-test
  (with-compute (compute)
    (is (= "echo" (compute/invoke* {:tasks [echo] :args [["echo"]]})))
    (is (= "echo" @(compute/invoke* echo {:args [["echo"]] :async true})))))