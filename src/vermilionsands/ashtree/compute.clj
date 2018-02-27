(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite])
  (:import [clojure.lang IDeref]
           [java.util Collection]
           [org.apache.ignite.lang IgniteCallable IgniteFuture]
           [org.apache.ignite IgniteCompute Ignite])
  (:gen-class))

(def ^:dynamic *callable-eval*
  "Eval function that would be used by IgniteCallable wrapper for serializable functions.

  By default it would keep 100 elements using LRU memoization."
  (memoize/lru eval :lru/threshold 100))

(deftype IgniteFn [f]
  IgniteCallable
  (call [_] (f)))

(deftype EvalIgniteFn [fn-form]
  IgniteCallable
  (call [_]
    (let [f ((or *callable-eval* eval) fn-form)]
      (f))))

(deftype AshtreeFuture [^IgniteFuture future]
  IDeref
  (deref [_]
    (.get future))

  IgniteFuture
  (cancel [_]
    (.cancel future))

  (chain [_ on-done]
    (.chain future on-done))

  (chainAsync [_ on-done exec]
    (.chainAsync future on-done exec))

  (get [_]
    (.get future))

  (get [_ timeout]
    (.get future timeout))

  (get [_ timeout unit]
    (.get future timeout unit))

  (isCancelled [_]
    (.isCancelled future))

  (isDone [_]
    (.isDone future))

  (listen [_ listener]
    (.listen future listener))

  (listenAsync [_ listener exec]
    (.listenAsync future listener exec)))

(defn symbol-fn
  "Returns a function that tries to resolve a symbol sym to a var and calls it with supplied args.

  Args:
  sym  - fully qualified symbol
  args - optional args that would be applied to resolved function"
  [sym & args]
  (fn []
    (let [f-var (resolve sym)]
      (when-not f-var
        (throw (IllegalArgumentException. (format "Cannot resolve %s to a var!" sym))))
      (apply @f-var args))))

(defn ignite-callable
  "Create an IgniteCallable instance for a function f.

  Accepts either a standard clojure function or a serializable function (from function namespace), which would be
  stored as data and evaled on call.

  Args:
  f - no-arg function"
  [f]
  (cond
    (function/serializable? f) (->EvalIgniteFn (function/eval-form f))
    (fn? f)                    (->IgniteFn f)
    :else (throw (IllegalArgumentException. (format "Don't know how to create IgniteCallable from %s" f)))))

(defn- task-compute [compute name timeout no-failover]
  (cond-> ^IgniteCompute compute
    timeout     (.withTimeout timeout)
    no-failover (.withNoFailover)
    name        (.withName name)))

(def ^:private call-fn       #(.call ^IgniteCompute %1 ^IgniteCallable %2))
(def ^:private acall-fn      #(.callAsync ^IgniteCompute %1 ^IgniteCallable %2))
(def ^:private call-coll-fn  #(.call ^IgniteCompute %1 ^Collection %2))
(def ^:private acall-coll-fn #(.callAsync ^IgniteCompute %1 ^Collection %2))
(def ^:private broadcast-fn  #(.broadcast ^IgniteCompute %1 ^IgniteCallable %2))
(def ^:private abroadcast-fn #(.broadcastAsync ^IgniteCompute %1 ^IgniteCallable %2))

(defn- distributed-invoke [compute task opts-map sync-fn async-fn]
  (let [{:keys [async name timeout no-failover]} opts-map
        compute ^IgniteCompute (task-compute compute name timeout no-failover)]
    (cond
      async (->AshtreeFuture (async-fn compute task))
      :else (sync-fn compute task))))

(defn call
  "Execute a function on a cluster using compute API instance.

  Function can be one of the following:
  * clojure function - has to be available on both caller and target node, should be AOT compiled
  * serializable function - from function namespace, would be passed as data, and evaled on target node (EXPERIMENTAL!!!)
  * fully qualified symbol - would be resolved to a function, only has to be valid on target node

  Returns a function return value or an future if :async true is passed as one of the options.
  Returned future is an IgniteFuture and can be derefed like standard future.

  Args:
  compute  - compute API instance
  f        - function to execute
  opts-map - options map

  Options:
  :async       - enable async execution if true
  :timeout     - timeout, after which ComputeTaskTimeoutException would be returned, in milliseconds
  :no-failover - execute with no failover mode if true
  :name        - name for this task"

  [^IgniteCompute compute f & [{:keys [async name timeout no-failover] :as opts-map}]]
  (distributed-invoke compute (ignite-callable f) opts-map call-fn acall-fn))

(defn call*
  "See call. Uses ignite/*compute* as compute instance."
  [f & [opts-map]]
  (call ignite/*compute* f opts-map))

(defn map-call
  "Executes a collection of functions on a cluster using compute API instance, splitting them across cluster.
  Returns a collection of results or a future if :async true is passed as one of the options.

  Accepts a collection of functions instead of a single function.

  See call documentation for more details."
  [^IgniteCompute compute fs & [{:keys [async name timeout no-failover] :as opts-map}]]
  (distributed-invoke compute (mapv ignite-callable fs) opts-map call-coll-fn acall-coll-fn))

(defn map-call*
  "See map-call. Uses ignite/*compute* as compute instance."
  [fs & [opts-map]]
  (map-call ignite/*compute* fs opts-map))

(defn broadcast
  "Execute a function on all nodes in a cluster.
  Returns a collection of results or a future if :async true is passed as one of the options.

  See call documentation for more details."
  [^IgniteCompute compute f & [{:keys [async name timeout no-failover] :as opts-map}]]
  (distributed-invoke compute (ignite-callable f) opts-map broadcast-fn abroadcast-fn))

(defn broadcast*
  "See broadcast. Uses ignite/*compute* as compute instance."
  [f & [opts-map]]
  (broadcast ignite/*compute* f opts-map))