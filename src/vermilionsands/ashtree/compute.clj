(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function])
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

(deftype AshtreeFuture [^IgniteFuture ignite-future]
  IDeref
  (deref [_]
    (.get ignite-future)))

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
  [^IgniteCompute compute f & [{:keys [async name timeout no-failover] :as opts-map}]]
  (distributed-invoke compute (ignite-callable f) opts-map call-fn acall-fn))

(defn map-call
  [^IgniteCompute compute fs & [{:keys [async name timeout no-failover] :as opts-map}]]
  (distributed-invoke compute (mapv ignite-callable fs) opts-map call-coll-fn acall-coll-fn))

(defn broadcast
  [^IgniteCompute compute f & [{:keys [async name timeout no-failover] :as opts-map}]]
  (distributed-invoke compute (ignite-callable f) opts-map broadcast-fn abroadcast-fn))