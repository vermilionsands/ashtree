(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function])
  (:import [org.apache.ignite.lang IgniteCallable]
           [org.apache.ignite IgniteCompute Ignite]
           [java.util Collection])
  (:gen-class))

(def ^:dynamic *callable-eval*
  (memoize/lru eval :lru/threshold 100))

(deftype IgniteFn [f]
  IgniteCallable
  (call [_] (f)))

(deftype EvalIgniteFn [fn-form]
  IgniteCallable
  (call [_] (or *callable-eval* eval) fn-form))

(defn symbol-fn
  [sym & args]
  (fn []
    (let [f-var (resolve sym)]
      (when-not f-var
        (throw (IllegalArgumentException. (format "Cannot resolve %s to a var!" sym))))
      (apply @f-var args))))

(defn ignite-callable
  [f]
  (cond
    (function/serializable? f) (->EvalIgniteFn (function/eval-form f))
    (fn? f)                    (->IgniteFn f)
    :else (throw (IllegalArgumentException. (format "Don't know how to create IgniteCallable from %s" f)))))

(defn- task-compute [compute name timeout no-failover]
  (cond-> compute
    timeout     (.withTimeout timeout)
    no-failover (.withNoFailover)
    name        (.withName name)))

;; todo - consider generalizing this code
;; and implementing broadcast in terms of call/reusing logic
;; and add version using compute from context

(defn call
  [^IgniteCompute compute f & [{:keys [async name timeout no-failover]}]]
  (let [task ^IgniteCallable (ignite-callable f)
        compute ^IgniteCompute (task-compute compute name timeout no-failover)]
    (cond
      async (.callAsync compute task)
      :else (.call compute task))))

(defn map-call
  [^IgniteCompute compute fs & [{:keys [async name timeout no-failover]}]]
  (let [task ^Collection (mapv ignite-callable fs)
        compute ^IgniteCompute (task-compute compute name timeout no-failover)]
    (cond
      async (.callAsync compute task)
      :else (.call compute task))))

(defn broadcast
  [^IgniteCompute compute f & [{:keys [async name timeout no-failover]}]]
  (let [task (ignite-callable f)
        compute (task-compute compute name timeout no-failover)]
    (cond
      async (.broadcastAsync compute task)
      :else (.broadcast compute task))))