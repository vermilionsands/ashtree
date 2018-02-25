(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function])
  (:import [org.apache.ignite.lang IgniteCallable]
           [org.apache.ignite IgniteCompute Ignite]
           [java.util Collection])
  (:gen-class))

(def ^:dynamic *callable-eval*
  (memoize/lru eval :lru/threshold 100))

(deftype IgniteFn [f args]
  IgniteCallable
  (call [_]
    (apply f args)))

(deftype EvalIgniteFn [fn-form args]
  IgniteCallable
  (call [_]
    (apply (or *callable-eval* eval) fn-form) args))

(deftype ResolveIgniteFn [sym args]
  IgniteCallable
  (call [_]
    (let [f-var (resolve sym)]
      (when-not f-var
        (throw (IllegalArgumentException. (format "Cannot resolve %s to a var!" sym))))
      (apply @f-var args))))

(defn ignite-callable [f args]
  (cond
    (= (type f) ::function/serializable-fn)
    (->EvalIgniteFn (function/eval-form f) args)

    (list? f)
    (->EvalIgniteFn f args)

    (symbol? f)
    (->ResolveIgniteFn f args)

    :else (->IgniteFn f args)))

(defn call
  [^IgniteCompute compute f & args]
  (.call compute ^IgniteCallable (ignite-callable f args)))

(defn call-for
  [^IgniteCompute compute f & args-seqs]
  (.call compute ^Collection (mapv #(ignite-callable f %) args-seqs)))

(defn call-many
  [^IgniteCompute compute & f-and-args-seq]
  (.call compute ^Collection (mapv (fn [[f & args]] (ignite-callable f args)) f-and-args-seq)))

(defn broadcast
  [^IgniteCompute compute f & args]
  (.broadcast compute ^IgniteCallable (ignite-callable f args)))