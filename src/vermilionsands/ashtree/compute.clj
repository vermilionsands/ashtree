(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function])
  (:import [org.apache.ignite.lang IgniteCallable]
           [org.apache.ignite IgniteCompute Ignite]
           [java.util Collection])
  (:gen-class))

;;todo maybe make this configurable
(def ^:private caching-eval (memoize/lru eval :lru/threshold 100))

(deftype IgniteCallableWrapper [f args]
  IgniteCallable
  (call [_]
    (apply f args)))

(deftype EvalIgniteCallableWrapper [fn-form args]
  IgniteCallable
  (call [_]
    (apply (caching-eval fn-form) args)))

(deftype SymbolIgniteCallableWrapper [sym args]
  IgniteCallable
  (call [_]
    (let [f-var (resolve sym)]
      (when-not f-var
        (throw (IllegalArgumentException. (format "Cannot resolve %s to function!" sym))))
      (apply @f-var args))))

(defn ignite-callable [f args]
  (cond
    (= (type f) ::function/serializable-fn)
    (->EvalIgniteCallableWrapper (function/eval-form f) args)

    (list? f)
    (->EvalIgniteCallableWrapper f args)

    (symbol? f)
    (->SymbolIgniteCallableWrapper f args)

    :else (->IgniteCallableWrapper f args)))

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