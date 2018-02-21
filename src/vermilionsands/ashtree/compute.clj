(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function])
  (:import [org.apache.ignite.lang IgniteCallable]
           [org.apache.ignite IgniteCompute]
           [java.util Collection])
  (:gen-class))

;; maybe make this configurable
(def ^:private caching-eval (memoize/lru eval :lru/threshold 100))

(deftype IgniteCallableWrapper [f args]
  IgniteCallable
  (call [_]
    (apply f args)))

(deftype EvalIgniteCallableWrapper [fn-form args]
  IgniteCallable
  (call [_]
    (let [f (caching-eval fn-form)]
      (apply f args))))

(defn ignite-callable [f args]
  (cond
    (= (type f) ::function/serializable-fn)
    (->EvalIgniteCallableWrapper (function/eval-form f) args)

    (list? f)
    (->EvalIgniteCallableWrapper f args)

    :else (->IgniteCallableWrapper f args)))

(defn apply-fn
  [^IgniteCompute compute f & args]
  (.call compute ^IgniteCallable (ignite-callable f args)))

(defn apply-fns
  [^IgniteCompute compute & f-and-args-seq]
  (.call compute ^Collection (mapv (fn [[f & args]] (ignite-callable f args)) f-and-args-seq)))

(defn apply-seq
  [^IgniteCompute compute f & args-seqs]
  (.call compute ^Collection (mapv #(ignite-callable f %) args-seqs)))