(ns vermilionsands.ashtree.compute
  (:import [org.apache.ignite.lang IgniteCallable]
           [org.apache.ignite IgniteCompute]
           [java.util Collection])
  (:gen-class))

(deftype IgniteCallableWrapper [f args]
  IgniteCallable
  (call [_]
    (apply f args)))

(defn apply-fn
  [^IgniteCompute compute f & args]
  (.call compute ^IgniteCallable (->IgniteCallableWrapper f args)))

(defn apply-fns
  [^IgniteCompute compute & f-and-args-seq]
  (.call compute ^Collection (mapv (fn [[f & args]] (->IgniteCallableWrapper f args)) f-and-args-seq)))

(defn apply-seq
  [^IgniteCompute compute f & args-seqs]
  (.call compute ^Collection (mapv #(->IgniteCallableWrapper f %) args-seqs)))
