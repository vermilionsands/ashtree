(ns vermilionsands.ashtree.compute
  (:import [org.apache.ignite.lang IgniteCallable]
           [org.apache.ignite IgniteCompute]
           [java.util Collection])
  (:gen-class))

(deftype IgniteCallableWrapper [f args]
  IgniteCallable
  (call [_]
    (apply f args)))

(deftype EvalIgniteCallableWrapper [f args]
  IgniteCallable
  (call [_]
    (let [g (eval f)]
      (apply g args))))

(defn apply-fn
  [^IgniteCompute compute f & args]
  (.call compute ^IgniteCallable (->IgniteCallableWrapper f args)))

(defn apply-form
  [^IgniteCompute compute fn-form & args]
  (.call compute ^IgniteCallable (->EvalIgniteCallableWrapper fn-form args)))

(defn apply-fns
  [^IgniteCompute compute & f-and-args-seq]
  (.call compute ^Collection (mapv (fn [[f & args]] (->IgniteCallableWrapper f args)) f-and-args-seq)))

(defn apply-forms
  [^IgniteCompute compute & fn-forms-and-args-seq]
 (.call compute ^Collection (mapv (fn [[fn-form & args]] (->EvalIgniteCallableWrapper fn-form args)) fn-forms-and-args-seq)))

(defn apply-seq
  [^IgniteCompute compute f & args-seqs]
  (.call compute ^Collection (mapv #(->IgniteCallableWrapper f %) args-seqs)))

(defn apply-form-seq
  [^IgniteCompute compute fn-form & args-seqs]
  (.call compute ^Collection (mapv #(->EvalIgniteCallableWrapper fn-form %) args-seqs)))
