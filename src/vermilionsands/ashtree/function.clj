;;; serializable function is inspired by https://github.com/yieldbot/serializable-fn
;;; by Seajure, The Seattle Clojure group and contributors
(ns vermilionsands.ashtree.function
  (:require [clojure.core.memoize :as memoize])
  (:import [clojure.lang Compiler$LocalBinding]))

(def ^:dynamic *callable-eval*
  "Eval function that would be used by IgniteCallable wrapper for serializable functions.

  By default it would keep 100 elements using LRU memoization."
  (memoize/lru eval :lru/threshold 100))

(defn used-symbols [form]
  (->> form
       (tree-seq coll? seq)
       (filter symbol?)
       (set)))

;; todo consider replacing java objects (for example arrays etc)
;; with something more eval friendly
(defn- bindings [local-bindings used-symbols-set]
  (->>
    (filter
      (fn [^Compiler$LocalBinding b]
        (used-symbols-set (.sym b)))
      local-bindings)
    (mapcat
      (fn [^Compiler$LocalBinding b]
        [(list symbol (name (.sym b)))
         (.sym b)]))
    vec))

(defmacro sfn
  "Serializable function.

  Like fn, but stores it's form with local bindings in metadata, for further use in
  serialization and deserialization."
  [& body]
  (let [form &form
        local-bindings (bindings (vals &env) (used-symbols (rest body)))]
    `(with-meta
       (fn ~@body)
       {:type      ::serializable-fn
        ::form     '~form
        ::bindings ~local-bindings})))

(defmacro defsfn
  "Like defn, but serializable. See sfn."
  [name & body]
  (let [[x & xs] body
        x (with-meta x {::name name})]
    `(def ~name
       (sfn ~x ~@xs))))

(defn eval-form [f]
  (let [form (cons 'fn (rest (-> f meta ::form)))
        bindings (-> f meta ::bindings)
        meta' (dissoc (meta f) ::form ::bindings :type)]
    (with-meta
      `(~'let [~@bindings] ~form)
      meta')))

(defn serializable? [x]
  (= (type x) ::serializable-fn))

(defn eval-fn
  "Returns a varargs function that evaluates form and calls the resulting function with supplied args.

  Eval function can be specified using *callable-eval*"
  [form]
  (let [form (cond-> form (serializable? form) (eval-form))]
    (with-meta
      (fn [& args]
        (let [f ((or *callable-eval* eval) form)]
          (apply f args)))
      (meta form))))

(defn symbol-fn
  "Returns a varargs function that tries to resolve a symbol sym to a var and calls it with supplied args."
  [sym]
  (let [{:keys [name ns]} (-> sym resolve meta)
        sym (if name
              (symbol (str (ns-name ns)) (str name))
              sym)]
    (with-meta
      (fn [& args]
        (let [f-var (resolve sym)]
          (when-not f-var
            (throw (IllegalArgumentException. (format "Cannot resolve %s to a var!" sym))))
          (apply @f-var args)))
      (meta sym))))