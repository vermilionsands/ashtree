(ns vermilionsands.ashtree.function
  (:require [clojure.core.memoize :as memoize])
  (:import [clojure.lang Compiler$LocalBinding]))

(def ^:dynamic *callable-eval*
  "Eval function that would be used by IgniteCallable wrapper for serializable functions.

  By default it would keep 100 elements using LRU memoization."
  (memoize/lru eval :lru/threshold 100))

(defn eval-fn [form]
  (with-meta
    (fn [& args]
      (let [f ((or *callable-eval* eval) form)]
        (apply f args)))
    (meta form)))

(defn symbol-fn
  "Returns a function that tries to resolve a symbol sym to a var and calls it with supplied args.

  Args:
  sym  - symbol, if it is not fully qualified symbol-fn would try to resolve it *as is* and create a
         fully qualified version
  args - optional args that would be applied to resolved function"
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

;; todo
;; parts of this logic seem to be useless in this usecase
;; maybe replace with the original serializable-fn or the one used in flambo?

(defn- generate-name [namespace line column fn-name]
  (let [ns-string (name namespace)]
    (if fn-name
      (symbol ns-string (name fn-name))
      (let [ns-name (last (clojure.string/split ns-string #"\."))
            fn-name (str \_ ns-name \- line \- column)]
        (symbol ns-string fn-name)))))

(defn used-symbols [form]
  (->> form
       (tree-seq coll? seq)
       (filter symbol?)
       (set)))

;; todo consider replacing java objects (for example arrays etc)
;; with some more friendly
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
  (let [namespace (ns-name *ns*)
        form &form
        {:keys [line column]} (meta &form)
        fn-name (->> (-> body first meta ::name)
                     (generate-name namespace line column))
        local-bindings (bindings (vals &env) (used-symbols (rest body)))]
    `(with-meta
       (fn ~@body)
       {:type ::serializable-fn
        ::name     '~fn-name
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
        meta' (dissoc (meta f) ::form ::bindings ::name :type)]
    (with-meta
      `(~'let [~@bindings] ~form)
      meta')))

(defn serializable? [x]
  (= (type x) ::serializable-fn))