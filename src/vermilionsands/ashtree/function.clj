(ns vermilionsands.ashtree.function
  (:import [clojure.lang Compiler$LocalBinding]))

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

;; todo consider replacing java objects (for example arrays etc) serializable/deserializable
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
       {::name     '~fn-name
        ::form     '~form
        ::bindings ~local-bindings})))

(defmacro defsfn
  "Like defn, but serializable. See sfn."
  [name & body]
  (let [[x & xs] body
        x (with-meta x {::name name})]
    `(def ~name
       (sfn ~x ~@xs))))