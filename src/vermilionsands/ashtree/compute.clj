(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite])
  (:import [clojure.lang IDeref IMeta IBlockingDeref IPending]
           [java.util Collection]
           [java.util.concurrent TimeUnit TimeoutException]
           [org.apache.ignite IgniteCompute Ignite]
           [org.apache.ignite.lang IgniteCallable IgniteFuture IgniteReducer])
  (:gen-class))

(def ^:dynamic *compute*
  "Compute API instance to be used with with-compute"
  nil)

(def ^:dynamic *callable-eval*
  "Eval function that would be used by IgniteCallable wrapper for serializable functions.

  By default it would keep 100 elements using LRU memoization."
  (memoize/lru eval :lru/threshold 100))

(deftype AshtreeCallable [f args]
  IgniteCallable
  (call [_]
    (apply f args))

  IMeta
  (meta [_] (meta f)))

(deftype AshtreeReducer [f state]
   IgniteReducer
   (collect [_ x]
     (swap! state f x)
     true)

   (reduce [_]
     @state))

(deftype AshtreeFuture [^IgniteFuture future]
  IDeref
  (deref [_]
    (.get future))

  IBlockingDeref
  (deref [_ timeout-ms timeout-val]
    (try
      (.get future timeout-ms TimeUnit/MILLISECONDS)
      (catch TimeoutException _ timeout-val)))

  IPending
  (isRealized [_]
    (.isDone future))

  IgniteFuture
  (cancel [_]
    (.cancel future))

  (chain [_ on-done]
    (AshtreeFuture. (.chain future on-done)))

  (chainAsync [_ on-done exec]
    (AshtreeFuture. (.chainAsync future on-done exec)))

  (get [_]
    (.get future))

  (get [_ timeout]
    (.get future timeout))

  (get [_ timeout unit]
    (.get future timeout unit))

  (isCancelled [_]
    (.isCancelled future))

  (isDone [_]
    (.isDone future))

  (listen [_ listener]
    (.listen future listener))

  (listenAsync [_ listener exec]
    (.listenAsync future listener exec)))

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

(defn- callable? [task]
  (instance? IgniteCallable task))

(defn callable
  "Create an IgniteCallable instance for a task.

  Accepts either a standard clojure function or a serializable function (from function namespace), which would be
  stored as data and evaled on call, or a fully qualified symbol that would be resolved on call.

  Args:
  task - clojure function, serializable function or fully qualified symbol
  args - argument vector for task, can be nil, or empty if task is a no-arg function"
  [task args]
  (cond
    (callable? task)              task
    (function/serializable? task) (->AshtreeCallable (eval-fn (function/eval-form task)) args)
    (symbol? task)                (->AshtreeCallable (symbol-fn task) args)
    (fn? task)                    (->AshtreeCallable task args)
    :else (throw (IllegalArgumentException. (format "Don't know how to create IgniteCallable from %s" task)))))

(defn- reducer? [task]
  (instance? IgniteReducer task))

(defn reducer
  "Create an IgniteReducer instance for a task. See callable for acceptable tasks.

  Args:
  task - underlying function should accept two arguments - accumulator and x

  Optional:
  init-value - initial value for state, otherwise state would be set to nil"
  [task & [init-value]]
  (let [state (atom init-value)]
    (cond
      (reducer? task)               task
      (function/serializable? task) (->AshtreeReducer (eval-fn (function/eval-form task)) state)
      (symbol? task)                (->AshtreeReducer (symbol-fn task) state)
      (fn? task)                    (->AshtreeReducer task state)
      :else (throw (IllegalArgumentException. (format "Don't know how to create IgniteReducer from %s" task))))))

(defn- compute-for-task [compute name timeout no-failover]
  (cond-> ^IgniteCompute compute
    timeout     (.withTimeout timeout)
    no-failover (.withNoFailover)
    name        (.withName name)))

(defn- parse-args [task opts-seq]
  (let [xs (cons :tasks (cons task opts-seq))
        xf (comp (partition-by keyword?) (partition-all 2) (map #(apply concat %)))]
    (reduce (fn [m [k & v]] (assoc m k (vec v))) {} (sequence xf xs))))

(defn- validate-opts [opts-map]
  (let [{:keys [tasks args]} opts-map]
    (when-not (or (= (count tasks) (count args))
                  (empty? args))
      (throw (IllegalArgumentException.
               (format "Count of tasks (%s) does not match with count of args vectors (%s)"
                       (count tasks) (count args)))))))

(defn- compute-method [{:keys [tasks async broadcast affinity-cache affinity-key reduce reduce-init]}]
  (let [[_ multiple?] tasks
        caches? (coll? affinity-cache)
        part-id? (and caches? (number? affinity-key))
        aff? affinity-cache]
    (cond
      async
      (cond
        broadcast #(.broadcastAsync ^IgniteCompute %1 ^IgniteCallable %2)
        reduce    #(.callAsync ^IgniteCompute %1 ^Collection %2 ^IgniteReducer (reducer reduce reduce-init))
        multiple? #(.callAsync ^IgniteCompute %1 ^Collection %2)
        part-id?  #(.affinityCallAsync ^IgniteCompute %1 ^Collection affinity-cache ^int (int affinity-key) ^IgniteCallable %2)
        caches?   #(.affinityCallAsync ^IgniteCompute %1 ^Collection affinity-cache ^Object affinity-key ^IgniteCallable %2)
        aff?      #(.affinityCallAsync ^IgniteCompute %1 ^String affinity-cache ^Object affinity-key ^IgniteCallable %2)
        :else     #(.callAsync ^IgniteCompute %1 ^IgniteCallable %2))

      :else
      (cond
        broadcast #(.broadcast ^IgniteCompute %1 ^IgniteCallable %2)
        reduce    #(.call ^IgniteCompute %1 ^Collection %2 ^IgniteReducer (reducer reduce reduce-init))
        multiple? #(.call ^IgniteCompute %1 ^Collection %2)
        part-id?  #(.affinityCall ^IgniteCompute %1 ^Collection affinity-cache ^int (int affinity-key) ^IgniteCallable %2)
        caches?   #(.affinityCall ^IgniteCompute %1 ^Collection affinity-cache ^Object affinity-key ^IgniteCallable %2)
        aff?      #(.affinityCall ^IgniteCompute %1 ^String affinity-cache ^Object affinity-key ^IgniteCallable %2)
        :else     #(.call ^IgniteCompute %1 ^IgniteCallable %2)))))

(defn invoke-map [opts-map]
  (let [{:keys [tasks args compute async name timeout no-failover]} opts-map
        _ (validate-opts opts-map)
        callable (cond->
                   (mapv callable tasks (or args (repeat nil)))
                   (nil? (second tasks)) first)
        compute (some->
                  (or compute *compute*)
                  (compute-for-task name timeout no-failover))
        f (compute-method opts-map)]
    (when-not compute (throw (IllegalArgumentException. "No compute API instance!")))
    (cond-> (f compute callable) async (->AshtreeFuture))))

(defn invoke
  "Execute a task on a cluster. By default it uses ignite/*compute* as compute instance.

  A task can be one of the following:
  * clojure function       - has to be available on both caller and executing node, should be AOT compiled
  * serializable function  - from function namespace. It would be passed as data, and evaled on executing node
                             (this is EXPERIMENTAL!!!)
  * symbol                 - preferably fully qualified, it would be resolved to a function. Only has to be valid
                             on executing node

  Returns a function's return value or a future if :async true is passed as one of the options.
  Returned future is an IgniteFuture and can be derefed like standard future.

  <fix samples and rest>

  Args:
  task - task to execute

  Optional:
  task+args+opts - arguments to task and options.

  Options:
  :args
  :broadcast
  :compute
  :async          - enable async execution if true
  :timeout        - timeout, after which ComputeTaskTimeoutException would be returned, in milliseconds
  :no-failover    - execute with no failover mode if true
  :name           - name for this task
  :affinity-cache - cache name(s) for affinity call
  :affinity-key   - affinity key or partition id
  :reduce         - if provided it would be called on the results reducing them into a single value.
                    It should accept 2 arguments (state, x) and should follow the same rules as task.
  :reduce-init    - initial state of reducer state"
  [task & tasks+args+opts]
  (try
    (invoke-map (parse-args task tasks+args+opts))
    (catch Exception e
      (throw
        (IllegalArgumentException.
          (format "Failed to parse args %s with %s" [task tasks+args+opts] e))))))

(defn distribute
  "Returns a function which, when called, would be executed in a distributed fashion."
  [task & opts]
  (let [opts-map (parse-args task opts)
        updated-opts (update opts-map :compute #(or % *compute*))]
    (with-meta
      (fn [& args] (invoke-map (assoc updated-opts :args [args])))
      (merge
        (meta task)
        {::opts-map updated-opts}))))

(defn fdistribute
  [task & opts]
  (apply distribute task (conj (vec opts) :async true)))

(defmacro with-compute
  "Evaluates body in a context in which *compute* is bound to a given compute API instance"
  [compute & body]
  `(binding [*compute* ~compute]
     ~@body))