(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite])
  (:import [clojure.lang IDeref IMeta]
           [java.util Collection]
           [org.apache.ignite.lang IgniteCallable IgniteFuture IgniteReducer]
           [org.apache.ignite IgniteCompute Ignite])
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

(defn- distributed-invoke [compute task opts sync-fn async-fn]
  (let [{:keys [:async :name :timeout :no-failover]} opts
        compute (some->
                  (or compute *compute*)
                  (compute-for-task name timeout no-failover))]
    (when-not compute
      (throw (IllegalArgumentException. "No compute API instance!")))
    (cond
      async (->AshtreeFuture (async-fn compute task))
      :else (sync-fn compute task))))

(defn invoke
  "Execute a task on a cluster. By default it uses ignite/*compute* as compute instance.

  A task can be one of the following:
  * clojure function       - has to be available on both caller and executing node, should be AOT compiled
  * serializable function  - from function namespace. It would be passed as data, and evaled on executing node
                             (this is EXPERIMENTAL!!!)
  * symbol                 - preferably fully qualified, it would be resolved to a function, only has to be valid
                             on executing node

  Returns a function's return value or a future if :async true is passed as one of the options.
  Returned future is an IgniteFuture and can be derefed like standard future.

  (invoke f :args [x1 x2 ...] :opts {:async true})
  (invoke 'fully.qualified/symbol :args [x1 x2 ...] :opts {:timeout 1} :compute some-custom-compute)
  (invoke 'symbol :args [x1 x2 ...] :opts {:timeout 1}) ;; would try to convert symbol to a fully qualified one
  (invoke (functions/sfn [x y] (+ x y)) :args [1 2])

  Args:
  task - task to execute

  Optional:
  args+opts - arguments to task and options. Supports :args args-vector, :opts opts-map, :compute compute-instance

  Options:
  :async          - enable async execution if true
  :timeout        - timeout, after which ComputeTaskTimeoutException would be returned, in milliseconds
  :no-failover    - execute with no failover mode if true
  :name           - name for this task
  :affinity-cache - cache name(s) for affinity call
  :affinity-key   - affinity key or partition id"
  [task & args+opts]
  (let [{:keys [args opts compute]} (apply hash-map args+opts)
        {:keys [affinity-cache affinity-key]} opts
        [sync-fn async-fn]
        ;; it begs for a rework...
        (if-not affinity-cache
          [#(.call      ^IgniteCompute %1 ^IgniteCallable %2)
           #(.callAsync ^IgniteCompute %1 ^IgniteCallable %2)]
          (cond
            (and (coll? affinity-cache) (number? affinity-key))
            [#(.affinityCall      ^IgniteCompute %1 ^Collection affinity-cache ^int (int affinity-key) ^IgniteCallable %2)
             #(.affinityCallAsync ^IgniteCompute %1 ^Collection affinity-cache ^int (int affinity-key) ^IgniteCallable %2)]
            (coll? affinity-cache)
            [#(.affinityCall      ^IgniteCompute %1 ^Collection affinity-cache ^Object affinity-key ^IgniteCallable %2)
             #(.affinityCallAsync ^IgniteCompute %1 ^Collection affinity-cache ^Object affinity-key ^IgniteCallable %2)]
            :else
            [#(.affinityCall      ^IgniteCompute %1 ^String affinity-cache ^Object affinity-key ^IgniteCallable %2)
             #(.affinityCallAsync ^IgniteCompute %1 ^String affinity-cache ^Object affinity-key ^IgniteCallable %2)]))]
    ;; push callable to distributed invoke
    (distributed-invoke compute (callable task args) opts sync-fn async-fn)))

(defn invoke-seq
  "Executes a seq of tasks on a cluster, splitting them across cluster. By default it uses ignite/*compute* as compute
  instance. Returns a collection of results or a future if :async true is passed as one of the options.

  (invoke-seq [f1 f2 ...] :args [vec1 vec2 ...] :opts {:async true} :reducer reducer-foo :reducer-init init-value)

  Args:
  tasks - sequence of tasks

  Optional:
  args+opts - arguments to tasks and options. See invoke for details. Args should be passed as :args vector-of-args-vectors
              For args first args vector would be applied to first task, second to second and so on. If a task is a
              no-arg then it's args can be either nil or [].

  Additional options:
  :reduce      - if provided it would be called on the results reducing them into a single value.
                It should accept 2 arguments (state, x) and should follow the same rules as task.
  :reduce-init - initial state of reducer state

  See invoke documentation for more details about tasks and options. invoke-seq does not support affinity."
  [tasks & args+opts]
  (let [{:keys [args opts compute]} (apply hash-map args+opts)
        {:keys [reduce reduce-init]} opts
        tasks (mapv callable tasks (or args (repeat nil)))
        [sync-fn async-fn]
        (if reduce
          [#(.call      ^IgniteCompute %1 ^Collection %2 ^IgniteReducer (reducer reduce reduce-init))
           #(.callAsync ^IgniteCompute %1 ^Collection %2 ^IgniteReducer (reducer reduce reduce-init))]
          [#(.call      ^IgniteCompute %1 ^Collection %2)
           #(.callAsync ^IgniteCompute %1 ^Collection %2)])]
    (distributed-invoke compute tasks opts sync-fn async-fn)))

(defn broadcast
  "Execute a task on all nodes in a cluster. By default it uses ignite/*compute* as compute instance.
  Returns a collection of results or a future if :async true is passed as one of the options.

  (broadcast f :args [x1 x2] :opts {:async true})

  See invoke documentation for more details about tasks and options. broadcast does not support affinity."
  [task & args+opts]
  (let [{:keys [args opts compute]} (apply hash-map args+opts)]
    (distributed-invoke compute (callable task args) opts
      #(.broadcast      ^IgniteCompute %1 ^IgniteCallable %2)
      #(.broadcastAsync ^IgniteCompute %1 ^IgniteCallable %2))))

(defmacro with-compute
  "Evaluates body in a context in which *compute* is bound to a given compute API instance"
  [compute & body]
  `(binding [*compute* ~compute]
     ~@body))