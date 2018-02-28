(ns vermilionsands.ashtree.compute
  (:require [clojure.core.memoize :as memoize]
            [vermilionsands.ashtree.function :as function]
            [vermilionsands.ashtree.ignite :as ignite])
  (:import [clojure.lang IDeref IMeta]
           [java.util Collection]
           [org.apache.ignite.lang IgniteCallable IgniteFuture IgniteReducer]
           [org.apache.ignite IgniteCompute Ignite])
  (:gen-class))

(def ^:dynamic *callable-eval*
  "Eval function that would be used by IgniteCallable wrapper for serializable functions.

  By default it would keep 100 elements using LRU memoization."
  (memoize/lru eval :lru/threshold 100))

(deftype IgniteFn [f args]
  IgniteCallable
  (call [_]
    (apply f args))

  IMeta
  (meta [_] (meta f)))

(deftype IgniteReducerFn [f state]
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
    (.chain future on-done))

  (chainAsync [_ on-done exec]
    (.chainAsync future on-done exec))

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
    {::opts (-> form meta ::opts)}))

(defn symbol-fn
  "Returns a function that tries to resolve a symbol sym to a var and calls it with supplied args.

  Args:
  sym  - fully qualified symbol
  args - optional args that would be applied to resolved function"
  [sym]
  (with-meta
    (fn [& args]
      (let [f-var (resolve sym)]
        (when-not f-var
          (throw (IllegalArgumentException. (format "Cannot resolve %s to a var!" sym))))
        (apply @f-var args)))
    {::opts (-> sym meta ::opts)}))

(defn callable
  "Create an IgniteCallable instance for a task.

  Accepts either a standard clojure function or a serializable function (from function namespace), which would be
  stored as data and evaled on call, or a fully qualified symbol that would be resolved on call.

  Args:
  task - clojure function, serializable function or fully qualified symbol
  args - argument vector for task, can be nil, or empty if task is a no-arg function"
  [task args]
  (cond
    (function/serializable? task) (->IgniteFn (eval-fn (function/eval-form task)) args)
    (symbol? task)                (->IgniteFn (symbol-fn task) args)
    (fn? task)                    (->IgniteFn task args)
    :else (throw (IllegalArgumentException. (format "Don't know how to create IgniteCallable from %s" task)))))

(defn with-opts
  "Append configuration options as meta to a given task.

  A task can be a function, a symbol or a collection that supports metadata.

  Options:
  :async       - enable async execution if true
  :timeout     - timeout, after which ComputeTaskTimeoutException would be returned, in milliseconds
  :no-failover - execute with no failover mode if true
  :name        - name for this task"
  [task & options]
  (let [{:keys [async name timeout no-failover]} (apply hash-map options)
        m (cond-> {}
                  async       (assoc ::async true)
                  name        (assoc ::name name)
                  timeout     (assoc ::timeout timeout)
                  no-failover (assoc ::no-failover true))]
    (vary-meta task assoc ::opts m)))

(defn- task-compute [compute name timeout no-failover]
  (cond-> ^IgniteCompute compute
    timeout     (.withTimeout timeout)
    no-failover (.withNoFailover)
    name        (.withName name)))

(defn reducer
  [task & [init-value]]
  (let [state (atom init-value)]
    (cond
      (function/serializable? task) (->IgniteReducerFn (eval-fn (function/eval-form task)) state)
      (symbol? task)                (->IgniteReducerFn (symbol-fn task) state)
      (fn? task)                    (->IgniteReducerFn task state)
      :else (throw (IllegalArgumentException. (format "Don't know how to create IgniteReducer from %s" task))))))

;; this is getting slightly out of hand...
(def ^:private call-fn       (fn [c x _] (.call ^IgniteCompute c ^IgniteCallable x)))
(def ^:private acall-fn      (fn [c x _] (.callAsync ^IgniteCompute c ^IgniteCallable x)))
(def ^:private broadcast-fn  (fn [c x _] (.broadcast ^IgniteCompute c ^IgniteCallable x)))
(def ^:private abroadcast-fn (fn [c x _] (.broadcastAsync ^IgniteCompute c ^IgniteCallable x)))
(def ^:private call-coll-fn  (fn [c x r]
                               (if r
                                 (.call ^IgniteCompute c ^Collection x ^IgniteReducer r)
                                 (.call ^IgniteCompute c ^Collection x))))
(def ^:private acall-coll-fn (fn [c x r]
                               (if r
                                 (.callAsync ^IgniteCompute c ^Collection x ^IgniteReducer r)
                                 (.callAsync ^IgniteCompute c ^Collection x))))

(defn- distributed-invoke [compute task sync-fn async-fn & [reducer]]
  (let [{:keys [::async ::name ::timeout ::no-failover]} (-> task meta ::opts)
        compute ^IgniteCompute (task-compute compute name timeout no-failover)]
    (cond
      async (->AshtreeFuture (async-fn compute task reducer))
      :else (sync-fn compute task reducer))))

(defn invoke
  "Execute a task on a cluster using compute API instance.

  A task can be one of the following:
  * clojure function       - has to be available on both caller and executing node, should be AOT compiled
  * serializable function  - from function namespace. It would be passed as data, and evaled on executing node
                             (this is EXPERIMENTAL!!!)
  * fully qualified symbol - would be resolved to a function, only has to be valid on executing node

  Returns a function's return value or a future if :async true is passed as one of the options.
  Returned future is an IgniteFuture and can be derefed like standard future.

  Args:
  compute  - compute API instance
  task     - function to execute

  Optional:
  args     - arguments to task"
  [^IgniteCompute compute task & args]
  (distributed-invoke compute (callable task args) call-fn acall-fn))

(defn invoke*
  "See invoke. Uses ignite/*compute* as compute instance."
  [task & args]
  (apply invoke ignite/*compute* task args))

(defn invoke-seq
  "Executes a seq of tasks on a cluster using compute API instance, splitting them across cluster.
  Returns a collection of results or a future if :async true is passed as one of the options.

  Args:
  compute - compute API instance
  tasks   - sequence of tasks

  Optional:
  args    - sequence of vectors with arguments to tasks. First vector would be applied to first task and so on.
            Can be skipped if all task are no-arg. If some tasks are no-arg use nil or empty-vector as their args.
  reducer - if provided it would be called on the results reducing them into a single value.
            It should accept 2 arguments (state, x) and should follow the same rules as task.

  See invoke documentation for more details."
  [^IgniteCompute compute tasks & [args reducer]]
  (let [tasks (mapv callable tasks (or args (repeat nil)))]
    (distributed-invoke compute tasks call-coll-fn acall-coll-fn reducer)))

(defn invoke-seq*
  "See invoke-seq. Uses ignite/*compute* as compute instance."
  [tasks & [args reducer]]
  (invoke-seq ignite/*compute* tasks args reducer))

(defn broadcast
  "Execute a task on all nodes in a cluster.
  Returns a collection of results or a future if :async true is passed as one of the options.

  See invoke documentation for more details."
  [^IgniteCompute compute task & args]
  (distributed-invoke compute (callable task args) broadcast-fn abroadcast-fn))

(defn broadcast*
  "See broadcast. Uses ignite/*compute* as compute instance."
  [task & args]
  (apply broadcast ignite/*compute* task args))