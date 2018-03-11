(ns vermilionsands.ashtree.compute
  (:require [vermilionsands.ashtree.function :as function])
  (:import [clojure.lang IDeref IMeta IBlockingDeref IPending]
           [java.util Collection]
           [java.util.concurrent TimeUnit]
           [org.apache.ignite IgniteCompute Ignite]
           [org.apache.ignite.compute ComputeTaskFuture ComputeTaskTimeoutException]
           [org.apache.ignite.lang IgniteCallable IgniteFuture IgniteReducer IgniteFutureTimeoutException IgniteClosure])
  (:gen-class))

(def ^:dynamic *compute*
  "Compute API instance to be used with with-compute"
  nil)

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

(deftype AshtreeClosure [f]
  IgniteClosure
  (apply [_ x]
    (f x)))

(defmacro ^:private with-timeout-val [no-error? timeout-val & body]
  `(try
     ~@body
     (catch ComputeTaskTimeoutException e#
       (if ~no-error? ~timeout-val (throw e#)))))

(deftype AshtreeFuture [^IgniteFuture future on-get no-timeout-error timeout-error-val]
  IDeref
  (deref [this]
    (.get this))

  IBlockingDeref
  (deref [this timeout-ms timeout-val]
    (try
      (.get this timeout-ms TimeUnit/MILLISECONDS)
      (catch IgniteFutureTimeoutException _ timeout-val)))

  IPending
  (isRealized [_]
    (.isDone future))

  ComputeTaskFuture
  (cancel [_]
    (.cancel future))

  (chain [_ on-done]
    (AshtreeFuture. (.chain future on-done) nil no-timeout-error timeout-error-val))

  (chainAsync [_ on-done exec]
    (AshtreeFuture. (.chainAsync future on-done exec) nil no-timeout-error timeout-error-val))

  (get [_]
    (with-timeout-val no-timeout-error timeout-error-val
      (if on-get
        (on-get (.get future))
        (.get future))))

  (get [this timeout]
    (.get this timeout TimeUnit/MILLISECONDS))

  (get [_ timeout unit]
    (with-timeout-val no-timeout-error timeout-error-val
      (if on-get
        (on-get (.get future timeout unit))
        (.get future timeout unit))))

  (isCancelled [_]
    (.isCancelled future))

  (isDone [this]
    (.isRealized this))

  (listen [_ listener]
    (throw (UnsupportedOperationException. "Not implemented yet!")))

  (listenAsync [_ listener exec]
    (throw (UnsupportedOperationException. "Not implemented yet!")))

  (getTaskSession [_]
    (when (instance? ComputeTaskFuture future)
      (.getTaskSession ^ComputeTaskFuture future))))

(defn- closure? [task]
  (instance? IgniteClosure task))

(defn ^IgniteClosure closure
  "Create an IgniteClosure instance for a task. See callable for acceptable tasks.

  Args:
  task - underlying function should accept one arguments"
  [task & [arg-coercion]]
  (let [f (if arg-coercion #(comp % arg-coercion) identity)]
    (cond
      (closure? task) task
      :else           (->AshtreeClosure (f (function/task->fn task))))))

(defn chain
  "Takes an Ignite future and tasks and chains them together returning a new future."
  [fut task & tasks]
  (let [x (.chain ^IgniteFuture fut ^IgniteClosure (closure task #(.get ^IgniteFuture %)))]
    (if (empty? tasks)
      x
      (recur x (first tasks) (rest tasks)))))

(defn- callable? [task]
  (instance? IgniteCallable task))

(defn ^IgniteCallable callable
  "Create an IgniteCallable instance for a task.

  Accepts either a standard clojure function or a serializable function (from function namespace), which would be
  stored as data and evaled on call, or a fully qualified symbol that would be resolved on call.

  Args:
  task - clojure function, serializable function or fully qualified symbol
  args - argument vector for task, can be nil, or empty if task is a no-arg function"
  [task & [args]]
  (cond
    (callable? task) task
    :else            (->AshtreeCallable (function/task->fn task) args)))

(defn- reducer? [task]
  (instance? IgniteReducer task))

(defn ^IgniteReducer reducer
  "Create an IgniteReducer instance for a task. See callable for acceptable tasks.

  Args:
  task - underlying function should accept two arguments - accumulator and x

  Optional:
  init-value - initial value for state, otherwise state would be set to nil"
  [task & [init-value]]
  (cond
    (reducer? task) task
    :else           (->AshtreeReducer (function/task->fn task) (atom init-value))))

(defn- compute-for-task [compute name timeout no-failover]
  (cond-> ^IgniteCompute compute
    timeout     (.withTimeout timeout)
    no-failover (.withNoFailover)
    name        (.withName name)))

;; spec to the rescue?
;; current approach seems to have some nasty corner cases
(defn parse-args [task opts-seq]
  (letfn [(take-value [k x xs]
            (if (#{:args :tasks} k)
              (let [[l r] (split-with (complement keyword?) xs)]
                [(vec (cons x l)) r])
              [x xs]))]
    (try
      (let [xs (cons :tasks (cons task opts-seq))]
        (loop [acc {} [k v & rest] xs]
          (if (nil? k)
            acc
            (let [[v' rest'] (take-value k v rest)]
              (recur (assoc acc k v') rest')))))
      (catch Exception e
        (throw
          (IllegalArgumentException.
            (format "Failed to parse args %s with %s" [task opts-seq] e)))))))

(defn- validate-opts [opts-map]
  (let [illegal (fn [s & xs] (throw (IllegalArgumentException. ^String (apply format s xs))))
        {:keys [tasks args broadcast affinity-cache affinity-key]} opts-map
        task-count (count tasks)
        arg-count (count args)]
    (cond
      (empty? tasks)
      (illegal "No tasks!")

      (and (not (empty? args)) (not= task-count arg-count))
      (illegal "Count of tasks (%s) does not match with count of args vectors (%s)"
               task-count arg-count)

      (and (:reduce opts-map) (< task-count 2))
      (illegal "Reduce is only supported for multiple tasks")

      (and affinity-cache (or broadcast (> task-count 1)))
      (illegal "Cannot mix affinity call with broadcast or multiple tasks")

      (or (and (nil? affinity-cache) (some? affinity-key))
          (and (some? affinity-cache) (nil? affinity-key)))
      (illegal "Both affinity-cache and affinity-key options are required")

      (and broadcast (> task-count 1))
      (illegal "Cannot mix multiple tasks with broadcast"))
    true))

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
        broadcast #(vec (.broadcast ^IgniteCompute %1 ^IgniteCallable %2))
        reduce    #(.call ^IgniteCompute %1 ^Collection %2 ^IgniteReducer (reducer reduce reduce-init))
        multiple? #(vec (.call ^IgniteCompute %1 ^Collection %2))
        part-id?  #(.affinityCall ^IgniteCompute %1 ^Collection affinity-cache ^int (int affinity-key) ^IgniteCallable %2)
        caches?   #(.affinityCall ^IgniteCompute %1 ^Collection affinity-cache ^Object affinity-key ^IgniteCallable %2)
        aff?      #(.affinityCall ^IgniteCompute %1 ^String affinity-cache ^Object affinity-key ^IgniteCallable %2)
        :else     #(.call ^IgniteCompute %1 ^IgniteCallable %2)))))

;; more and more features are creeping in, it needs another cleanup
(defn invoke*
  "Execute a task on a cluster. See invoke for more details.

  Accepts an options map which should contain a seq of tasks under :tasks key plus can have any option valid for invoke."
  ([opts-map]
   (let [{:keys [tasks args compute async broadcast name no-failover reduce timeout timeout-val]} opts-map
         no-timeout-error (contains? opts-map :timeout-val)
         _ (validate-opts opts-map)
         ;; move into AshtreeFuture or ctor function
         to-vec? (or broadcast (and (second tasks) (nil? reduce)))
         ignite-callable
         (if (second tasks)
           (mapv callable tasks (or args (repeat nil)))
           (callable (first tasks) (first args)))
         compute
         (if-let [c (or compute *compute*)]
           (compute-for-task c name timeout no-failover)
           (throw (IllegalArgumentException. "No compute API instance!")))
         method (compute-method opts-map)]
     (if async
       (if to-vec?
         ;; replace with ctor function
         (->AshtreeFuture (method compute ignite-callable) vec no-timeout-error timeout-val)
         (->AshtreeFuture (method compute ignite-callable) nil no-timeout-error timeout-val))
       (with-timeout-val no-timeout-error timeout-val
         (method compute ignite-callable)))))
  ([task opts-map]
   (invoke* (update opts-map :tasks #(cons %2 %1) task))))

(defn invoke
  "Execute a task on a cluster. Returns one of the following: task result, future or vector of results, depending
  on passed options.

  Sample calls:
  (invoke task)
  (invoke task :args args-seq)
  (invoke task :args args-seq :async true)
  (invoke task1 task2 :args args-seq1 args-seq2 :async true)
  (invoke task :compute compute-instance)

  Args:
  task - task to execute

  Task can be one of the following:
  * clojure function       - has to be available on both caller and executing node, should be AOT compiled
  * serializable function  - defined using sfn/defsfn from function namespace, would be passed as data, and evaled on executing node
                             (experimental option)
  * symbol                 - fully qualified symbol, would be resolved to a function on executing node and executed

  Optional:
  task+args+opts - optional tasks, arguments and options. Should begin with additional tasks (if any) after which
                   args and options can be specified. If multiple tasks are passed all of them would be called together
                   and return value would be changed to a vector of results.

  Options:
  :affinity-cache - cache name, or seq of names, for affinity call. Only for single task without broadcast.
  :affinity-key   - affinity key or partition id when affinity-cache is specified
  :args           - seqs of arguments to tasks.

                    Should either have a seq for each task, or be skipped if all tasks are no-args.
                    If only some tasks accept no arguments you can pass for them nils or empty seqs.

                    For example:
                    (invoke task1 no-arg-task task3 :args arg-seq1 nil args-seq2)

  :async          - if true enables async mode and return type would be an IgniteFuture instance
  :broadcast      - if true would be called in broadcast mode (on all nodes associated with compute instance)
  :compute        - compute instance, if not specified *compute* would be used instead
  :name           - name for this task
  :no-failover    - execute with no failover mode if true
  :reduce         - if provided it would be called on the results reducing them into a single value.
                    It should accept 2 arguments (state, x) and should follow the same rules as task.
                    Only for multiple tasks.

  :reduce-init    - initial state of reducer state
  :timeout        - timeout, after which ComputeTaskTimeoutException would be returned, in milliseconds
  :timeout-val    - val returned on timeout (can be nil), instead of ComputeTaskTimeoutException

  Return values based on options:
  * default - task result
  * broadcast - vector of results
  * multiple tasks - vector of results
  * multiple tasks with reducer - reducer result
  * async - as above, but result would be wrapped in an IgniteFuture instance"
  [task & tasks+args+opts]
  (invoke* (parse-args task tasks+args+opts)))

(defn distribute
  "Returns a new function which would call invoke with supplied task and options. If called with bounded *compute* it
  would be retained as an argument to :compute."
  [task & opts]
  (let [opts-map (parse-args task opts)
        updated-opts (update opts-map :compute #(or % *compute*))]
    (with-meta
      (fn [& args] (invoke* (assoc updated-opts :args [args])))
      (merge
        (meta task)
        {::opts-map updated-opts}))))

(defn finvoke
  "Same as invoke, but will add :async true to options."
  [task & tasks+args+opts]
  (apply invoke task (conj (vec tasks+args+opts) :async true)))

(defn fdistribute
  "Same as distribute, but will add :async true to options."
  [task & opts]
  (apply distribute task (conj (vec opts) :async true)))

(defmacro with-compute
  "Evaluates body in a context in which *compute* is bound to a given compute API instance"
  [compute & body]
  `(binding [*compute* ~compute]
     ~@body))