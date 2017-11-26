(ns vermilionsands.ashtree.data
  (:import [clojure.lang IAtom IDeref IMeta IReference IRef]
           [org.apache.ignite Ignite IgniteAtomicReference IgniteMessaging]
           [org.apache.ignite.lang IgniteBiPredicate]))

(defprotocol DistributedAtom
  (set-shared-validator! [this f]
    "Like clojure.core/set-validator! but sets a validator that would be shared among all instances.
    Validator function has to be available on all instances using this atom.")

  (get-shared-validator [this]
    "Returns shared validator for this atom.")

  (add-shared-watch [this k f]
    "Like clojure.core/add-watch but the watch would be shared amon all instances, and would be executed on
    each instane upon notification.
    Watch function should has to be avaialable on all instances using this atom.")

  (remove-shared-watch [this k]
    "Removes shared watch under key k.")

  (get-shared-watches [this]
    "Returns shared watches for this atom."))

(defn- validate
  "Executes f on x and throws an exception if result is false, or rethrows an exception.
  Otherwise returns nil."
  [f x]
  (try
    (when (and f (false? (f x)))
      (throw (IllegalStateException. "Invalid reference state!")))
    (catch RuntimeException e
      (throw e))
    (catch Exception e
      (throw (IllegalStateException. "Invalid reference state!" e)))))

(defn- notify [ignite-atom old-val new-val]
  (let [messaging (.-messaging ignite-atom)
        {:keys [notification-topic notification-timeout]} (.get ^IgniteAtomicReference (.-shared_ctx ignite-atom))]
    (if notification-topic
      (.sendOrdered ^IgniteMessaging messaging notification-topic [old-val new-val] notification-timeout)
      (doseq [[k f] (concat (.getWatches ignite-atom) (get-shared-watches ignite-atom))]
        (when f
          (f k ignite-atom old-val new-val))))))

(defn- value-swap* [ignite-atom f args]
  (let [[x y rest] args
        old-val (deref ignite-atom)
        new-val (if rest
                  (apply f old-val x y rest)
                  (apply f old-val args))]
    (doseq [g [(deref (.-local_ctx ignite-atom)) (.get ^IgniteAtomicReference (.-shared_ctx ignite-atom))]]
      (validate (:validator g) new-val))
    (if (.compareAndSet ^IgniteAtomicReference (.-state ignite-atom) old-val new-val)
      (do
        (notify ignite-atom old-val new-val)
        new-val)
      (recur ignite-atom f args))))

(deftype IgniteAtom [^IgniteAtomicReference state
                     ^IgniteAtomicReference shared-ctx
                     local-ctx
                     messaging]
  IAtom
  (swap [this f]
    (value-swap* this f nil))

  (swap [this f x]
    (value-swap* this f [x]))

  (swap [this f x y]
    (value-swap* this f [x y]))

  (swap [this f x y args]
    (value-swap* this f [x y args]))

  (compareAndSet [this old-val new-val]
    (validate (:validator @local-ctx) new-val)
    (validate (:validator (.get shared-ctx)) new-val)
    (let [ret (.compareAndSet state old-val new-val)]
      (when ret
        (notify this old-val new-val))
      ret))

  (reset [this new-val]
    (let [old-val (deref this)]
      (validate (:validator @local-ctx) new-val)
      (validate (:validator (.get shared-ctx)) new-val)
      (.set state new-val)
      (notify this old-val new-val)
      new-val))

  IMeta
  (meta [_]
    (:meta @local-ctx))

  IReference
  (resetMeta [_ m]
    (swap! local-ctx assoc :meta m)
    m)

  (alterMeta [_ f args]
    (let [g #(apply f % args)]
      (:meta (swap! local-ctx update :meta g))))

  IRef
  (setValidator [this f]
    (validate f (deref this))
    (swap! local-ctx assoc :validator f)
    nil)

  (getValidator [_]
    (:validator @local-ctx))

  (addWatch [this k f]
    (swap! local-ctx update :watches assoc k f)
    this)

  (removeWatch [this k]
    (swap! local-ctx update :watches dissoc k)
    this)

  (getWatches [_]
    (:watches @local-ctx))

  IDeref
  (deref [_] (.get state))

  DistributedAtom
  (set-shared-validator! [this f]
    (validate f (deref this))
    (loop []
      (let [old (.get shared-ctx)
            new (assoc (.get shared-ctx) :validator f)
            ok? (.compareAndSet shared-ctx old new)]
        (when-not ok?
          (recur)))))

  (get-shared-validator [_]
    (:validator (.get shared-ctx)))

  (add-shared-watch [this k f]
    (loop []
      (let [old (.get shared-ctx)
            new (assoc-in (.get shared-ctx) [:watches k] f)
            ok? (.compareAndSet shared-ctx old new)]
        (when-not ok?
          (recur))))
    this)

  (remove-shared-watch [this k]
    (loop []
      (let [old (.get shared-ctx)
            new (update (.get shared-ctx) [:watches] dissoc k)
            ok? (.compareAndSet shared-ctx old new)]
        (when-not ok?
          (recur))))
    this)

  (get-shared-watches [_]
    (:watches (.get shared-ctx))))

(defn- atom-id [id]
  (str "ashtree-atom-" (name id)))

(defn- find-reference [instance id]
  (some? (.atomicReference instance id nil false)))

(defn- retrieve-shared-objects [instance id]
  (let [state     (.atomicReference instance id nil false)
        ctx       (.atomicReference instance (str id "-ctx") nil false)
        messaging (when (:notifications? (.get ctx)) (.message instance))]
    {:state state :ctx ctx :messaging messaging}))

(defn- init-shared-objects [instance id init notifications? notification-timeout]
  (let [lock (.reentrantLock instance id true true true)]
    (.lock lock)
    (try
      (when-not (find-reference instance id)
        (let [messaging (when notifications? (.message instance))
              init-ctx  (merge
                          (if notifications?
                            {:notification-topic id
                             :notification-timeout (or notification-timeout 0)}
                            {})
                          {:notifications? notifications?})
              ctx       (.atomicReference instance (str id "-ctx") init-ctx true)
              state     (.atomicReference instance id init true)]
          {:state state :ctx ctx :messaging messaging}))
      (finally
        (.unlock lock)))))

(defn- add-listener! [ignite-atom]
  (let [listener-id
        (.localListen
          ^IgniteMessaging (.-messaging ignite-atom)
          (:notification-topic (.get ^IgniteAtomicReference (.-shared_ctx ignite-atom)))
          (reify IgniteBiPredicate
            (apply [_ _ message]
              (let [[old-val new-val] message]
                (doseq [[k f] (concat (.getWatches ignite-atom) (get-shared-watches ignite-atom))]
                  (when f
                    (f k ignite-atom old-val new-val)))
                true))))]
    (swap! (.-local_ctx ignite-atom) assoc :listener listener-id)
    ignite-atom))

(defn distributed-atom
  [^Ignite instance id x & [opts]]
  (let [id (atom-id id)
        {:keys [global-notifications notification-timeout]} opts
        {:keys [state ctx messaging]}
        (if-not (find-reference instance id)
          (or (init-shared-objects instance id x global-notifications notification-timeout)
              (retrieve-shared-objects instance id))
          (retrieve-shared-objects instance id))
        ignite-atom (->IgniteAtom state ctx (atom {}) messaging)]
    (when messaging
      (add-listener! ignite-atom))
    ignite-atom))