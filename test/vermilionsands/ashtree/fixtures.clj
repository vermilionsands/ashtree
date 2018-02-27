(ns vermilionsands.ashtree.fixtures
  (:import [org.apache.ignite Ignition Ignite]
           [org.apache.ignite.cache CacheMode]
           [org.apache.ignite.configuration IgniteConfiguration AtomicConfiguration]))

(def ^:dynamic *ignite-instance* nil)

(defn- instance-config []
  (doto (IgniteConfiguration.)
    (.setPeerClassLoadingEnabled true)
    (.setAtomicConfiguration
      (doto (AtomicConfiguration.)
        (.setCacheMode CacheMode/REPLICATED)))
    (.setIgniteInstanceName (name (gensym "test-instance-")))))

(defn ignite-fixture
  ([]
   (ignite-fixture 1 true))
  ([n bind?]
   (when (< n 1)
     (throw (IllegalArgumentException. (format "Number of instances cannot be lower than 1! Got %s" n))))
   (fn [f]
     (let [instances (mapv (fn [_] (Ignition/start ^IgniteConfiguration (instance-config))) (range n))]
       (try
         (if bind?
           (binding [*ignite-instance* (first instances)]
             (f))
           (f))
         (finally
           (doseq [i instances]
             (Ignition/stop (.getIgniteInstanceName (.configuration ^Ignite i)) true))))))))
