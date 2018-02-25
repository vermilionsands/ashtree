(ns vermilionsands.ashtree.fixtures
  (:import [org.apache.ignite Ignition]
           [org.apache.ignite.configuration IgniteConfiguration]))

(def ^:dynamic *ignite-instance* nil)

(defn ignite-fixture []
  (fn [f]
    (let [cfg (doto (IgniteConfiguration.)
                (.setIgniteInstanceName "test-instance"))
          instance (Ignition/start cfg)]
      (try
        (binding [*ignite-instance* instance]
          (f))
        (finally
          (Ignition/stop "test-instance" true))))))
