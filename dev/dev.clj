(ns dev
  (:require [functions :refer :all]
            [vermilionsands.ashtree.compute :as c]
            [vermilionsands.ashtree.data :as d]
            [vermilionsands.ashtree.function :as f]
            [vermilionsands.ashtree.ignite :as i])
  (:import [org.apache.ignite Ignition Ignite IgniteCompute]
           [org.apache.ignite.cache CacheMode]
           [org.apache.ignite.configuration IgniteConfiguration AtomicConfiguration]
           [org.apache.ignite.lang IgniteRunnable IgniteCallable]))

(defonce ^:dynamic *ignite* nil)
(defonce ^:dynamic *compute* nil)

(defn ignite! []
  (let [cfg (doto (IgniteConfiguration.)
              (.setPeerClassLoadingEnabled true)
              (.setAtomicConfiguration
                (doto (AtomicConfiguration.)
                  (.setCacheMode CacheMode/REPLICATED))))
        ignite (Ignition/start cfg)]
    (alter-var-root #'*ignite* (fn [_] ignite))
    (alter-var-root #'*compute* (constantly (i/compute *ignite*)))))