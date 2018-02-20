(ns user
  (:require [vermilionsands.ashtree.compute :as compute]
            [vermilionsands.ashtree.data :as data])
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
    (alter-var-root #'*compute* (fn [_] (.compute *ignite*)))))

(defn hello-world [x]
  (println "Hello, " x "!")
  x)