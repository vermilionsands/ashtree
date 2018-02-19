(ns user
  (:require [vermilionsands.ashtree.data :as data])
  (:import [org.apache.ignite Ignition]
           [org.apache.ignite.cache CacheMode]
           [org.apache.ignite.configuration IgniteConfiguration AtomicConfiguration]))

(defonce ^:dynamic *ignite* nil)

(defn ignite! []
  (let [cfg (doto (IgniteConfiguration.)
              (.setAtomicConfiguration
                (doto (AtomicConfiguration.)
                  (.setCacheMode CacheMode/REPLICATED))))
        ignite (Ignition/start cfg)]
    (alter-var-root #'*ignite* (fn [_] ignite))))