(ns user
  (:require [vermilionsands.ashtree.data :as data])
  (:import [org.apache.ignite Ignition]))

(defonce ^:dynamic *ignite* nil)

(defn ignite! []
  (let [ignite (Ignition/start)]
    (alter-var-root #'*ignite* (fn [_] ignite))))