(ns vermilionsands.ashtree.util.functions
  "AOT-compiled functions for various tests."
  (:require [clojure.string :as string]
            [vermilionsands.ashtree.ignite :as ignite])
  (:import [org.apache.ignite Ignite IgniteCache IgniteCluster])
  (:gen-class))

;; for atom tests
(defn less-than-10 [x]
  (< x 10))

(defn less-than-4 [x]
  (< x 4))

(defn watch-and-store []
  (let [a (atom [])]
    [(fn [_ _ old-val new-val]
       (swap! a conj [old-val new-val]))
     a]))

(def watch-log (atom []))

(defn store-to-atom-watch [_ _ old-val new-val]
  (swap! watch-log conj [old-val new-val]))

;; for compute tests
(defn echo [x] x)

(defn get-node-id [^Ignite ignite]
  (.id (.localNode ^IgniteCluster (ignite/cluster ignite))))

(defn inc-node-state [^Ignite ignite]
  (let [k "counter"
        c (ignite/cluster ignite)]
    (if-let [counter-atom (ignite/get-local c k)]
      (swap! counter-atom inc)
      (do
        (ignite/put-local! c k (atom 0) true)
        (swap! (ignite/get-local c k) inc)))))

(defn cache-peek [^IgniteCache cache key]
  (.localPeek cache key nil))