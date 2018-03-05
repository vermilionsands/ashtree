(ns vermilionsands.ashtree.util.functions
  "AOT-compiled functions for various tests."
  (:require [clojure.string :as string])
  (:import [org.apache.ignite Ignite IgniteCache])
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
  (let [n (.localNode (.cluster ignite))]
    (.id (.localNode (.cluster ignite)))))

(defn inc-node-state [^Ignite ignite]
  (let [k "counter"
        local-node-map (.nodeLocalMap (.cluster ignite))]
    (if-let [counter-atom (.get local-node-map k)]
      (swap! counter-atom inc)
      (do
        (.putIfAbsent local-node-map k (atom 0))
        (swap! (.get local-node-map k) inc)))))

(defn cache-peek [^IgniteCache cache key]
  (.localPeek cache key nil))