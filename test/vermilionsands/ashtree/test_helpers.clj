(ns vermilionsands.ashtree.test-helpers
  (:require [clojure.string :as string])
  (:gen-class))

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

(defn- to-upper-case* [x]
  (string/upper-case x))

;; deliberately complicated to avoid using only java/core methods and fns
(defn to-upper-case [x]
  (to-upper-case* x))