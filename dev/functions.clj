(ns functions
  (:require [vermilionsands.ashtree.ignite :as i])
  (:import [org.apache.ignite Ignite IgniteCluster])
  (:gen-class))

(defn hello-world [x]
  (println "Hello, " x "!")
  x)

(defn get-node-id [^Ignite ignite]
  (.id (.localNode ^IgniteCluster (i/cluster ignite))))