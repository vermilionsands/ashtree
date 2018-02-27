(ns functions
  (:import [org.apache.ignite Ignite])
  (:gen-class))

(defn hello-world [x]
  (println "Hello, " x "!")
  x)

(defn get-node-id [^Ignite ignite]
  (.id (.localNode (.cluster ignite))))