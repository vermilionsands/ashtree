(ns dev
  (:require [functions :refer :all]
            [vermilionsands.ashtree.compute :as c]
            [vermilionsands.ashtree.data :as d]
            [vermilionsands.ashtree.function :as f]
            [vermilionsands.ashtree.ignite :as i])
  (:import [org.apache.ignite Ignition Ignite IgniteCompute]
           [org.apache.ignite.cache CacheMode]
           [org.apache.ignite.configuration IgniteConfiguration AtomicConfiguration]
           [org.apache.ignite.spi.communication.tcp TcpCommunicationSpi]
           [org.apache.ignite.spi.discovery.tcp TcpDiscoverySpi]
           [org.apache.ignite.spi.discovery.tcp.ipfinder.multicast TcpDiscoveryMulticastIpFinder]
           [org.apache.ignite.spi.discovery.tcp.ipfinder.vm TcpDiscoveryVmIpFinder]))

(defonce ^:dynamic ^Ignite *ignite* nil)
(defonce ^:dynamic ^IgniteCompute *compute* nil)

(defn set-port [^IgniteConfiguration cfg port]
  (let [tcp-comm-spi (TcpCommunicationSpi.)]
    (.setLocalPort tcp-comm-spi port)
    (.setCommunicationSpi cfg tcp-comm-spi)))

(defn set-discovery [^IgniteConfiguration cfg discovery-mode arg]
  (let [spi (TcpDiscoverySpi.)
        ip-finder
        (condp = discovery-mode
          :multicast
          (let [ip-finder (TcpDiscoveryMulticastIpFinder.)]
            ;;"228.10.10.157"
            (.setMulticastGroup ip-finder ^String arg))

          :static
          (let [ip-finder (TcpDiscoveryVmIpFinder.)]
            ;;"1.2.3.4", "1.2.3.5:47500..47509"
            (.setAddresses ip-finder arg))

          :mixed
          (let [ip-finder (TcpDiscoveryMulticastIpFinder.)]
            (.setMulticastGroup ip-finder (first arg))
            (.setAddresses ip-finder (second arg))))

        tcp-discovery-spi (.setIpFinder spi ip-finder)]
    (.setDiscoverySpi cfg tcp-discovery-spi)))

(defn ignite! [& opts]
  (let [{:keys [port client discovery]} (apply hash-map opts)
        cfg (doto (IgniteConfiguration.)
              (.setPeerClassLoadingEnabled true)
              (.setAtomicConfiguration
                (doto (AtomicConfiguration.)
                  (.setCacheMode CacheMode/REPLICATED))))
        cfg (cond-> cfg
              port      (set-port port)
              client    (.setClientMode true)
              discovery (set-discovery (:mode discovery) (:arg discovery)))
        ignite (Ignition/start ^IgniteConfiguration cfg)]
    (alter-var-root #'*ignite* (constantly ignite))
    (alter-var-root #'*compute* (constantly (i/compute *ignite*)))))

(defn extinguish! []
  (alter-var-root #'*ignite* (constantly nil))
  (alter-var-root #'*compute* (constantly nil))
  (Ignition/stopAll true))