(defproject ashtree "0.1.0"
  :description "Clojure + Apache Ignite"
  :url "https://github.com/vermilionsands/ashtree"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/core.memoize "0.5.9"]
                 [org.apache.ignite/ignite-core "2.3.0"]]
  :profiles {:dev  {:source-paths ["dev"]
                    :aot [functions vermilionsands.ashtree.compute]
                    :dependencies [[org.clojure/clojure "1.9.0"]
                                   [org.apache.ignite/ignite-log4j "2.3.0"]]
                    :global-vars {*warn-on-reflection* true}}
             :test {:aot [vermilionsands.ashtree.util.functions]}}
  :aot [vermilionsands.ashtree.compute])