(defproject ashtree "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.memoize "0.5.9"]
                 [org.apache.ignite/ignite-core "2.3.0"]]
  :profiles {:dev {:source-paths ["dev"]
                   :aot [functions vermilionsands.ashtree.compute]}
             :test {:aot [vermilionsands.ashtree.test-helpers]}}
  :global-vars {*warn-on-reflection* true})