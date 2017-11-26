(defproject ashtree "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.ignite/ignite-core "2.3.0"]]
  :profiles {:dev {:source-paths ["dev"]}
             :test {:aot [vermilionsands.ashtree.test-helpers]}})