(defproject datomic-playground "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "0.4.474"]
                 [clj-time "0.14.2"]
                 [com.stuartsierra/component "0.3.2"]
                 [integrant "0.6.3"]
                 [com.taoensso/timbre "4.10.0"]
                 ;Free
                 ;[com.datomic/datomic-free "0.9.5661"]
                 ;Time to go big time
                 [com.datomic/datomic-pro "0.9.5661"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [org.postgresql/postgresql "9.3-1102-jdbc41"]
                 [funcool/beicon "4.1.0"]
                 [clojure-csv "2.0.2"]
                 [digest "1.4.8"]])
