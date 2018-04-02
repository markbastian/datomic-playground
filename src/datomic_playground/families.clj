(ns datomic-playground.families
  (:require [datomic-playground.components.datomic :as sys]
            [datomic.api :as d]
            [clojure.pprint :as pp]
            [beicon.core :as rx])
  (:import (java.util.concurrent BlockingQueue)))

;http://blog.datomic.com/2013/10/the-transaction-report-queue.html
;https://fuqua.io/blog/2014/05/pushing-database-changes-to-the-web-with-datomic/
;tx-report-queue

(def schema
  [{:db/ident :family/name
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/string
    :db/unique :db.unique/identity}
   {:db/ident :family/spouse
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}
   {:db/ident :family/child
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/ident :family/sibling
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}])

(def data
  [{:family/name "Mark"}
   {:family/name "Becky"}
   {:family/name "Chloe"}
   {:family/name "Jenny"}
   {:family/name "Mark" :family/spouse [:family/name "Becky"] :family/child [:family/name "Chloe"]}
   {:family/name "Becky" :family/spouse [:family/name "Mark"] :family/child [:family/name "Chloe"]}
   {:family/name "Mark" :family/spouse [:family/name "Becky"] :family/child [:family/name "Jenny"]}
   {:family/name "Becky" :family/spouse [:family/name "Mark"] :family/child [:family/name "Jenny"]}
   {:family/name "Chloe" :family/sibling [:family/name "Jenny"]}
   {:family/name "Jenny" :family/sibling [:family/name "Chloe"]}])

;(sys/stop)
(sys/start {:db-uri "datomic:mem://families"})
(def conn (:conn sys/*db*))

;(def stream (rx/from-coll (d/tx-report-queue conn)))

;(def stream
;  (let [^BlockingQueue report-queue (d/tx-report-queue conn)]
;    (rx/from-coll (repeatedly #(.take report-queue)))))

;(def stream
;  (rx/create (fn [sink]
;               (sink 1)          ;; next with `1` as value
;               (sink (rx/end 2)) ;; next with `2` as value and end the stream
;               (fn []))))
;
;(rx/on-value stream #(println "v:" %))

;(rx/on-value stream #(println "v:" %))
;
;(defn change-monitor [conn]
;  (future
;    (let [^BlockingQueue report-queue (d/tx-report-queue conn)]
;      (while true
;        (let [{:keys [tx-data] :as report} (.take report-queue)]
;          (pp/pprint [:REPORT! tx-data]))))))
;
;(change-monitor conn)

;@(d/transact conn schema)
;
;@(d/transact
;  conn
;  [{:family/name   "Mark"
;    :family/spouse {:family/name "Becky"}
;    :family/child  {:family/name "Chloe"}}])

;(defn load-data [conn]
;  (d/transact conn schema)
;  (for [d data] @(d/transact conn [d])))
;
;(load-data conn)
;
;(d/pull (d/db conn) '[*] [:family/name "Mark"])
;(d/pull (d/db conn) '[:family/name
;                      {:family/spouse 1}
;                      {:family/child 1}]  [:family/name "Mark"])