(ns datomic-playground.families
  (:require [datomic-playground.components.datomic :as sys]
            [datomic.api :as d]
            [clojure.pprint :as pp]
            [beicon.core :as rx]
            [com.stuartsierra.component :as component])
  (:import (java.util.concurrent BlockingQueue)))

;http://blog.datomic.com/2013/10/the-transaction-report-queue.html
;https://fuqua.io/blog/2014/05/pushing-database-changes-to-the-web-with-datomic/
;tx-report-queue

(def schema
  [{:db/ident :family/gender
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :family/male}
   {:db/ident :family/female}
   {:db/ident :family/name
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
    :db/valueType   :db.type/ref}
   {:db/ident :family/parent
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}])

(def data
  [{:family/name "Mark" :family/gender :family/male}
   {:family/name "Becky" :family/gender :family/female}
   {:family/name "Chloe" :family/gender :family/female}
   {:family/name "Jenny" :family/gender :family/female}
   {:family/name "Mark" :family/spouse {:family/name "Becky"} :family/child {:family/name "Chloe"}}
   {:family/name "Becky" :family/spouse {:family/name "Mark"} :family/child {:family/name "Chloe"}}
   {:family/name "Mark" :family/spouse {:family/name "Becky"} :family/child {:family/name "Jenny"}}
   {:family/name "Becky" :family/spouse {:family/name "Mark"} :family/child {:family/name "Jenny"}}
   {:family/name "Chloe" :family/sibling {:family/name "Jenny"}
    :family/parent [{:family/name "Becky"} {:family/name "Mark"}]}
   {:family/name "Jenny" :family/sibling {:family/name "Chloe"}
    :family/parent [{:family/name "Becky"} {:family/name "Mark"}]}])

;(sys/stop)
(sys/start {:db-uri "datomic:mem://families"})
(def conn (:conn sys/*db*))

@(d/transact conn schema)

@(d/transact conn data)

;@(d/transact
;  conn
;  [{:family/name   "Mark"
;    :family/spouse {:family/name "Becky"}
;    :family/child  {:family/name "Chloe"}}])
;
;(defn load-data [conn]
;  (d/transact conn schema)
;  (for [d data] @(d/transact conn [d])))
;
;(load-data conn)
;
;(d/pull (d/db conn) '[*] [:family/name "Mark"])
;
;(d/pull
;  (d/db conn)
;  '[:family/name
;    {:family/gender 1}
;    {[:family/spouse :limit 1] [:family/name]}
;    {:family/child 1}]
;  [:family/name "Mark"])