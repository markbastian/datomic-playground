(ns datomic-playground.simple-example
  (:require [datomic.api :as d]))

(def db-uri
  "datomic:sql://families?jdbc:postgresql://localhost:5432/datomic?user=datomic&password=datomic")

(d/create-database db-uri)

(def conn (d/connect db-uri))

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

@(d/transact conn schema)

@(d/transact
   conn
   [{:family/name   "Mark"
     :family/spouse {:family/name "Becky"}
     :family/child  {:family/name "Chloe"}}])

(def data
  [{:family/name "Mark"}
   {:family/name "Becky"}
   {:family/name "Chloe"}
   {:family/name "Jenny"}
   {:family/name "Mark" :family/spouse {:family/name "Becky"} :family/child {:family/name "Chloe"}}
   {:family/name "Becky" :family/spouse {:family/name "Mark"} :family/child {:family/name "Chloe"}}
   {:family/name "Mark" :family/spouse {:family/name "Becky"} :family/child {:family/name "Jenny"}}
   {:family/name "Becky" :family/spouse {:family/name "Mark"} :family/child {:family/name "Jenny"}}
   {:family/name "Chloe" :family/sibling {:family/name "Jenny"}}
   {:family/name "Jenny" :family/sibling {:family/name "Chloe"}}])

(defn load-data [conn]
  (d/transact conn schema)
  (for [d data] @(d/transact conn [d])))

(load-data conn)

(d/pull (d/db conn) '[*] [:family/name "Mark"])
(d/pull (d/db conn) '[:family/name
                      {:family/spouse 1}
                      {:family/child 1}]  [:family/name "Mark"])

(count (seq (d/datoms (d/history (d/db conn)) :eavt)))