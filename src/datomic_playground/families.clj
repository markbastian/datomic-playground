(ns datomic-playground.families
  (:require [datomic-playground.components.datomic :as sys]
            [datomic.api :as d]))

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


(sys/stop)
(sys/start {:db-uri "datomic:mem://families"})
(def conn (:conn sys/*db*))

@(d/transact conn schema)

@(d/transact
  conn
  [{:family/name   "Mark"
    :family/spouse {:family/name "Becky"}
    :family/child  {:family/name "Chloe"}}])

(defn load-data [conn]
  (d/transact conn schema)
  (for [d data] @(d/transact conn [d])))

(load-data conn)

(d/pull (d/db conn) '[*] [:family/name "Mark"])
(d/pull (d/db conn) '[:family/name
                      {:family/spouse 1}
                      {:family/child 1}]  [:family/name "Mark"])