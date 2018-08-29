(ns datomic-playground.olympics.olympics-v3
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datomic-playground.olympics.common-data :refer [noc-regions athletes]]
            [datomic-playground.olympics.system :as system]
            [clojure.string :as cs]
            [clojure.core.async :as async :refer [go go-loop >!! <!! >! <! chan close!]]
            [durable-queue :as q]))

(defn conn [] (get (system/system) [:olympics-v3/conn :datomic/conn]))

(def schema (->> "datomic_playground/schemas/olympics_v3.edn"
                 io/resource
                 slurp
                 (edn/read-string {:readers {'db/id  datomic.db/id-literal
                                             'db/fn  datomic.function/construct
                                             'base64 datomic.codec/base-64-literal}})))

(defn load-schema [conn]
  @(d/transact-async conn schema))

(defn load-noc-data [conn]
  (time (let [data (map (fn [n] [:olympics/insert-noc n]) noc-regions)
              {:keys [tx-data]} @(d/transact-async conn data)]
          (prn (count tx-data)))))


(defn load-athlete-data [conn]
  (future
    (doseq [chunk (partition-all 10 athletes)]
      (d/transact-async conn [[:olympics/insert-athletes chunk]]))))

;~10ms - way faster than datascript
(defn query [conn]
  (let [db (d/db conn)]
    (->> (d/q
           '[:find ?e ?k ?v
             :in $ ?sport-name ?year-value ?medal-name
             :where
             [?sport :sport/name ?sport-name]
             [?year :year/value ?year-value]
             [?medal :medal/name ?medal-name]
             [?e :athlete/sport ?sport]
             [?e :athlete/year ?year]
             [?e :athlete/medal ?medal]
             [?e ?a ?v]
             [?a :db/ident ?k]]
           db "Basketball" 1992 "Gold")
         (group-by first)
         (map (fn [[_ v]] (apply hash-map (mapcat rest v)))))))

(defn all-uniques [conn]
  (time
    (let [db (d/db conn)]
      (count
        (d/q
          '[:find ?e ?id ?games ?event
            :in $
            :where
            [?e :athlete/id ?id]
            [?e :athlete/games ?games]
            [?e :athlete/event ?event]]
          db)))))

;~80ms
;(let [{:keys [datomic/conn]} (system)] (query conn))

#_(defn query [conn]
    (time
      (let [db (d/db conn)]
        (count
          (d/q
            '[:find ?e ?a ?v
              :in $ ?sport-name ?year-value ?medal-name
              :where
              [?sport :sport/name ?sport-name]
              [?year :year/value ?year-value]
              [?medal :medal/name ?medal-name]
              [?e _ ?sport]
              [?e _ ?year]
              [?e _ ?medal]
              [?e ?a ?vref]
              [?vref _ ?v]]
            db "Basketball" 1992 "Gold")))))