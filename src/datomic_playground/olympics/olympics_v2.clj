(ns datomic-playground.olympics.olympics-v2
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datomic-playground.olympics.common-data :refer [noc-regions athletes]]
            [datomic-playground.olympics.system :as system]
            [taoensso.timbre :as timbre]))

(defn conn [] (get (system/system) [:olympics-v2/conn :datomic/conn]))

(defn noc [{:keys [noc region notes]}]
  (let [regions (cond-> [] region (conj region) notes (conj notes))]
    {:noc/name   noc
     :noc/region (map (fn [r] {:region/name r}) regions)}))

(defn athlete [{:keys [sport noc age sex name city year season event
                       team weight id games height medal]}]
  (cond-> {:athlete/id     {:person/id id}
           :athlete/name   {:person/name name}
           :athlete/sex    {:person/sex sex}
           :athlete/sport  {:sport/name sport}
           :athlete/event  {:event/name event}
           :athlete/noc    {:noc/name noc}
           :athlete/team   {:team/name team}
           :athlete/games  {:games/name games}
           :athlete/city   {:city/name city}
           :athlete/season {:season/name season}
           :athlete/year   {:year/value year}}
          height (assoc :athlete/height (double height))
          age (assoc :athlete/age age)
          weight (assoc :athlete/weight (double weight))
          medal (assoc :athlete/medal {:medal/name medal})))

(def schema (->> "datomic_playground/schemas/olympics_v2.edn" io/resource slurp edn/read-string))

(defn load-schema [conn]
  @(d/transact-async conn schema))

(defn load-noc-data [conn]
  (time (let [{:keys [tx-data]} @(d/transact-async conn (map noc noc-regions))]
          (prn (count tx-data)))))

(defn load-athlete-data [conn]
  (->> athletes
       (map athlete)
       (partition-all 10)
       (map (partial d/transact-async conn))))

;~10ms - way faster than datascript
(defn query [conn]
  (time
    (let [db (d/db conn)]
      (count
        (d/q
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
          db "Basketball" 1992 "Gold")))))

;~80ms
;(let [{:keys [datomic/conn]} (system)] (query conn))

#_
(defn query [conn]
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