(ns datomic-playground.olympics.core
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datomic-playground.olympics.system]
            [datomic-playground.olympics.common-data :refer [noc-regions athletes]]))

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
  (d/transact-async conn schema))

(defn load-data [conn]
  (do
    (future (time (let [{:keys [tx-data]} @(d/transact-async conn (map noc noc-regions))] (prn (count tx-data)))))
    (future (time (let [{:keys [tx-data]} @(d/transact-async conn (map athlete athletes))] (prn (count tx-data)))))))

;~25ms - way faster than datascript
(defn query [conn]
  (time
  (let [db (d/db conn)]
    (->>
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
        db "Basketball" 1992 "Gold")
      (group-by first)
      (map (fn [[k v]] (reduce (fn [m [_ a v]] (assoc m a v)) {} v)))))))