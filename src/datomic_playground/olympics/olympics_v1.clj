(ns datomic-playground.olympics.olympics-v1
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datomic-playground.olympics.system :as system]
            [datomic-playground.olympics.common-data :refer [noc-regions athletes]]
            [taoensso.timbre :as timbre]))

(defn conn [] (get (system/system) [:olympics-v1/conn :datomic/conn]))

(defn noc [{:keys [noc region notes]}]
  (let [regions (cond-> [] region (conj region) notes (conj notes))]
    {:noc/name   noc
     :noc/region regions}))

(defn athlete [{:keys [sport noc age sex name city year season event
                       team weight id games height medal]}]
  (cond-> {:athlete/id     id
           :athlete/name   name
           :athlete/sex    sex
           :athlete/sport  sport
           :athlete/event  event
           :athlete/noc    noc
           :athlete/team   team
           :athlete/games  games
           :athlete/city   city
           :athlete/season season
           :athlete/year   year}
          height (assoc :athlete/height (double height))
          age (assoc :athlete/age age)
          weight (assoc :athlete/weight (double weight))
          medal (assoc :athlete/medal medal)))

(def schema (->> "datomic_playground/schemas/olympics_v1.edn" io/resource slurp edn/read-string))

(defn load-schema [conn]
  @(d/transact conn schema))

(defn load-noc-data-sync [conn]
  (->> noc-regions
       (map noc)
       (partition-all 10)
       (pmap (partial d/transact conn))
       (mapv deref)))

(defn load-noc-data [conn]
  (doseq [n (partition-all 10 (map noc noc-regions))]
    (future
      (try
        @(d/transact-async conn n)
        (catch Throwable t (timbre/error (.getMessage t)))))))

(defn load-athlete-data [conn]
  (doall
    (for [docs (partition-all 10 (map athlete athletes))]
      (try
        (d/transact-async conn docs)
        (catch Throwable t (timbre/error (.getMessage t)))))))

(defn query [conn]
  (let [db (d/db conn)]
    (time
      (count
        (d/q
          '[:find ?e ?k ?v
            :in $ ?sport-name ?year-value ?medal-name
            :where
            [?e :athlete/sport ?sport-name]
            [?e :athlete/year ?year-value]
            [?e :athlete/medal ?medal-name]
            [?e ?a ?v]
            [?a :db/ident ?k]]
          db "Basketball" 1992 "Gold")))))

;~80ms
;(let [{:keys [datomic/conn]} (system)] (query conn))
