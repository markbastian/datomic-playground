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
  (future
    (doseq [chunk (partition-all 10 (map athlete athletes))]
      (d/transact-async conn chunk))))

(defn query [conn]
  (let [db (d/db conn)]
    (->>
      (d/q
        '[:find ?e ?k ?v
          :in $ ?sport-name ?year-value ?medal-name
          :where
          [?e :athlete/sport ?sport-name]
          [?e :athlete/year ?year-value]
          [?e :athlete/medal ?medal-name]
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

;Games
{:games/city   "Barcelona"
 :games/year   1992
 :games/season "Summer"
 :games/games  "1992 Summer"
 :games/events [{:event/sport  "Basketball"
                 :event/event  "Basketball Women's Basketball"
                 :event/medalists [{:athlete/medal  "Gold"
                                    :medal/winner :winner}]}]
 :games/athletes [{:athlete/age    28
                   :athlete/sex    "F"
                   :athlete/name   "Irina Edvinovna Minkh"
                   :athlete/weight 64.0
                   :athlete/id     80175
                   :athlete/height 175.0
                   :athlete/noc    "EUN"
                   :athlete/team   "Unified Team"}]}


;~80ms
;(let [{:keys [datomic/conn]} (system)] (query conn))
