(ns datomic-playground.weather
  (:require [datomic-playground.components.datomic :as sys]
            [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

;Datom ref
;https://docs.datomic.com/cloud/whatis/data-model.html

(def eagle-data
  [{:weather/location "Eagle, ID"}
   {:weather/location "Meridian, ID"}
   {:weather/location "Boise, ID"}
   {:weather/station "A"}
   {:weather/station "B"}
   {:weather/location "Eagle, ID" :weather/temp 101.0 :time/toa #inst"2018-01-01"}
   {:weather/location "Meridian, ID" :weather/temp 101.0 :time/toa #inst"2018-01-01"}
   {:weather/location "Eagle, ID" :weather/forecast "partly cloudy" :weather/forecaster "Sam" :time/toa #inst"2018-01-01"}
   {:weather/location "Meridian, ID" :weather/temp 90.0 :time/toa #inst"2018-01-02"}
   {:weather/location "Eagle, ID" :weather/temp 91.0 :time/toa #inst"2018-01-02"}
   {:weather/location "Eagle, ID" :weather/forecast "cloudy" :weather/forecaster "Bob" :time/toa #inst"2018-01-02"}
   {:weather/location "Eagle, ID" :weather/temp 103.0 :time/toa #inst"2018-01-03"}
   {:weather/location "Meridian, ID" :weather/temp 105.0 :time/toa #inst"2018-01-03"}
   {:weather/location "Eagle, ID" :weather/forecast "sunny" :weather/forecaster "Mark" :time/toa #inst"2018-01-03"}])

(def schema (-> "datomic_playground/weather_schema.edn" io/resource slurp edn/read-string))

;(sys/stop)
(sys/start {:db-uri "datomic:mem://weather"})
(def conn (:conn sys/*db*))

(defn load-data [conn]
  (reduce
    (fn [c d]
      ;Put a little time between transactions.
      (Thread/sleep 100)
      (doto c (d/transact [d])))
    (doto conn (d/transact schema))
    eagle-data))

(load-data conn)

;Get the most current doc
(d/pull (d/db conn) '[*] [:weather/location "Eagle, ID"])
(d/pull (d/db conn) '[*] [:weather/station "B"])

(defn get-transaction-details [conn loc]
  (d/q
    '[:find ?av ?a ?v ?t-toa
      :in $ ?e
      :where
      [$ ?e ?a ?v ?t-toa]
      [$ ?a _ ?av]]
    (d/db conn) [:weather/location loc]))

(let [v (get-transaction-details conn "Eagle, ID")
      p (partition 2 v)]
  (map (fn [[k v]]
         [k
          (d/pull (d/as-of (d/db conn) v) '[*] 1)]) p))

(sort-by
  first
  (d/q
    '[:find ?toa ?temp
      :in $ ?e
      :where
      [$ ?e :time/toa ?toa ?t true]
      [$ ?e :weather/temp ?temp ?t true]]
    (d/history (d/db conn)) [:weather/location "Eagle, ID"]))