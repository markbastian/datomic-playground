(ns datomic-playground.weather
  (:require [datomic-playground.components.datomic :as sys]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]))

;Datom ref
;https://docs.datomic.com/cloud/whatis/data-model.html

(def eagle-station    {:db/id 1 :weather/location "Eagle, ID"})
(def meridian-station {:db/id 2 :weather/location "Meridian, ID"})
(def boise-station    {:db/id 3 :weather/location "Boise, ID"})

(def eagle-data
  [{ :weather/temp 101.0 :weather/station "A" :time/toa #inst"2018-01-01"}
   { :weather/temp 101.0 :weather/station "B" :time/toa #inst"2018-01-01"}
   { :weather/forecast "partly cloudy" :weather/forecaster "Sam" :time/toa #inst"2018-01-01"}
   { :weather/temp 90.0 :weather/station "A" :time/toa #inst"2018-01-02"}
   { :weather/temp 91.0 :weather/station "B" :time/toa #inst"2018-01-02"}
   { :weather/forecast "cloudy" :weather/forecaster "Bob" :time/toa #inst"2018-01-02"}
   { :weather/temp 103.0 :weather/station "A" :time/toa #inst"2018-01-03"}
   { :weather/temp 105.0 :weather/station "B" :time/toa #inst"2018-01-03"}
   { :weather/forecast "sunny" :weather/forecaster "Mark" :time/toa #inst"2018-01-03"}])

(def all-eagle-data (mapv #(into % eagle-station) eagle-data))

(def schema
  [{:db/ident       :time/toa
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index       true}

   {:db/ident       :weather/temp
    :db/valueType   :db.type/double
    :db/cardinality :db.cardinality/one}

   {:db/ident :weather/station
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :weather/forecast
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :weather/forecaster
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :weather/location
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(sys/stop)
(sys/start {:db-uri "datomic:mem://weather"})
(def conn (:conn sys/*db*))

(defn load-data [conn]
  (reduce
    (fn [c d]
      ;Put a little time between transactions.
      (Thread/sleep 100)
      (doto c (d/transact [d])))
    (doto conn (d/transact schema))
    all-eagle-data))

(load-data conn)

;Get the most current doc
(d/pull (d/db conn) '[*] 1)

(defn get-transaction-details [conn]
  (let [v (d/q
            '[:find [?toa ?t-toa ?temp ?t-temp ?forecast ?t-forecast ?forecaster ?t-forecaster]
              :in $ ?e
              :where
              [?e :time/toa ?toa ?t-toa]
              [?e :weather/temp ?temp ?t-temp]
              [?e :weather/forecast ?forecast ?t-forecast]
              [?e :weather/forecaster ?forecaster ?t-forecaster]]
            (d/db conn) 1)]
    (zipmap [:toa :t-toa :temp :t-temp :forecast :t-forecast :forecaster :t-forecaster]
            v)))

(d/q
  '[:find ?e ?fc ?t
    :in $ ?t
    :where
    [?e :weather/forecast ?fc ?t true]
    ;[?e :weather/forecaster ?v ?t]
    ]
  (d/history (d/db conn)) 13194139534319)

;State at a transaction
(d/pull (d/as-of (d/db conn) 13194139534319) '[*] 1)

(sort-by
  first
  (d/q
    '[:find ?toa ?temp ?ra ?rb
      :in $ ?e
      :where
      [?e :time/toa ?toa ?t ?ra]
      [?e :weather/temp ?temp ?t ?rb]]
    (d/history (d/db conn)) 1))

(sort-by
  first
  (d/q
    '[:find ?toa ?temp
      :in $ ?e
      :where
      [?e :time/toa ?toa ?t]
      [?e :weather/temp ?temp ?t]]
    (d/history (d/db conn)) 1))

(sort-by
  first
  (d/q
    '[:find ?toa
      :in $ ?e
      :where
      [?e :time/toa ?toa ?t]]
    (d/history (d/db conn)) 1))

(d/q
  '[:find ?e ?a ?v ?tx ?added
    :in $ ?e
    :where
    [?e ?a ?v ?tx ?added]]
  (d/history (d/db conn))
  1)