(ns datomic-playground.olympics.olympics-v3
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datomic-playground.olympics.common-data :refer [noc-regions athletes]]
            [datomic-playground.olympics.system :as system]
            [clojure.string :as cs]
            [clojure.core.async :as async :refer [go go-loop >!! <!! >! <! chan close!]]))

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
  (time
    (->> athletes
         (partition-all 10)
         (map (fn [a] [[:olympics/insert-athletes a]]))
         (map (fn [a] (d/transact-async conn a)))
         count)))

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

;https://docs.datomic.com/on-prem/best-practices.html#pipeline-transactions
(defn tx-pipeline
  "Transacts data from from-ch. Returns a map with:
     :result, a return channel getting {:error t} or {:completed n}
     :stop, a fn you can use to terminate early."
  [conn conc from-ch]
  (let [to-ch (chan 100)
        done-ch (chan)
        transact-data (fn [data]
                        (try
                          @(d/transact-async conn data)
                          ; if exception in a transaction
                          ; will close channels and put error
                          ; on done channel.
                          (catch Throwable t
                            (.printStackTrace t)
                            (close! from-ch)
                            (close! to-ch)
                            (>!! done-ch {:error t}))))]

    ; go block prints a '.' after every 1000 transactions, puts completed
    ; report on done channel when no value left to be taken.
    (go-loop [total 0]
      (when (zero? (mod total 1000))
        (print ".") (flush))
      (if-let [c (<! to-ch)]
        (recur (inc total))
        (>! done-ch {:completed total})))

    ; pipeline that uses transducer form of map to transact data taken from
    ; from-ch and puts results on to-ch
    (async/pipeline-blocking conc to-ch (map transact-data) from-ch)

    ; returns done channel and a function that you can use
    ; for early termination.
    {:result done-ch
     :stop (fn [] (async/close! to-ch))}))
