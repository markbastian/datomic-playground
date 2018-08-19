(ns datomic-playground.olympics.common-data
  (:require [clojure.edn :as edn]
            [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [clojure.string :as cs]))

;https://www.kaggle.com/heesoo37/120-years-of-olympic-history-athletes-and-results
(defn normalize [m]
  (let [some-val (complement #{"" "NA"})
        parse #(cond-> % (re-matches #"\d+(\.\d+)?" %) edn/read-string)
        add-kv (fn [m [k v]] (cond-> m (some-val v) (assoc k (parse v))))]
    (reduce add-kv {} m)))

(defn read-file [f]
  (let [[h & r] (->> f io/resource slurp csv/parse-csv)]
    (map #(normalize (zipmap (map (comp keyword cs/trim cs/lower-case) h) %)) r)))

(defonce noc-regions (read-file "datomic_playground/olympics/noc_regions.csv"))
(defonce athletes (read-file "datomic_playground/olympics/athlete_events.csv"))
