(ns datomic-playground.files-example
  (:require [datomic-playground.components.datomic :as sys]
            [datomic.api :as d]
            [clojure-csv.core :as csv]
            [clojure.string :as cs]
            [clojure.instant :as instant]
            [digest]
            [clojure.java.io :as io]
            [cider.inlined-deps.toolsreader.v1v1v1.clojure.tools.reader.edn :as edn]))

(defn kwize
  ([str] (kwize nil str))
  ([ns str]
   (keyword (name ns) (-> str cs/lower-case cs/trim (cs/replace #"\W+" "-")))))

(defn load-data [filename]
  (let [[_ datestr] (re-matches #"data-(\d{4}-\d{2}-\d{2}).csv" filename)
        txi (instant/read-instant-date datestr)
        c (slurp filename)
        md5 (digest/md5 c)
        [header & rows] (csv/parse-csv c :delimiter \|)
        cols (map (partial kwize :person) header)
        fm {:file/name filename :file/md5 md5}]
    (->> rows
         (filter #(every? (complement empty?) %))
         (map #(zipmap cols %))
         (map #(update % :person/date instant/read-instant-date))
         (map #(assoc % :meta/source (assoc fm :file/records %)))
         (cons fm)
         (cons {:db/id "datomic.tx" :db/txInstant txi}))))

(sys/stop)
(sys/start {:db-uri "datomic:mem://families"})
(def conn (:conn sys/*db*))

@(d/transact conn (-> "datomic_playground/files_schema.edn" io/resource slurp edn/read-string))
@(d/transact conn (load-data "data-2010-01-01.csv"))
@(d/transact conn (load-data "data-2012-01-01.csv"))
@(d/transact conn (load-data "data-2015-01-01.csv"))

(->> (d/entity (d/db conn) [:person/ssn "123-45-6789"])
     :meta/source
     :file/records
     first
     :db/id
     (d/pull (d/db conn) '[*]))

(d/q
  '[:find ?a ?i
    :in $
    :where
    [?e :person/street-address ?a ?t true]
    [?t :db/txInstant ?i]]
  (d/db conn))

(d/q
  '[:find ?a ?d ?i
    :in $ ?e
    :where
    [?e :person/street-address ?a ?t true]
    [?e :person/date ?d ?t true]
    ;Transaction date
    [?t :db/txInstant ?i]]
  (d/history (d/db conn)) [:person/ssn "123-45-6789"])

;All files asserting this source
(d/q
  '[:find ?f
    :in $ ?e
    :where
    [?e :meta/source ?src ?t]
    [?src :file/name ?f ?t]]
  (d/history (d/db conn)) [:person/ssn "123-45-6789"])

;Most recent source assertion
(d/q
  '[:find ?f
    :in $ ?e
    :where
    [?e :meta/source ?src ?t]
    [?src :file/name ?f ?t]]
  (d/db conn) [:person/ssn "123-45-6789"])