(ns datomic-playground.files-example
  (:require [datomic-playground.components.datomic :as sys]
            [datomic.api :as d]
            [clojure-csv.core :as csv]
            [clojure.string :as cs]
            [clojure.instant :as instant]
            [digest]
            [clojure.java.io :as io]
            [cider.inlined-deps.toolsreader.v1v1v1.clojure.tools.reader.edn :as edn]
            [clojure.pprint :as pp]))

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
         (map #(assoc % :meta/source fm))
         (cons fm)
         (cons {:db/id "datomic.tx" :db/txInstant txi}))))

(defn trq-fn [_ {:keys [db-before db-after tx-data tempids]}]
  #_(pp/pprint
    (d/q
      '[:find ?n .
        :in $ ?e
        :where
        [?e :person/name ?n]]
      db-after [:person/ssn "123-45-6789"]))
  (pp/pprint
    (d/q
      '[:find ?n
        :in $
        :where
        [?e :file/records ?r]
        [?r :person/name ?n]]
      db-after))
  #_(pp/pprint
    (d/pull-many db-after '[*] (distinct (map second tempids)))))

(sys/restart {:db-uri "datomic:mem://families"
              :f trq-fn})
(def conn (get-in sys/*db* [:datomic :conn]))

(defn load-all [conn]
  (doto conn
    (d/transact-async (load-data "data-2010-01-01.csv"))
    (d/transact-async (load-data "data-2012-01-01.csv"))
    (d/transact-async (load-data "data-2015-01-01.csv"))))

(load-all conn)

;(let [{:keys [db-before db-after tx-data tempids]}
;      @(d/transact conn (load-data "data-2015-01-01.csv"))]
;  (pp/pprint (first tx-data) #_(d/entity db-after (first tx-data))))

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

(->> (d/entity (d/db conn) [:person/ssn "123-45-6789"])
     :meta/source
     :file/md5)

(d/pull
  (d/db conn)
  '[*
    {:meta/source [:file/md5 {:meta/_source [:person/ssn]}]}]
  [:person/ssn "123-45-6789"])

(d/pull
  (d/db conn)
  '[*
    {:meta/_source [:person/ssn]}]
  [:file/md5 "079b28edae033e4e01f3cff009628283"])

(->> (d/entity (d/db conn) [:file/md5 "079b28edae033e4e01f3cff009628283"])
     :meta/_source)