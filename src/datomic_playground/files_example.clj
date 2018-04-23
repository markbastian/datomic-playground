(ns datomic-playground.files-example
  (:require [datomic-playground.components.datomic :as sys]
            [datomic.api :as d]
            [clojure-csv.core :as csv]
            [clojure.string :as cs]
            [clojure.instant :as instant]
            [digest]
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
         ;Optional - set the transaction time. Must be monotonic.
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

;(defn load-all [conn]
;  (doto conn
;    (d/transact-async (load-data "data-2010-01-01.csv"))
;    (d/transact-async (load-data "data-2012-01-01.csv"))
;    (d/transact-async (load-data "data-2015-01-01.csv"))))
;
;(load-all conn)

(defn subs-info [{:keys [db-before db-after tx-data tempids] :as r}]
  (d/q
    '[:find ?sn ?attr ?v
      :in $ [[_ ?e]]
      :where
      [?e :person/subscription ?subs-entity]
      [?subs-entity :subscription/fields ?attr]
      [?subs-entity :subscription/name ?sn]
      [?e ?attr ?v]]
    db-after tempids))

(let [{:keys [datomic trq]} (sys/restart {:db-uri "datomic:mem://tmp" :f trq-fn})
      {:keys [conn]} datomic
      data (load-data "data-2010-01-01.csv")
      txa @(d/transact-async conn data)
      txb
      @(d/transact conn [{:person/ssn "123-45-6789"
                          :person/subscription {:subscription/name :s0
                                                :subscription/fields [:person/name
                                                                      :person/zip-code]}}])
      txc
      @(d/transact conn [{:person/ssn "123-45-6789"
                          :person/subscription {:subscription/name :s1
                                                :subscription/fields [:person/name
                                                                      :person/city]}}])
      txd
      @(d/transact conn [{:person/ssn "123-45-6789"
                          :person/name "Mike"}])]
  ;(d/pull-many b '[*] (mapv second d))
  ;(d/q
  ;  '[:find ?f
  ;    :in $ [?entities]
  ;    :where
  ;    [?entities :person/subscription ?v
  ;     ?v :subscription/fields ?f]]
  ;  db-after (map second tempids))
  ;(d/pull-many
  ;  b
  ;  '[:person/ssn
  ;    {:person/subscription
  ;     [:subscription/fields]}]
  ;  (mapv second d))
  ;(d/pull-many
  ;  b
  ;  '[{:person/_subscription [:person/ssn]} :subscription/fields]
  ;  (mapv second d))
  ;Works!!!!!
  (subs-info txa)
  (subs-info txb)
  (subs-info txc)
  ;(subs-info txd)
  ;Was only one entity actually changed?
  ;(d/q
  ;  '[:find ?subs-entity ?a ?n
  ;    :in $ [?subs-entity]
  ;    :where
  ;    [$ ?subs-entity ?a ?n]]
  ;  b (mapv second d))
  ;(d/q
  ;  '[:find ?subs-entity
  ;    :in $ [?subs-entity]
  ;    :where
  ;    [$ ?subs-entity]]
  ;  b (mapv second d))
  )

[{:person/street-address "3 Bueno Blvd",
  :person/state-code "NM",
  :person/subscription [#:db{:id 17592186045424}],
  :meta/source #:db{:id 17592186045421},
  :person/date #inst"2010-05-30T00:00:00.000-00:00",
  :person/city "Rio Rancho",
  :person/name "Mark Bastian",
  :db/id 17592186045420,
  :person/zip-code "83713",
  :person/ssn "123-45-6789"}
 {:db/id 17592186045424,
  :subscription/name :s0,
  :subscription/fields [:person/name :person/zip-code]}]

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