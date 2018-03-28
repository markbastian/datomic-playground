(ns datomic-playground.time-actual
  (:require [datomic.api :as d]
            [datomic-playground.api :as api]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [clj-time.coerce :as c]
            [datomic-playground.components.datomic :as sys]
            [com.stuartsierra.component :as component]))

;http://augustl.com/blog/2013/querying_datomic_for_history_of_entity/

;https://docs.datomic.com/on-prem/getting-started/transact-schema.html
;https://docs.datomic.com/on-prem/getting-started/query-the-data.html
;https://docs.datomic.com/on-prem/getting-started/see-historic-data.html

;(def conn "datomic:mem://events")
;(d/create-database conn)
;(def conn (d/connect conn))

(def event-schema
  [{:db/ident       :event/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index       true}

   {:db/ident       :event/toa
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index       true}

   {:db/ident :event/p
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}

   {:db/ident :event/f
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}

   {:db/ident :event/a
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}

   {:db/ident :event/md
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}])


(def times
  (->> (p/periodic-seq (c/from-string "2017-01-01") (t/months 1))
       (map c/to-date)))

(def amounts (take 24 (repeatedly #(double (rand-int 10000)))))

(defn fh []
  (conj (vec (take-while pos? (iterate (fn [r] (- r (* (rand) 1.0))) 1.0))) 0.0))

(defn gen-sec []
  (let [p (double (rand-int 10000))
        m (map (fn [f t] {:db/id 1
                          :event/name "A"
                          :event/p p
                          :event/f f
                          :event/a (* f p)
                          :event/toa t}) (fh) times)
        md (:event/toa (last m))]
    (map (fn [x] (assoc x :event/md md)) m)))

(let [comp (-> {:db-uri "datomic:mem://events"}
               sys/map->DatomicDB
               component/start)
      schema @(api/transact comp event-schema)
      inserts (doall (for [sec (gen-sec)] @(api/transact comp [sec])))
      db (api/db comp)
      r (d/q
          '[:find ?n .
            :where
            [?e :event/name ?n]]
          db)]
  (component/stop comp)
  r)

(sys/start {:db-uri "datomic:mem://events"})
(def conn (:conn sys/*db*))

@(d/transact conn event-schema)

(for [sec (gen-sec)]
  @(d/transact conn [sec]))

(def db (d/db conn))

(d/q
  '[:find ?n .
    :where
    [?e :event/name ?n]]
  db)

(d/q
  '[:find ?n ?p ?f ?a ?t ?md
    :where
    [?e :event/name ?n]
    [?e :event/p ?p]
    [?e :event/f ?f]
    [?e :event/a ?a]
    [?e :event/toa ?t]
    [?e :event/md ?md]]
  db)

@(d/transact conn [{:event/name "B" :event/p 12345.0 :event/f 1.0 :event/toa #inst"2018-01-01"}])

(d/q
  '[:find ?e ?p ?f ?toa ?md ?tn ?tp ?tf ?ttoa ?tmd
    :in $ ?n
    :where
    [?e :event/name ?n ?tn]
    [?e :event/p ?p ?tp]
    [?e :event/f ?f ?tf]
    [?e :event/toa ?toa ?ttoa]
    [?e :event/md ?md ?tmd]]
  (d/db conn) "B")

(d/q
  '[:find ?e ?a ?b
    :in $ ?t
    :where
    [?e ?a ?b ?t]]
  (d/db conn) 13194139534316)

(d/q
  '[:find ?e ?f ?p ?t
    :in $ ?n
    :where
    [?e :event/name ?n]
    [?e :event/p ?p]
    [?e :event/f ?f ?t]]
  (d/history (d/db conn)) "B")

(d/q
  '[:find ?e ?a ?v ?tx ?added
    :in $ ?e
    :where
    [?e ?a ?v ?tx ?added]]
  (d/history (d/db conn))
  17592186045421)

@(d/transact conn [{:db/id 17592186045421 :event/name "B" :event/p 12345.0 :event/f 1.0 :event/toa #inst"2018-01-01"}])
@(d/transact conn [{:db/id 17592186045421 :event/name "B" :event/p 12345.0 :event/f 0.8 :event/toa #inst"2018-02-01"}])
@(d/transact conn [{:db/id 17592186045421 :event/md #inst"2018-02-01"}])

;
;(def hdb (d/history db))
;
;;[(pull ?a [:name :order]) ...]
;#_
;(->> (d/q '[:find ?aname ?v ?inst
;            :in $ ?e
;            :where [?e ?a ?v ?tx true]
;            [?tx :db/txInstant ?inst]
;            [?a :db/ident ?aname]]
;          history [:item/id "DLC-042"])
;     (sort-by #(nth % 2)))
;
;(d/q
;  '[:find ?n ?e ?f ?toa
;    :in $ ?n
;    :where
;    [$ ?e :event/name ?n]
;    [$ ?e :event/toa ?toa]
;    [$ ?e :event/f ?f]
;    ;[(< #inst"2017-04-01" ?toa)]
;    ;[(< ?toa #inst"2017-06-01")]
;    ]
;  hdb "A")
;
;(d/q
;  '[:find [(pull ?e [:toa :f]) ...]
;    :in $ ?n
;    :where
;    [$ ?e :event/name ?n]
;    [$ ?e :event/toa ?toa]
;    [$ ?e :event/f ?f]
;    ;[(< #inst"2017-04-01" ?toa)]
;    ;[(< ?toa #inst"2017-06-01")]
;    ]
;  hdb "A")
;
;(d/q '[:find ?e ?a ?v ?t ?op
;       :in $ ?e
;       :where [?e ?a ?v ?t ?op]]
;     (d/history (d/db conn)) 1)
;
;;(def commando-id
;;  (ffirst (d/q '[:find ?e
;;                 :where [?e :movie/title "Commando"]]
;;               db)))
;;
;;@(d/transact conn [{:db/id commando-id :movie/genre "future governor"}])
;;
;;(d/q all-data-from-1985 db)
