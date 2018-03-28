(ns datomic-playground.movies
  (:require [datomic.api :as d]
            [datomic-playground.components.datomic :as sys]))

;https://docs.datomic.com/on-prem/getting-started/transact-schema.html
;https://docs.datomic.com/on-prem/getting-started/query-the-data.html
;https://docs.datomic.com/on-prem/getting-started/see-historic-data.html

(sys/start {:db-uri "datomic:mem://events"})


(def movie-schema [{:db/ident :movie/title
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/doc "The title of the movie"}

                   {:db/ident :movie/genre
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/doc "The genre of the movie"}

                   {:db/ident :movie/release-year
                    :db/valueType :db.type/long
                    :db/cardinality :db.cardinality/one
                    :db/doc "The year the movie was released in theaters"}])

;@(d/transact conn movie-schema)

(def first-movies [{:movie/title "The Goonies"
                    :movie/genre "action/adventure"
                    :movie/release-year 1985}
                   {:movie/title "Commando"
                    :movie/genre "action/adventure"
                    :movie/release-year 1985}
                   {:movie/title "Repo Man"
                    :movie/genre "punk dystopia"
                    :movie/release-year 1984}])

;@(d/transact conn first-movies)
;
;(def db (d/db conn))
;
;(def all-data-from-1985 '[:find ?title ?year ?genre
;                          :where [?e :movie/title ?title]
;                          [?e :movie/release-year ?year]
;                          [?e :movie/genre ?genre]
;                          [?e :movie/release-year 1985]])
;
;(def commando-id
;  (ffirst (d/q '[:find ?e
;                 :where [?e :movie/title "Commando"]]
;               db)))
;
;@(d/transact conn [{:db/id commando-id :movie/genre "future governor"}])
;
;(d/q all-data-from-1985 db)