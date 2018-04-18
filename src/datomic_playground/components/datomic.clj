(ns datomic-playground.components.datomic
  (:require [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import (java.util.concurrent BlockingQueue)))

(defrecord DatomicDB [db-uri delete-on-stop?])

(defn load-schemas []
  (->>
    "datomic_playground/schemas"
    io/resource
    io/as-file
    .listFiles
    (map #(-> % slurp edn/read-string))))

(extend-type DatomicDB
  component/Lifecycle
  (start [{:keys [db-uri conn] :as component}]
    (if-not conn
      (let [db-exists? (d/create-database db-uri)]
        (when db-exists?
          (timbre/info (str "Created db " db-uri)))
        (let [conn (d/connect db-uri)]
          (doseq [schema (load-schemas)]
            (d/transact-async conn schema))
          (assoc component :conn conn)))
      component))
  (stop [{:keys [db-uri delete-on-stop? conn] :as component}]
    (if conn
      (do
        (when delete-on-stop?
          (timbre/info (str "Deleted db? " (d/delete-database db-uri))))
        (dissoc (update component :conn d/release) :conn))
      component)))

(defn create-db [{:keys [db-uri] :or {db-uri "datomic:mem://default"}}]
  (map->DatomicDB {:db-uri db-uri}))

(defrecord TRQ [datomic f])

(extend-type TRQ
  component/Lifecycle
  (start [{:keys [datomic f handler] :as component}]
    (if-not handler
      (let [{:keys [conn]} datomic
            ^BlockingQueue q (d/tx-report-queue conn)]
        (assoc component :handler (future (while true (f component (.take q))))))
      component))
  (stop [{:keys [handler] :as component}]
    (dissoc (cond-> component handler (update :handler future-cancel)) :handler)))

(defn create-trq [conf]
  (map->TRQ conf))

(defn create-system [conf]
  (component/system-map
    :datomic (create-db conf)
    :trq (component/using
           (create-trq (into {:f (fn [_ v] (pp/pprint [:first (first v)]))} conf))
           [:datomic])
    ;:trqa/fn (fn [_ v] (prn (str "Count " (count v))))
    ;:trqa (component/using (create-trq conf) {:datomic :datomic :f :trqa/fn})
    ))

(def ^:dynamic *db* (create-system {}))

(defn start [conf]
  (alter-var-root #'*db* (constantly (component/start-system (create-system conf)))))

(defn stop []
  (alter-var-root #'*db* component/stop-system))

(defn restart [conf]
  (do
    (stop)
    (start conf)))

