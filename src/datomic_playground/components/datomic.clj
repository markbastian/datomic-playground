(ns datomic-playground.components.datomic
  (:require [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]))

(defrecord DatomicDB [db-uri delete-on-stop?])

(extend-type DatomicDB
  component/Lifecycle
  (start [{:keys [db-uri] :as component}]
    (let [db-exists? (d/create-database db-uri)]
      (when db-exists?
        (timbre/info (str "Created db " db-uri)))
      (assoc component :conn (d/connect db-uri))))
  (stop [{:keys [db-uri delete-on-stop?] :as component}]
    (do
      (when delete-on-stop?
        (timbre/info (str "Deleted db? " (d/delete-database db-uri))))
      (-> component
          (update :conn d/release)
          (assoc :conn nil)))))

(defn create-db [{:keys [db-uri]}]
  (map->DatomicDB {:db-uri db-uri}))

(def ^:dynamic *db* (map->DatomicDB {}))

(defn start [conf]
  (alter-var-root #'*db* #(-> % (into conf) component/start)))

(defn stop []
  (alter-var-root #'*db* component/stop))