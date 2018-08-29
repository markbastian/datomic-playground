(ns datomic-playground.olympics.system
  (:require [integrant.core :as ig]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]))

(defmethod ig/init-key :datomic/uri [_ {:keys [db-uri]}] db-uri)

(defmethod ig/init-key :datomic/create [_ {:keys [db-uri]}]
  (d/create-database db-uri))

(defmethod ig/init-key :datomic/delete [_ {:keys [db-uri]}] db-uri)

(defmethod ig/halt-key! :datomic/delete [_ db-uri]
  (if db-uri
    (do
      (timbre/debug (str "Deleting " db-uri "."))
      (d/delete-database db-uri))
    (timbre/debug "No db-uri defined for deletion in :datomic/delete.")))

(defmethod ig/init-key :datomic/conn [_ {:keys [db-uri]}]
  (d/connect db-uri))

(defmethod ig/halt-key! :datomic/conn [_ conn]
  (d/release conn))

(def datomic-config
  {[:olympics-v1/uri :datomic/uri]       {:db-uri "datomic:sql://olympics-v1?jdbc:postgresql://localhost:5432/datomic?user=datomic&password=datomic"}
   [:olympics-v1/create :datomic/create] {:db-uri (ig/ref [:olympics-v1/uri :datomic/uri])}
   [:olympics-v1/delete :datomic/delete] {:db-uri (ig/ref [:olympics-v1/uri :datomic/uri])}
   [:olympics-v1/conn :datomic/conn]     {:created? (ig/ref [:olympics-v1/create :datomic/create])
                                          :db-uri (ig/ref [:olympics-v1/uri :datomic/uri])}

   [:olympics-v2/uri :datomic/uri]       {:db-uri "datomic:sql://olympics-v2?jdbc:postgresql://localhost:5432/datomic?user=datomic&password=datomic"}
   [:olympics-v2/create :datomic/create] {:db-uri (ig/ref [:olympics-v2/uri :datomic/uri])}
   [:olympics-v2/delete :datomic/delete] {:db-uri (ig/ref [:olympics-v2/uri :datomic/uri])}
   [:olympics-v2/conn :datomic/conn]     {:created? (ig/ref [:olympics-v2/create :datomic/create])
                                          :db-uri (ig/ref [:olympics-v2/uri :datomic/uri])}

   [:olympics-v3/uri :datomic/uri]       {:db-uri "datomic:sql://olympics-v3?jdbc:postgresql://localhost:5432/datomic?user=datomic&password=datomic"}
   [:olympics-v3/create :datomic/create] {:db-uri (ig/ref [:olympics-v3/uri :datomic/uri])}
   [:olympics-v3/delete :datomic/delete] {:db-uri (ig/ref [:olympics-v3/uri :datomic/uri])}
   [:olympics-v3/conn :datomic/conn]     {:created? (ig/ref [:olympics-v3/create :datomic/create])
                                          :db-uri (ig/ref [:olympics-v3/uri :datomic/uri])}})

(defonce ^:dynamic *system* nil)
(defn system [] *system*)

(defn start
  ([system]
   (if (nil? (deref system))
     (alter-var-root system (constantly (ig/init datomic-config)))
     system))
  ([] (start #'*system*)))

(defn stop
  ([system] (when (deref system) (alter-var-root system ig/halt!)))
  ([] (stop #'*system*)))

(defn restart
  ([system] (doto system stop start))
  ([] (restart #'*system*)))
