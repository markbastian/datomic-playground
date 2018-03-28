(ns datomic-playground.api
  (:require [datomic.api :as d]))

(defn transact [{:keys [conn]} data]
  (d/transact conn data))

(defn db [{:keys [conn]}]
  (d/db conn))