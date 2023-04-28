(ns xtdb.bench2.tpch
  (:require [clojure.string :as str]
            [xtdb.api.impl :as xt.impl]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.util :as util]
            [xtdb.ingester]
            [xtdb.test-util :as tu])
  (:import (java.time Duration)
           (xtdb.ingester Ingester)
           (clojure.lang Var)))

(def default-scale-factor 0.05)

(defn relative-timeout ^Duration [scale-factor]
  (Duration/ofMillis (* 1000 60 10 (/ scale-factor 0.05))))

(comment
  (relative-timeout default-scale-factor)
  )

(defn ingest
  [{node :sut}
   {:keys [scale-factor]
    :or {scale-factor default-scale-factor}}]
  (println "Ingesting")
  (tpch/submit-docs! node scale-factor))

(defn sync-node
  [{node :sut}
   {:keys [timeout]
    :or {timeout (relative-timeout default-scale-factor)}}]
  (println "Syncing")
  @(.awaitTxAsync ^Ingester (util/component node :xtdb/ingester)
                  (xt.impl/latest-submitted-tx node)
                  timeout))

(defn benchmark
  [{:keys [query-lang
           scale-factor]
    :or {query-lang :ra
         scale-factor default-scale-factor}}]
  {:pre [(#{:ra} query-lang)]}
  (let [ingest
        {:t :call
         :stage :ingest
         :f [ingest {:scale-factor scale-factor}]}

        sync
        {:t :call
         :stage :sync
         :f [sync-node {:timeout (relative-timeout scale-factor)}]}

        query-tasks
        (case query-lang
          :ra
          (for [q-var tpch-ra/queries
                :let [sym (.toSymbol ^Var q-var)]]
            {:t :call
             :stage (keyword (first (str/split (name sym) #"\-")))
             :f (fn [{node :sut}]
                  (try
                    (println "Running" sym)
                    (let [{::tpch-ra/keys [params table-args]} (meta @q-var)]
                      (println "Returned" (count (tu/query-ra @q-var {:node node
                                                           :params params
                                                           :table-args table-args}))))
                    (catch Throwable e
                      (println e))))}))]
    {:seed 0
     :tasks (into [ingest sync] query-tasks)}))
