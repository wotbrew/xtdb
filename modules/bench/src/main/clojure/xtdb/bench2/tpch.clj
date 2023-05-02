(ns xtdb.bench2.tpch
  (:require [clojure.string :as str]
            [xtdb.api.impl :as api-impl]
            [xtdb.datalog :as xt]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.datasets.tpch.datalog :as tpch-datalog]
            [xtdb.node :as node]
            [xtdb.util :as util]
            [xtdb.ingester]
            [xtdb.test-util :as tu])
  (:import (java.io Closeable)
           (java.time Duration)
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
  (print "Ingesting" scale-factor "...") (flush)
  (tpch/submit-docs! node scale-factor)
  (println " ✅"))

(defn sync-node
  [{node :sut}
   {:keys [timeout]
    :or {timeout (relative-timeout default-scale-factor)}}]
  (print "Syncing...") (flush)
  @(.awaitTxAsync ^Ingester (util/component node :xtdb/ingester)
                  (api-impl/latest-submitted-tx node)
                  timeout)
  (println " ✅"))

(defn run-datalog-query [node n]
  (let [q-var (nth tpch-datalog/queries (dec n) nil)
        _ (assert q-var (str "no such query" n))
        {::tpch-datalog/keys [in-args]} (meta @q-var)]
    (count (apply xt/q node @q-var in-args))))

(defn run-ra-query [node n]
  (let [q-var (nth tpch-ra/queries (dec n) nil)
        {::tpch-ra/keys [params table-args]} (meta @q-var)
        q-opts {:node node, :params params, :table-args table-args}]
    (count (tu/query-ra @q-var q-opts))))

(defn run-query [node query-lang n & {:keys [rethrow]}]
  {:pre [(#{:ra :datalog} query-lang)]}
  (print "Running" "query" n "...") (flush)
  (try
    (case query-lang
      :ra (run-ra-query node n)
      :datalog (run-datalog-query node n))
    (println " ✅")
    (catch Throwable e
      (println " ❌ -" (list 'run-query 'n query-lang n :rethrow true))
      (when rethrow (throw e)))))

(defn benchmark
  [{:keys [query-lang
           scale-factor]
    :or {query-lang :ra
         scale-factor default-scale-factor}}]
  {:pre [(#{:ra, :datalog} query-lang)]}
  (let [ingest
        {:t :call
         :stage :ingest
         :f [ingest {:scale-factor scale-factor}]}

        sync
        {:t :call
         :stage :sync
         :f [sync-node {:timeout (relative-timeout scale-factor)}]}

        query-tasks
        (for [i (range 1 23)]
          {:t :call
           :stage (keyword (str "q" i))
           :f (fn [{node :sut}] (run-query node query-lang i))})]

    {:seed 0
     :tasks (into [ingest sync] query-tasks)}))

(comment

  ;; switch scale factor
  (def sf 0.05)
  (def sf 0.1)
  (def sf 0.25)
  (def sf 0.5)
  (def sf 1.0)

  (def n
    (do (when (bound? #'n) (.close ^Closeable n))
        (node/start-node {})))
  (.close n)

  ;; ingesting
  (ingest {:sut n} {:scale-factor sf})
  (sync-node {:sut n} {})

  ;; switch query lang
  (def ql :datalog)
  (def ql :ra)

  ;; running queries

  (defn t [i]
    (println ql sf)
    (time (run-query n ql i :rethrow true)))

  (t 1)
  (t 2)
  (t 3)
  (t 4)
  (t 5)
  (t 6)
  (t 7)
  (t 8)
  (t 9)
  (t 10)
  (t 11)
  (t 12)
  (t 13)
  (t 14)
  (t 15)
  (t 16)
  (t 17)
  (t 18)
  (t 19)
  (t 20)
  (t 21)
  (t 22)

  )

(comment

  (do

    (require '[clojure.java.io :as io])
    (require '[xtdb.bench2.xtdb2 :refer [run-benchmark]])
    (require '[xtdb.bench2.report :refer [show-html-report vs filter-stages]])

    (defn delete-directory-recursive
      "Recursively delete a directory."
      [^java.io.File file]
      (when (.exists file)
        (when (.isDirectory file)
          (run! delete-directory-recursive (.listFiles file)))
        (io/delete-file file)))

    (def node-dir (io/file "tmp/tpch-node"))
    (delete-directory-recursive node-dir)

    (defn qr [opts]
      (delete-directory-recursive node-dir)
      (run-benchmark
        {:node-opts {:node-dir (.toPath node-dir)}
         :benchmark-type :tpch
         :benchmark-opts opts}))

    (defn queries-only [report] (filter-stages report #(not= :ingest %)))

    )

  (def sf005-ra (qr {:scale-factor 0.05, :query-lang :ra}))
  (def sf005-datalog (qr {:scale-factor 0.05, :query-lang :datalog}))

  (show-html-report
    (vs
      "datalog"
      (queries-only sf005-datalog)
      "ra"
      (queries-only sf005-ra)))

  )
