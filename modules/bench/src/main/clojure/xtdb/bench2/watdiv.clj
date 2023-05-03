(ns xtdb.bench2.watdiv
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [xtdb.bench :as bench]
            [xtdb.datalog :as d]
            [xtdb.node :as node])
  (:import (clojure.lang IReduceInit)
           (java.io PushbackReader)))

(def docs-s3-path "watdiv/watdiv-10M.edn")
(def query-s3-path "watdiv/watdiv-stress-100/test.1.edn")

(defn download [s3-path file]
  (print "Downloading" s3-path "to" (str (io/file file)) "...") (flush)
  (bench/download-s3-dataset-file s3-path (.toPath (io/file file)))
  (println "Done!"))

(defn edn-seq [^PushbackReader rdr]
  (lazy-seq
    (let [eof (Object.)
          edn (edn/read {:eof eof} rdr)]
      (when-not (identical? eof edn)
        (cons edn (edn-seq rdr))))))

(defn edn-reducable [file]
  (reify IReduceInit
    (reduce [_ f init]
      (with-open [rdr (io/reader file)
                  pbr (PushbackReader. rdr)]
        (reduce f init (edn-seq pbr))))))

(defn sample-edn [n file]
  (into [] (take n) (edn-reducable file)))

(defn rewrite-attribute [a]
  (if (keyword? a)
    a
    (keyword (munge (str a)))))

(defn doc->xtdb2-put [doc]
  (let [id (:_id doc)
        attributes (dissoc doc :_id)]
    [:put :xt_docs (assoc (update-keys attributes rewrite-attribute) :xt/id id)]))

(defn where->xtdb-where [where]
  (assert (every? #(and (vector? %) (= 3 (count %))) where) "only triples supported")
  (let [e-vars (mapv #(nth % 0) where)
        e-var-indexes (zipmap e-vars (range))]
    (->> where
         (group-by #(nth % 0))
         (sort-by (comp e-var-indexes key))
         (mapv (fn [[e-var triples]]
                 (list '$ :xt_docs (into {e-var :xt/id} (map (fn [[_ a v]] [v a])) triples)))))))

(defn query->xtdb2-datalog [query]
  (update query :where where->xtdb-where))

(defn ingest [node doc-file]
  (let [xf (comp (map doc->xtdb2-put)
                 (partition-all 512))]
    (->> (eduction xf (edn-reducable doc-file))
         (run! #(d/submit-tx node %)))))

(comment

  (def dev-docs-file (io/file "tmp/watdiv-docs.edn"))
  (def dev-query-file (io/file "tmp/watdiv-queries.edn"))

  (when-not (.exists dev-docs-file)
    (download docs-s3-path dev-docs-file))

  (when-not (.exists dev-query-file)
    (download query-s3-path dev-query-file))

  ;; sample docs
  (sample-edn 5 dev-docs-file)

  ;; sample queries
  (sample-edn 5 dev-query-file)

  ;; sample puts
  (map doc->xtdb2-put (sample-edn 5 dev-docs-file))

  ;; sample queries
  (map query->xtdb2-datalog (sample-edn 5 dev-query-file))

  (def all-docs (vec (edn-reducable dev-docs-file)))
  (count all-docs)

  (def all-queries (vec (edn-reducable dev-query-file)))
  (count all-queries)
  (count (map query->xtdb2-datalog all-queries))

  (def n (node/start-node {}))
  (time (ingest n dev-docs-file))

  ;; docs have sets in them, causes a meta crash.
  (count (d/q n (query->xtdb2-datalog (first all-queries))))

  )
