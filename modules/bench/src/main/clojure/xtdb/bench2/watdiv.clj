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

(defn rewrite-value [v]
  (if (set? v) (vec v) v))

(defn xtdb2-doc [watdiv-doc]
  (let [id (:_id watdiv-doc)
        doc' (dissoc watdiv-doc :_id)]
    (into {:xt/id id} (map (fn [[a v]] [(rewrite-attribute a) (rewrite-value v)])) doc')))

(defn xtdb2-put [watdiv-doc] [:put :xt_docs (xtdb2-doc watdiv-doc)])

(defn doc-attribute-cardinality [doc]
  (update-vals doc #(if (coll? %) :many :one)))

(defn cardinality-schema [docs]
  (reduce (partial merge-with (fn [a b] (if (= a b) a :mixed))) {} (map doc-attribute-cardinality docs)))

(def memo-gensym (memoize (fn [& p] (gensym (apply str p)))))

(defn value-bindings [v-sym card]
  (case card
    :one {:match v-sym}
    :many {:match [v-sym '...]}
    :mixed
    (let [match-sym (memo-gensym v-sym 'mixed-card-binding)]
      {:match match-sym
       :project [match-sym [v-sym '...]]})))

(defn where->xtdb-where [where card-schema]
  (assert (every? #(and (vector? %) (= 3 (count %))) where) "only triples supported")
  (let [e-vars (mapv #(nth % 0) where)
        e-var-indexes (zipmap e-vars (range))]
    (->> where
         (group-by #(nth % 0))
         (sort-by (comp e-var-indexes key))
         (mapcat (fn [[e-var triples]]
                   (let [v-binds (mapv (fn [[_ a v]] (value-bindings v (card-schema (rewrite-attribute a)))) triples)]
                     (->> (keep :project v-binds)
                          (into [(list '$ :xt_docs (into {:xt/id e-var} (map (fn [{:keys [match]} [_ a]] [(rewrite-attribute a) match]) v-binds triples)))])))))
         vec)))

(defn query->xtdb2-datalog [query card-schema]
  (update query :where where->xtdb-where card-schema))

(defn ingest [node doc-file]
  (let [xf (comp (map xtdb2-put)
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
  (map xtdb2-put (sample-edn 5 dev-docs-file))

  ;; sample queries
  (map query->xtdb2-datalog (sample-edn 5 dev-query-file))

  (def all-docs (mapv xtdb2-doc (edn-reducable dev-docs-file)))
  (count all-docs)
  (def cs (cardinality-schema all-docs))

  (def all-queries (mapv #(query->xtdb2-datalog % cs) (edn-reducable dev-query-file)))
  (count all-queries)

  (def n (node/start-node {}))
  (.close n)

  (time (ingest n dev-docs-file))

  ;; docs have sets in them, causes a meta crash.
  (count (d/q n (first all-queries)))

  (cardinality-schema (map xtdb2-doc all-docs))

  )
