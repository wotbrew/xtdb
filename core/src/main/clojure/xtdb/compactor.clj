(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            xtdb.buffer-pool
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util.function IntPredicate)
           [org.apache.arrow.memory BufferAllocator]
           org.apache.arrow.vector.types.pojo.Field
           org.apache.arrow.vector.VectorSchemaRoot
           [org.roaringbitmap RoaringBitmap]
           xtdb.buffer_pool.IBufferPool
           (xtdb.metadata IMetadataManager IMetadataPredicate)
           xtdb.object_store.ObjectStore
           (xtdb.trie ILeafLoader LiveHashTrie)
           xtdb.util.WritableByteBufferChannel
           xtdb.vector.IRowCopier))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ICompactor
  (^void compactAll []))

(defn- ->log-leaf-schema [leaves]
  (trie/log-leaf-schema (->> (for [^ILeafLoader leaf leaves]
                               (-> (.getSchema leaf)
                                   (.findField "op")
                                   (.getChildren) ^Field first
                                   (.getChildren) ^Field last
                                   types/field->col-type))
                             (apply types/merge-col-types))))

(defn merge-tries! [^BufferAllocator allocator, leaves, leaf-out-ch, trie-out-ch, merge-plan]
  (let [log-leaf-schema (->log-leaf-schema leaves)]
    (util/with-open [trie-wtr (trie/open-trie-writer allocator log-leaf-schema
                                                     leaf-out-ch trie-out-ch)

                     leaf-root (VectorSchemaRoot/create log-leaf-schema allocator)]

      (let [leaf-wtr (vw/root->writer leaf-root)]
        (letfn [(merge-tries* [{:keys [path], [node-tag node-arg] :node}]
                  (case node-tag
                    :branch (let [idxs (mapv merge-tries* node-arg)]
                              (.writeBranch trie-wtr (int-array idxs)))

                    :leaf (let [loaded-leaves (trie/load-leaves leaves {:leaves node-arg})
                                merge-q (trie/->merge-queue (mapv :rel-rdr loaded-leaves) loaded-leaves {:path path})

                                ^"[Lxtdb.vector.IRowCopier;"
                                row-copiers (->> (for [{:keys [rel-rdr]} loaded-leaves]
                                                   (.rowCopier leaf-wtr rel-rdr))
                                                 (into-array IRowCopier))]
                            (loop [trie (-> (doto (LiveHashTrie/builder (vr/vec->reader (.getVector leaf-root "xt$iid")))
                                              (.setRootPath path))
                                            (.build))]
                              (if-let [lp (.poll merge-q)]
                                (let [pos (.copyRow ^IRowCopier (aget row-copiers (.getOrdinal lp))
                                                    (.getIndex lp))]

                                  (.advance merge-q lp)
                                  (recur (.add trie pos)))

                                (let [pos (trie/write-live-trie trie-wtr trie (vw/rel-wtr->rdr leaf-wtr))]
                                  (.clear leaf-root)
                                  (.clear leaf-wtr)
                                  pos))))))]

          (merge-tries* merge-plan)

          (.end trie-wtr))))))

(defn exec-compaction-job! [^BufferAllocator allocator, ^ObjectStore obj-store, ^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool,
                            {:keys [table-name table-tries out-trie-key]}]
  (try
    (log/infof "compacting '%s' '%s' -> '%s'..." table-name (mapv :trie-key table-tries) out-trie-key)
    (util/with-open [roots (trie/open-arrow-trie-files buffer-pool table-tries)
                     leaves (trie/open-leaves buffer-pool table-name table-tries nil)
                     leaf-out-bb (WritableByteBufferChannel/open)
                     trie-out-bb (WritableByteBufferChannel/open)]

      (merge-tries! allocator leaves
                    (.getChannel leaf-out-bb) (.getChannel trie-out-bb)
                    (trie/table-merge-plan (constantly true)
                                           (meta/matching-tries metadata-mgr table-tries roots
                                                                (reify IMetadataPredicate
                                                                  (build [_ _table-metadata]
                                                                    (reify IntPredicate
                                                                      (test [_ _page-idx]
                                                                        true)))))
                                           nil))

      (log/debugf "uploading '%s' '%s'..." table-name out-trie-key)

      @(.putObject obj-store (trie/->table-leaf-obj-key table-name out-trie-key)
                   (.getAsByteBuffer leaf-out-bb))
      @(.putObject obj-store (trie/->table-trie-obj-key table-name out-trie-key)
                   (.getAsByteBuffer trie-out-bb)))

    (log/infof "compacted '%s' -> '%s'." table-name out-trie-key)

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defn compaction-jobs [table-name table-tries]
  (for [[level table-tries] (->> (trie/current-table-tries table-tries)
                                 (group-by :level))
        job (partition 4 table-tries)]
    {:table-name table-name
     :table-tries job
     :out-trie-key (trie/->trie-key (inc level)
                                    (:row-from (first job))
                                    (:row-to (last job)))}))

(defmethod ig/prep-key :xtdb/compactor [_ opts]
  (into {:allocator (ig/ref :xtdb/allocator)
         :obj-store (ig/ref :xtdb/object-store)
         :buffer-pool (ig/ref :xtdb.buffer-pool/buffer-pool)
         :metadata-mgr (ig/ref :xtdb.metadata/metadata-manager)}
        opts))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [allocator ^ObjectStore obj-store metadata-mgr buffer-pool]}]
  (reify ICompactor
    (compactAll [_]
      (log/info "compact-all")
      (loop []
        (let [jobs (for [table-name (->> (.listObjects obj-store "tables")
                                         ;; TODO should obj-store listObjects only return keys from the current level?
                                         (into #{} (keep #(second (re-find #"^tables/([^/]+)" %)))))
                         job (compaction-jobs table-name (->> (trie/list-table-trie-files obj-store table-name)
                                                              (trie/current-table-tries)))]
                     job)
              jobs? (boolean (seq jobs))]

          (doseq [job jobs]
            (exec-compaction-job! allocator obj-store metadata-mgr buffer-pool job))

          (when jobs?
            (recur)))))))

(defmethod ig/halt-key! :xtdb/compactor [_ _compactor])

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn compact-all! [node]
  (let [^ICompactor compactor (util/component node :xtdb/compactor)]
    (.compactAll compactor)))
