(ns xtdb.log.watcher
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            xtdb.api.protocols
            xtdb.indexer
            [xtdb.log :as xt-log]
            xtdb.operator.scan
            [xtdb.util :as util]
            xtdb.watermark)
  (:import java.lang.AutoCloseable
           (java.nio ByteBuffer)
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.ipc.ArrowStreamReader
           org.apache.arrow.vector.TimeStampMicroTZVector
           xtdb.api.protocols.TransactionInstant
           xtdb.indexer.IIndexer
           [xtdb.log Log LogSubscriber]))

(defmethod ig/prep-key :xtdb.log/watcher [_ opts]
  (-> (merge {:allocator (ig/ref :xtdb/allocator)
              :log (ig/ref :xtdb/log)
              :indexer (ig/ref :xtdb/indexer)}
             opts)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defn- get-bb-long [^ByteBuffer buf ^long pos default]
  (if (< (+ pos 8) (.limit buf))
    (.getLong buf pos)
    default))

(defn- watch-log! [{:keys [^BufferAllocator allocator, ^Log log, ^IIndexer indexer]}]
  (let [!cancel-hook (promise)]
    (.subscribe log
                (:tx-id (.latestCompletedTx indexer))
                (reify LogSubscriber
                  (onSubscribe [_ cancel-hook]
                    (deliver !cancel-hook cancel-hook))

                  (acceptRecord [_ record]
                    (if (Thread/interrupted)
                      (throw (InterruptedException.))

                      (condp = (Byte/toUnsignedInt (.get ^ByteBuffer (.-record record) 0))
                        xt-log/hb-user-arrow-transaction
                        (with-open [tx-ops-ch (util/->seekable-byte-channel (.-record record))
                                    sr (ArrowStreamReader. tx-ops-ch allocator)
                                    tx-root (.getVectorSchemaRoot sr)]
                          (.loadNextBatch sr)

                          (let [^TimeStampMicroTZVector system-time-vec (.getVector tx-root "system-time")
                                ^TransactionInstant tx-key (cond-> (.tx record)
                                                             (not (.isNull system-time-vec 0))
                                                             (assoc :system-time (-> (.get system-time-vec 0) (util/micros->instant))))]

                            (.indexTx indexer tx-key tx-root (.limit (.-record record)))))

                        xt-log/hb-flush-chunk
                        (let [expected-chunk-tx-id (get-bb-long (:record record) 1 -1)]
                          (log/debugf "received flush-chunk signal: %d" expected-chunk-tx-id)
                          (.forceFlush indexer (:tx record) expected-chunk-tx-id))

                        (throw (IllegalStateException. (format "Unrecognized log record type %d" (Byte/toUnsignedInt (.get ^ByteBuffer (.-record record) 0))))))))))
    !cancel-hook))

(defmethod ig/init-key :xtdb.log/watcher [_ {:keys [allocator] :as deps}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "watcher")]
    (let [!watcher-cancel-hook (watch-log! (-> deps
                                               (assoc :allocator allocator)))]


      (reify
        AutoCloseable
        (close [_]
          (util/try-close @!watcher-cancel-hook)
          (util/close allocator))))))

(defmethod ig/halt-key! :xtdb.log/watcher [_ log-watcher]
  (util/close log-watcher))
