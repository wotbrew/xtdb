(ns xtdb.bench2.rocksdb
  (:require [xtdb.bench2 :as b2])
  (:import (org.rocksdb Statistics TickerType)
           (java.util ArrayList)
           (java.time Duration)
           (java.util.concurrent Executors TimeUnit)))

(defn statistics-sampler [stat-instances]
  (let [sample-list (ArrayList.)]
    (fn send-msg
      ([] (send-msg :sample))
      ([msg]
       (case msg
         :summarize
         (let [{tickers :ticker} (group-by :kind sample-list)]
           (vec (for [[[kv-name ticker] samples] (group-by (juxt :kv-name :ticker) tickers)]
                  {:id (str "rocksdb." (name kv-name) "." (str ticker))
                   :metric (str "rocksdb." (name kv-name) "." (str ticker))
                   :meter (str "rocksdb." (name kv-name) "." (str ticker))
                   :unit "count"
                   :samples (mapv (fn [{:keys [measurement, time-ms]}]
                                    {:value measurement
                                     :time-ms time-ms})
                                  samples)})))
         (let [time-ms (System/currentTimeMillis)]
           (doseq [[k ^Statistics stats] stat-instances
                   ticker [TickerType/BLOCK_CACHE_MISS
                           TickerType/BLOCK_CACHE_HIT
                           TickerType/BLOCK_CACHE_BYTES_READ
                           TickerType/BLOCK_CACHE_BYTES_WRITE]
                   :let [measurement (.getTickerCount stats ^TickerType ticker)]]
             (.add sample-list {:kind :ticker, :kv-name k, :ticker ticker, :time-ms time-ms, :measurement measurement}))))))))

(defn stage-wrapper
  [get-statistics-fn]
  (fn wrap-stage [task f]
   (let [stage (:stage task)
         ^Duration duration (:duration task)
         sample-freq
         (if duration
           (long (max 1000 (/ (* (.toMillis duration)) 120)))
           1000)]
     (fn instrumented-stage [worker]
       (if-some [stat-instances (get-statistics-fn worker)]
         (let [sampler (statistics-sampler stat-instances)
               executor (Executors/newSingleThreadScheduledExecutor)]
           (.scheduleAtFixedRate
             executor
             ^Runnable (let [bindings (get-thread-bindings)]
                         (fn []
                           (push-thread-bindings bindings)
                           (try
                             (sampler)
                             (finally (pop-thread-bindings)))))
             0
             (long sample-freq)
             TimeUnit/MILLISECONDS)
           (try
             (let [start-ms (System/currentTimeMillis)]
               (f worker)
               (.shutdownNow executor)
               (when-not (.awaitTermination executor 1000 TimeUnit/MILLISECONDS)
                 (throw (ex-info "Could not shut down sampler executor in time" {:stage stage})))
               (b2/add-report worker {:stage stage,
                                      :start-ms start-ms
                                      :end-ms (System/currentTimeMillis)
                                      :metrics (sampler :summarize)}))
             (finally
               (.shutdownNow executor))))
         (do
           (println "WARN rocks stage wrapper could not find stats via (get-statistics-fn worker)")
           (f worker)))))))
