(ns dev.index-trace
  (:require [xtdb.memory :as mem]
            [xtdb.kv :as kv]
            [xtdb.io :as xio]
            [xtdb.system :as sys]
            [clojure.java.io :as io]
            [xtdb.api :as xt])
  (:import (java.io DataOutputStream Closeable FileOutputStream)
           (java.util.zip GZIPOutputStream)
           (java.nio.file Path)))

(defn- traced-kv
  [kv-store ^DataOutputStream trace-out close-hook]
  (let [write-kv (fn [^DataOutputStream out k v]
                   (let [k-bytes (mem/->on-heap k)
                         k-len (alength k-bytes)
                         ^bytes v-bytes (when (some? v) (mem/->on-heap v))]
                     (.writeInt out k-len)
                     (.write out k-bytes)
                     (.writeInt out (if v-bytes (alength v-bytes) -1))
                     (when v-bytes (.write out v-bytes))))]
    (reify kv/KvStore
      (new-snapshot [this]
        (kv/new-snapshot kv-store))
      (store [this kvs]
        (when (seq kvs)
          (locking trace-out
            (let [ret (kv/store kv-store kvs)]
              (.write trace-out (byte 1))
              (.writeInt trace-out (count kvs))
              (doseq [[k v] kvs] (write-kv trace-out k v))
              (.write trace-out (byte 0))
              ret))))
      (fsync [this] (kv/fsync kv-store))
      (compact [this] (kv/compact kv-store))
      (count-keys [this] (kv/count-keys kv-store))
      (db-dir [this] (kv/db-dir kv-store))
      (kv-name [this] (kv/kv-name kv-store))
      Closeable
      (close [_] (xio/try-close kv-store) (close-hook)))))

(defn traced-kv-store
  "XTDB module for tracing a kv, include :kv-store link as a dep, :file should
  be the file you want to contain the kv trace."
  {::sys/deps {:kv-store `xtdb.mem-kv/->kv-store}
   ::sys/args {:file {:spec ::sys/path
                      :required? true}}}
  [{:keys [kv-store, ^Path file]}]

  ;; deps should be prepped
  (assert kv-store)
  (assert file)

  (let [out (FileOutputStream. (.toFile file))
        #_#_zout (GZIPOutputStream. out)
        dout (DataOutputStream. out)]
    (traced-kv
      kv-store
      dout
      (fn [] (.close dout) #_(.close zout) (.close out)))))

(comment

  (sys/prep-system {:kv {:xtdb/module `traced-kv-store
                         :file "tmp/foo.index-trace"}})


  (sys/prep-system
    {::actual-kv-store {:xtdb/module 'xtdb.mem-kv/->kv-store}
     ::traced-kv-store {:xtdb/module `traced-kv-store
                        :file "tmp/tpch-0.01.index-trace"
                        :kv-store ::actual-kv-store}
     :xtdb/index-store {:xtdb/module 'xtdb.kv.index-store/->kv-index-store
                        :kv-store ::traced-kv-store}})

  (def n
    (xt/start-node {}))


  (require 'xtdb.fixtures.tpch)
  (xtdb.fixtures.tpch/submit-docs! n 0.01 32)
  (xt/sync n)

  ;; build a tx seq

  (require 'dev.tx-trace)
  (dev.tx-trace/write-file "tmp/tpch-0.01.tx-trace" (dev.tx-trace/tx-seq n))
  (.close n)

  ;; start two nodes
  (def n1
    (xt/start-node {::actual-kv-store {:xtdb/module 'xtdb.mem-kv/->kv-store}
                    ::traced-kv-store {:xtdb/module `traced-kv-store
                                       :file "tmp/tpch-0.01.index-trace1"
                                       :kv-store ::actual-kv-store}
                    :xtdb/tx-ingester {:deterministic-writes true}
                    :xtdb/index-store {:kv-store ::traced-kv-store}}))

  (def n2
    (xt/start-node {::actual-kv-store {:xtdb/module 'xtdb.mem-kv/->kv-store}
                    ::traced-kv-store {:xtdb/module `traced-kv-store
                                       :file "tmp/tpch-0.01.index-trace2"
                                       :kv-store ::actual-kv-store}
                    :xtdb/tx-ingester {:deterministic-writes true}
                    :xtdb/index-store {:kv-store ::traced-kv-store}}))

  ;; seed them with the same transactions
  (dev.tx-trace/submit-file n1 "tmp/tpch-0.01.tx-trace")
  (dev.tx-trace/submit-file n2 "tmp/tpch-0.01.tx-trace")
  (xt/sync n1)
  (xt/sync n2)

  (xt/attribute-stats n1)
  (xt/attribute-stats n2)

  (= (xt/attribute-stats n1) (xt/attribute-stats n2))

  ;; close after sync and (little wait for stats)
  (.close n1)
  (.close n2)

  (defn file-size [s]
    (java.nio.file.Files/size (.toPath (io/file s))))

  (file-size "tmp/tpch-0.01.index-trace1")
  (file-size "tmp/tpch-0.01.index-trace2")







  )
