(ns dev.tx-trace
  "Tools for working with transaction sequences (as in open-tx-log)."
  (:require [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [clojure.java.io :as io]
            [xtdb.api :as xt])
  (:import (java.io DataOutputStream DataInputStream)
           (java.util ArrayList HashMap Date)
           (clojure.lang PersistentArrayMap)
           (java.util.zip GZIPOutputStream GZIPInputStream)))

(defn tx-seq
  "Returns a (lazy) seq of transactions that are committed on the node.

  The returned maps will have the same form as (open-tx-log)."
  ([node] (tx-seq node nil))
  ([node {:keys [basis]}]
   (let [basis-id (::xt/tx-id basis)]
     ((fn tx-seq-next [after]
        (lazy-seq
          (let [batch (with-open [c (xt/open-tx-log node after true)]
                        (into []
                              (comp (take-while #(not= (::xt/tx-id %) basis-id))
                                    (take 4096))
                              (iterator-seq c)))]
            (if (= 4096 (count batch))
              (concat batch (tx-seq-next (::xt/tx-id (peek batch))))
              batch))))
      basis-id))))

(defn- write-clj-tx
  [^DataOutputStream out,
   encode-fn
   tx]
  ;; begin tx
  (.write out (byte 1))
  (.writeLong out (long (::xt/tx-id tx)))
  (.writeLong out (long (inst-ms (::xt/tx-time tx))))
  (.writeInt out (count (::xt/tx-ops tx)))
  (doseq [op (::xt/tx-ops tx)]
    (case (nth op 0)
      ::xt/delete
      (let [[_ id valid-start valid-end] op]
        ;; delete
        (.write out (byte 2))

        (if valid-start
          (.writeLong out (long (inst-ms valid-start)))
          (.writeLong out -1))

        (if valid-end
          (.writeLong out (long (inst-ms valid-end)))
          (.writeLong out -1))

        (nippy/freeze-to-out! out id))
      ::xt/put
      (let [[_ doc valid-start valid-end] op]
        ;; put
        (.write out (byte 1))

        (if valid-start
          (.writeLong out (long (inst-ms valid-start)))
          (.writeLong out -1))

        (if valid-end
          (.writeLong out (long (inst-ms valid-end)))
          (.writeLong out -1))

        (encode-fn out doc))
      ;; skip, redundant for informational purposes
      ::xt/match (.write out (byte 0))))
  ;; end tx
  (.write out (byte 0)))

(defn- read-clj-tx
  [^DataInputStream in,
   decode-fn]
  (let [tx-id (.readLong in)
        tx-time (Date. (.readLong in))
        tx-op-count (.readInt in)
        op-buf (object-array tx-op-count)
        _ (dotimes [i tx-op-count]
            (case (int (.read in))
              0 nil
              1 (let [valid-start-ms (.readLong in)
                      valid-end-ms (.readLong in)]
                  (->> (cond
                         (and (= -1 valid-start-ms)
                              (= -1 valid-end-ms))
                         [::xt/put (decode-fn in)]

                         (= -1 valid-start-ms)
                         [::xt/put (decode-fn in) (Date. valid-start-ms)]

                         :else
                         [::xt/put (decode-fn in) (Date. valid-start-ms) (Date. valid-end-ms)])
                       (aset op-buf i)))
              2
              (let [valid-start-ms (.readLong in)
                    valid-end-ms (.readLong in)]
                (->> (cond
                       (and (= -1 valid-start-ms)
                            (= -1 valid-end-ms))
                       [::xt/delete (nippy/thaw-from-in! in)]

                       (= -1 valid-start-ms)
                       [::xt/delete (nippy/thaw-from-in! in) (Date. valid-start-ms)]

                       :else
                       [::xt/delete (nippy/thaw-from-in! in) (Date. valid-start-ms) (Date. valid-end-ms)])
                     (aset op-buf i)))))
        tx-ops (seq op-buf)]

    (when-not (= 0 (.read in)) (throw (ex-info "Corrupt xt stream" {})))

    {::xt/tx-id tx-id
     ::xt/tx-time tx-time
     ::xt/tx-ops tx-ops}))

(defn write-out [^DataOutputStream out tx-seq]
  (let [symbol-table (HashMap.)
        nippy nippy/freeze-to-out!
        encode-kv (fn [^DataOutputStream out k v]
                    (if-some [sym (.get symbol-table k)]
                      (do (.write out (byte 0))
                          (.writeInt out (int sym)))
                      (do (.write out (byte 1))
                          (let [off (.size symbol-table)]
                            (nippy out k)
                            (.put symbol-table k off))))
                    (nippy out v)
                    out)
        encode-fn (fn [^DataOutputStream out doc]
                    (.writeInt out (count doc))
                    (reduce-kv encode-kv out doc))]
    (doseq [tx tx-seq]
      (write-clj-tx out encode-fn tx))))

(defn write-file
  "Writes the transactions to the given file blocking until done, the file is compressed (zip).

  If you want to use difference compression, you can write directly to a DataOutputStream with write-out."
  [file tx-seq]
  (with-open [out (io/output-stream file)
              zout (GZIPOutputStream. out)
              dout (DataOutputStream. zout)]
    (write-out dout tx-seq)))

(defn read-in
  "Reads transactions, see (write-out) for inverse."
  [^DataInputStream in]
  (let [buf-size 64
        symbol-table (ArrayList.)
        nippy nippy/thaw-from-in!
        decode-fn (fn [^DataInputStream in]
                    (let [len (.readInt in)
                          buf (object-array (* 2 len))]
                      (dotimes [i len]
                        (aset buf (* i 2)
                              (if (= 0 (.read in))
                                (.get symbol-table (.readInt in))
                                (let [k (nippy in)]
                                  (.add symbol-table k)
                                  k)))
                        (aset buf (inc (* i 2)) (nippy in)))
                      (PersistentArrayMap. buf)))]
    ((fn read-next []
       (lazy-seq
         (loop [buf (object-array buf-size)]
           (loop [i 0]
             (cond
               (= i buf-size) (concat buf (read-next))
               (= 1 (.read in)) (do (aset buf i (read-clj-tx in decode-fn)) (recur (unchecked-inc-int i)))
               :else (take i buf)))))))))

(defn submit-file
  "Submits the transactions in the file (created with write-file) to the (presumed empty) node.

  Behaviour undefined if the node already contains data."
  [node file]
  (with-open [in (io/input-stream file)
              zin (GZIPInputStream. in)
              din (DataInputStream. zin)]
    (doseq [tx (read-in din)]
      (xt/submit-tx node (::xt/tx-ops tx) {::xt/tx-time (::xt/tx-time tx)}))))

(comment

  (def n (xt/start-node {}))

  (tx-seq n)

  (require 'xtdb.fixtures.tpch)
  (xtdb.fixtures.tpch/submit-docs! n 0.01 32)

  (xt/sync n)

  (first (tx-seq n))

  (write-file "tmp/tpch-0.01.tx-seq" (tx-seq n))

  (with-open [in (io/input-stream "tmp/tpch-0.01.tx-seq")
              zin (GZIPInputStream. in)
              din (DataInputStream. zin)]
    (first (read-in din)))

  (def n2 (xt/start-node {}))
  (submit-file n2 "tmp/tpch-0.01.tx-seq")
  (xt/sync n2)

  (count (tx-seq n))
  (count (tx-seq n2))

  (with-open [in (io/input-stream "tmp/tpch-0.01.tx-seq")
              zin (GZIPInputStream. in)
              din (DataInputStream. zin)]
    (= (tx-seq n)
       (tx-seq n2)
       (read-in din)))

  (.close n)
  (.close n2)

  )

(defn spread-tx-seq
  "Removes batching from the tx-sequence, the new transactions will contain one op each.

  Rewrites tx-time (new sequence starts at unix epoch). Valid time behaviour undefined.

  Options:

  :shuffle (bool, default false)

  Randomizes the new transaction order. "
  ([tx-seq] (spread-tx-seq {} tx-seq))
  ([{:keys [shuffle]} tx-seq]
   (->> tx-seq
        (mapcat ::xt/tx-ops)
        (remove nil?)
        ((fn [sq] (if shuffle (shuffle sq) sq)))
        (map-indexed (fn [i tx] {::xt/tx-id i, ::xt/tx-time (Date. (long i)), ::xt/tx-ops [tx]})))))

(defn batch-tx-seq
  "Re-batches the transactions so that each contains `n` docs.
  Rewrites tx-time (new sequence starts at unix epoch). Valid time behaviour undefined."
  [n tx-seq]
  (->> (spread-tx-seq tx-seq)
       (partition-all n)
       (map (fn [tx-batch]
              (assoc (select-keys (first tx-batch) [::xt/tx-time, ::xt/tx-id]) ::xt/tx-ops (into [] (mapcat ::xt/tx-ops) tx-batch))))))
