(ns xtdb.bench2.core1
  (:require [xtdb.api :as xt]
            [xtdb.bench2.measurement :as bm]
            [xtdb.bus :as bus]
            [xtdb.bench2 :as b2]
            [xtdb.bench2.tools :as bt]
            [xtdb.bench2.ec2 :as ec2]
            [xtdb.io :as xio]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.kv :as kv]
            [xtdb.memory :as mem]
            [clojure.spec.alpha :as s])
  (:import (java.time Duration)
           (java.util.concurrent.atomic AtomicLong)
           (java.io Closeable File DataOutputStream DataInputStream)
           (java.util Date HashMap ArrayList)
           (clojure.lang PersistentArrayMap)
           (java.security MessageDigest)
           (io.micrometer.core.instrument MeterRegistry Timer Tag Timer$Sample)))

(set! *warn-on-reflection* false)

(defn generate [worker f n]
  (let [doc-seq (remove nil? (repeatedly (long n) (partial f worker)))
        partition-count 512]
    (doseq [chunk (partition-all partition-count doc-seq)]
      (xt/submit-tx (:sut worker) (mapv (partial vector ::xt/put) chunk)))))

(defn install-tx-fns [worker fns]
  (->> (for [[id fn-def] fns]
         [::xt/put {:xt/id id, :xt/fn fn-def}])
       (xt/submit-tx (:sut worker))))

(defn install-proxy-node-meters!
  [^MeterRegistry meter-reg]
  (let [timer #(-> (Timer/builder %)
                   (.minimumExpectedValue (Duration/ofNanos 1))
                   (.maximumExpectedValue (Duration/ofMinutes 2))
                   (.publishPercentiles (double-array bm/percentiles))
                   (.register meter-reg))]
    {:submit-tx-timer (timer "node.submit-tx")
     :query-timer (timer "node.query")}))

(defmacro reify-protocols-accepting-non-methods
  "On older versions of XT node methods may be missing."
  [& reify-def]
  `(reify ~@(loop [proto nil
                   forms reify-def
                   acc []]
              (if-some [form (first forms)]
                (cond
                  (symbol? form)
                  (if (class? (resolve form))
                    (recur nil (rest forms) (conj acc form))
                    (recur form (rest forms) (conj acc form)))

                  (nil? proto)
                  (recur nil (rest forms) (conj acc form))

                  (list? form)
                  (if-some [{:keys [arglists]} (get (:sigs @(resolve proto)) (keyword (name (first form))))]
                    ;; arity-match
                    (if (some #(= (count %) (count (second form))) arglists)
                      (recur proto (rest forms) (conj acc form))
                      (recur proto (rest forms) acc))
                    (recur proto (rest forms) acc)))
                acc))))

(defn bench-proxy ^Closeable [node ^MeterRegistry meter-reg]
  (let [last-submitted (atom nil)
        last-completed (atom nil)

        ;; todo hook into dropwizard perhaps
        ;; and delete these?
        submit-counter (AtomicLong.)
        indexed-counter (AtomicLong.)
        indexed-docs-counter (AtomicLong.)
        indexed-bytes-counter (AtomicLong.)
        indexed-av-counter (AtomicLong.)

        _
        (doto meter-reg
          (.gauge "node.tx" ^Iterable [(Tag/of "event" "submitted")] submit-counter)
          (.gauge "node.tx" ^Iterable [(Tag/of "event" "indexed")] indexed-counter)
          (.gauge "node.indexed.docs" indexed-docs-counter)
          (.gauge "node.indexed.bytes" indexed-bytes-counter)
          (.gauge "node.indexed.av" indexed-av-counter))

        {:keys [^Timer submit-tx-timer
                ^Timer query-timer]}
        (install-proxy-node-meters! meter-reg)

        fn-gauge (partial bm/new-fn-gauge meter-reg)

        on-indexed
        (fn [{:keys [submitted-tx, doc-ids, av-count, bytes-indexed] :as event}]
          (reset! last-completed submitted-tx)
          (when (seq doc-ids)
            (.getAndAdd indexed-docs-counter (count doc-ids))
            (.getAndAdd indexed-av-counter (long av-count))
            (.getAndAdd indexed-bytes-counter (long bytes-indexed)))
          (.getAndIncrement indexed-counter)
          nil)

        on-indexed-listener
        (bus/listen (:bus node) {::xt/event-types #{:xtdb.tx/indexed-tx}} on-indexed)

        on-query
        (let [st (atom {})
              start
              (fn [query-id] (swap! st assoc query-id (Timer/start)))
              stop
              (fn [query-id]
                (.stop ^Timer$Sample (@st query-id) query-timer)
                (swap! st dissoc query-id))]
          (fn [{:xtdb.query/keys [query-id]
                ::xt/keys [event-type]}]
            (if (identical? :xtdb.query/submitted-query event-type)
              (start query-id)
              (stop query-id))))

        on-query-events
        #{:xtdb.query/submitted-query
          :xtdb.query/failed-query
          :xtdb.query/completed-query}

        on-query-listener
        (bus/listen (:bus node) {::xt/event-types on-query-events} on-query)

        compute-lag-nanos
        (fn []
          (or
            (when-some [[{::xt/keys [tx-id]} ms] @last-submitted]
              (when-some [{completed-tx-id ::xt/tx-id
                           completed-tx-time ::xt/tx-time} @last-completed]
                (when (< completed-tx-id tx-id)
                  (* (long 1e6) (- ms (inst-ms completed-tx-time))))))
            0))]

    (fn-gauge "node.tx.lag" (comp #(/ % 1e9) compute-lag-nanos) {:unit "seconds"})
    (fn-gauge "node.kv.keys" #(:xtdb.kv/estimate-num-keys (xt/status node) 0) {:unit "count"})
    (fn-gauge "node.kv.size" #(:xtdb.kv/size (xt/status node) 0) {:unit "bytes"})

    (reify-protocols-accepting-non-methods
      xt/PXtdb
      (status [_] (xt/status node))
      (tx-committed? [_ submitted-tx] (xt/tx-committed? node submitted-tx))
      (sync [_] (xt/sync node))
      (sync [_ timeout] (xt/sync node timeout))
      (sync [_ tx-time timeout] (xt/sync node tx-time timeout))
      (await-tx-time [_ tx-time] (xt/await-tx-time node tx-time))
      (await-tx-time [_ tx-time timeout] (xt/await-tx-time node tx-time timeout))
      (await-tx [_ tx] (xt/await-tx node tx))
      (await-tx [_ tx timeout] (xt/await-tx node tx timeout))
      (listen [_ event-opts f] (xt/listen node event-opts f))
      (latest-completed-tx [_] (xt/latest-completed-tx node))
      (latest-submitted-tx [_] (xt/latest-submitted-tx node))
      (attribute-stats [_] (xt/attribute-stats node))
      (active-queries [_] (xt/active-queries node))
      (recent-queries [_] (xt/recent-queries node))
      (slowest-queries [_] (xt/slowest-queries node))
      xt/PXtdbSubmitClient
      (submit-tx-async [_ tx-ops] (xt/submit-tx-async node tx-ops))
      (submit-tx-async [_ tx-ops opts] (xt/submit-tx-async node tx-ops opts))

      ;; record on both branches as older versions of xt do not have the arity-3 version
      (submit-tx [this tx-ops]
        (let [ret (.recordCallable ^Timer submit-tx-timer ^Callable (fn [] (xt/submit-tx node tx-ops)))]
          (reset! last-submitted [ret (System/currentTimeMillis)])
          (.incrementAndGet submit-counter)
          ret))
      (submit-tx [_ tx-ops opts]
        (let [ret (.recordCallable ^Timer submit-tx-timer ^Callable (fn [] (xt/submit-tx node tx-ops opts)))]
          (reset! last-submitted [ret (System/currentTimeMillis)])
          (.incrementAndGet submit-counter)
          ret))

      (open-tx-log [_ after-tx-id with-ops?] (xt/open-tx-log node after-tx-id with-ops?))
      xt/DBProvider
      (db [_] (xt/db node))
      (db [_ valid-time-or-basis] (xt/db node valid-time-or-basis))
      (open-db [_] (xt/open-db node))
      (open-db [_ valid-time-or-basis] (xt/open-db node valid-time-or-basis))
      Closeable
      (close [_]
        (.close on-query-listener)
        (.close on-indexed-listener)))))

(defn wrap-task [task f]
  (let [{:keys [stage]} task]
    (bm/wrap-task
      task
      (if stage
        (fn instrumented-stage [worker]
          (if bm/*stage-reg*
            (with-open [node-proxy (bench-proxy (:sut worker) bm/*stage-reg*)]
              (f (assoc worker :sut node-proxy)))
            (f worker)))
        f))))

(defn quick-run [benchmark node-opts]
  (let [f (b2/compile-benchmark benchmark wrap-task)]
    (with-open [node (xt/start-node node-opts)]
      (f node))))

(defn install-sha-artifacts-to-m2 [repository sha]
  (let [tmp-dir (xio/create-tmpdir "benchmark-xtdb")]
    (bt/sh-ctx
      {:dir tmp-dir
       :env {"XTDB_VERSION" sha}}
      (bt/log "Fetching xtdb ref" sha "from" repository)
      (bt/sh "git" "init")
      (bt/sh "git" "remote" "add" "origin" repository)
      (bt/sh "git" "fetch" "origin" "--depth" "1" sha)
      (bt/sh "git" "checkout" sha)
      (bt/log "Installing jars for build")
      (bt/sh "sh" "lein-sub" "install")
      (.delete tmp-dir))))

(defn sha-artifacts-exist-in-m2? [sha]
  (.exists (io/file (System/getenv "HOME") ".m2" "repository" "com" "xtdb" "xtdb-core" sha)))

(defn sut-module-deps [{:keys [modules, sha]}]
  (distinct
    (for [impl modules
          [nm ver :as dep] (case impl
                             :rocks [['com.xtdb/xtdb-rocksdb]]
                             :lmdb [['com.xtdb/xtdb-lmdb]])]
      (if ver dep [nm sha]))))

(defn sut-lein-project [{:keys [sha] :as sut}]
  (list
    'defproject 'sut "0-SNAPSHOT"
    :dependencies
    (vec
      (concat [['com.xtdb/xtdb-bench
                "dev-SNAPSHOT"
                :exclusions '[com.xtdb/xtdb-core
                              com.xtdb/xtdb-jdbc
                              com.xtdb/xtdb-kafka
                              com.xtdb/xtdb-kafka-embedded
                              com.xtdb/xtdb-rocksdb
                              com.xtdb/xtdb-lmdb
                              com.xtdb/xtdb-lucene
                              com.xtdb/xtdb-metrics
                              com.xtdb.labs/xtdb-rdf
                              com.xtdb/xtdb-test

                              pro.juxt.clojars-mirrors.clj-http/clj-http
                              software.amazon.awssdk/s3
                              com.amazonaws/aws-java-sdk-ses
                              com.amazonaws/aws-java-sdk-logs

                              ;; rdf
                              org.eclipse.rdf4j/rdf4j-repository-sparql
                              org.eclipse.rdf4j/rdf4j-sail-nativerdf
                              org.eclipse.rdf4j/rdf4j-repository-sail

                              ;; cloudwatch metrics deps
                              io.github.azagniotov/dropwizard-metrics-cloudwatch
                              software.amazon.awssdk/cloudwatch]]
               ['com.xtdb/xtdb-core sha]]
              (sut-module-deps sut)))))

(defn install-self-to-m2 []
  (bt/sh-ctx
    {:dir "bench"}
    (bt/sh "lein" "install")))

(def ^:dynamic *rocks-stats-cubby-hole*)

(defn undata-node-opts [{:keys [index, log, docs]}]
  (let [rocks-kv (fn [k] {:xtdb/module 'xtdb.rocksdb/->kv-store,
                          :metrics (fn [_]
                                     (fn [_db stats]
                                       (when (thread-bound? #'*rocks-stats-cubby-hole*)
                                         (set! *rocks-stats-cubby-hole* (assoc *rocks-stats-cubby-hole* k stats)))))
                          :db-dir (xio/create-tmpdir "kv")
                          :db-dir-suffix "kv"})
        lmdb-kv (fn [] {:xtdb/module 'xtdb.lmdb/->kv-store,
                        :db-dir (xio/create-tmpdir "kv")
                        :db-dir-suffix "kv"})
        undata (fn [k t]
                 (case t
                   nil {}
                   :rocks
                   {:kv-store (rocks-kv k)}
                   :lmdb
                   {:kv-store (lmdb-kv)}))]
    {:xtdb/tx-log (undata :log log)
     :xtdb/document-store (undata :docs docs)
     :xtdb/index-store (undata :index index)}))

;; remove this we figure it out
(require 'xtdb.bench2.rocksdb)

(declare trace)

(defn run-benchmark
  [{:keys [node-opts
           benchmark-type
           benchmark-opts]}]
  (let [benchmark
        (case benchmark-type
          :auctionmark
          ((requiring-resolve 'xtdb.bench2.auctionmark/benchmark) benchmark-opts)
          :tpch
          ((requiring-resolve 'xtdb.bench2.tpch/benchmark) benchmark-opts)
          :trace (trace benchmark-opts))
        benchmark-fn (b2/compile-benchmark
                       benchmark
                       (let [rocks-wrap (xtdb.bench2.rocksdb/stage-wrapper
                                          (fn [_]
                                            *rocks-stats-cubby-hole*))]
                         (fn [task f]
                           (-> (wrap-task task f)
                               (cond->> (:stage task) (rocks-wrap task))))))]
    (binding [*rocks-stats-cubby-hole* {}]
      (with-open [node (xt/start-node (undata-node-opts node-opts))]
        (benchmark-fn node)))))

(defn build-jar
  [{:keys [repository
           version
           sha
           use-existing-dev-snapshot
           modules]
    :or {repository "git@github.com:xtdb/xtdb.git"
         version "master"
         modules [:rocks :lmdb]}}]
  (let [sha (or sha (bt/resolve-sha repository version))]
    (assert (nil? sh/*sh-dir*) "must be in xtdb project dir!")

    (when-not use-existing-dev-snapshot
      (bt/log "Installing current bench sources to ~/.m2")
      (install-self-to-m2))

    (when-not (sha-artifacts-exist-in-m2? sha)
      (bt/log "Installing target sha to ~/.m2")
      (install-sha-artifacts-to-m2 repository sha))

    (bt/log "Building sut.jar")
    (let [proj-form (sut-lein-project
                      {:sha sha
                       :version version
                       :modules [:rocks :lmdb]})
          tmp-dir (xio/create-tmpdir "sut-lein")
          jar-file (io/file tmp-dir "target" "sut-0-SNAPSHOT-standalone.jar")]
      (spit (io/file tmp-dir "project.clj") (pr-str proj-form))
      (bt/sh-ctx {:dir tmp-dir} (bt/sh "lein" "uberjar"))
      jar-file)))

(def s3-jar-bucket "xtdb-bench")
(defn s3-jar-key [jar-hash] (str "b2/jar/" jar-hash ".jar"))
(defn s3-jar-path [jar-hash] (str "s3://" s3-jar-bucket "/" (s3-jar-key jar-hash)))
(defn ec2-jar-path [jar-hash] (str "sut-" jar-hash ".jar"))

(defn s3-object-exists? [bucket key]
  (binding [bt/*sh-ret* :map]
    (= 0 (:exit (bt/aws "s3api"
                        "head-object"
                        "--bucket" bucket
                        "--key" key)))))

;; use s3 for the jar rather than scp, so you do not have to upload it multiple times (s3 to ec2 is fast, rural broadband less so)
(defn s3-upload-jar
  [jar-file]
  (let [jar-hash (-> (bt/sh "sha256sum" "-b" (.getAbsolutePath jar-file))
                     (str/split #"\s+")
                     first)]
    ;; this is not as useful as you'd think - each uberjar is a .zip with non-deterministic meta, timestamps and such
    ;; there are tools available for this, but not sure what do for now.
    #_(s3-object-exists? s3-jar-bucket (s3-jar-key jar-hash))
    (bt/aws "s3" "cp" (.getAbsolutePath (io/file jar-file)) (s3-jar-path jar-hash))
    jar-hash))

(defn ec2-file-exists? [ec2 file]
  (binding [bt/*sh-ret* :map]
    (= 0 (:exit (ec2/ssh ec2 "test" "-f" (str file))))))

(defrecord JarHandle [ec2 jar-path])

(defn ec2-get-jar [ec2 s3-jar-hash]
  (ec2/ssh ec2 "aws" "s3" "cp" (s3-jar-path s3-jar-hash) (ec2-jar-path s3-jar-hash)))

(defn ec2-use-jar [ec2 s3-jar-hash]
  (ec2/ssh ec2 "ln" "-sf" (ec2-jar-path s3-jar-hash) "sut.jar"))

(defn ec2-setup [ec2]
  (ec2/await-ssh ec2)
  (ec2/install-packages ec2 ["awscli" "java-17-amazon-corretto-headless"]))

(def ^:redef ec2-repls [])

(defn kill-java [ec2]
  (binding [bt/*sh-ret* :map]
    (ec2/ssh ec2 "pkill" "java")))

(defrecord Ec2ReplProcess [ec2 ^Process fwd-process]
  Closeable
  (close [_]
    (.destroy fwd-process)
    (kill-java ec2)))

(defn ec2-repl [ec2]
  (let [_
        (->
          (ProcessBuilder.
            (->> (concat
                   (ec2/ssh-cmd ec2)
                   ["java"
                    "-Dclojure.server.repl=\"{:port 5555, :accept clojure.core.server/repl}\""
                    "-jar" "sut.jar"
                    "-e" (pr-str (pr-str '(let [o (Object.)]
                                            (try
                                              (locking o (.wait o))
                                              (catch InterruptedException _)))))])
                 (vec)))
          (doto (.inheritIO))
          (.start))

        fwd-proc
        (ec2/ssh-fwd (assoc ec2 :local-port 5555, :remote-port 5555))]

    (bt/log "Forwarded localhost:5555 to the ec2 box, connect to it as a socket REPL.")

    (let [ret (->Ec2ReplProcess ec2 fwd-proc)]
      (alter-var-root #'ec2-repls conj ret)
      ret)))

(defn ec2-eval
  ([ec2 code] (ec2-eval ec2 code {}))
  ([ec2 code {:keys [env, java-opts]}]
   ;; todo :env
   (apply ec2/ssh ec2 (concat ["java"] java-opts ["-jar" "sut.jar" "-e" (pr-str (pr-str code))]))))

(defn ec2-run-benchmark* [run-benchmark-opts report-s3-path]
  (let [report (run-benchmark run-benchmark-opts)
        tmp-file (File/createTempFile "report" ".edn")]
    (spit tmp-file (pr-str report))
    (bt/aws "s3" "cp" (.getAbsolutePath tmp-file) report-s3-path)))

(defn new-s3-report-path []
  (str "s3://xtdb-bench/b2/report/report-" (System/currentTimeMillis) ".edn"))

(defn ec2-run-benchmark [ec2 {:keys [env
                                     java-opts
                                     run-benchmark-opts
                                     report-s3-path]}]
  (ec2-eval
    ec2
    `((requiring-resolve 'xtdb.bench2.core1/ec2-run-benchmark*) ~run-benchmark-opts ~report-s3-path)
    {:env env
     :java-opts java-opts}))

(defn run-test [{:keys [ec2, s3-jar, env, java-opts, run-benchmark-opts, report-s3-path]}]
  (ec2-get-jar ec2 s3-jar)
  (ec2-use-jar ec2 s3-jar)
  (ec2-run-benchmark
    ec2
    {:env env
     :java-opts java-opts
     :run-benchmark-opts run-benchmark-opts
     :report-s3-path report-s3-path}))

;; tx seq, database as a file we can
;; record a trace and then repeat the indexing

(defn tx-seq
  [node]
  (let [rewrite-op
        (fn ! [st op]
          (case (nth op 0)
            (::xt/put ::xt/delete) (conj! st op)
            ;; redundant
            ::xt/match st
            ::xt/fn
            (let [[_ _ tx] op] (reduce ! st (::xt/tx-ops tx)))))

        rewrite-tx
        (fn [tx]
          (let [ops (persistent! (reduce rewrite-op (transient []) (::xt/tx-ops tx)))]
            (when (seq ops)
              (assoc tx ::xt/tx-ops ops))))]
    ((fn tx-seq-next [after]
       (lazy-seq
         (let [batch (with-open [c (xt/open-tx-log node after true)]
                       (into [] (comp (keep rewrite-tx) (take 4096)) (iterator-seq c)))]
           (if (= 4096 (count batch))
             (concat batch (tx-seq-next (::xt/tx-id (peek batch))))
             batch)))) -1)))

(defn write-tx-op [^DataOutputStream out, encode-fn, op tx]
  (case (nth op 0)
    ::xt/put
    (let [[_ doc valid-start valid-end] op]
      ;; put
      (.write out (byte 1))

      (if valid-start
        (.writeLong out (long (inst-ms valid-start)))
        (.writeLong out (long (inst-ms (::xt/tx-time tx)))))

      (if valid-end
        (.writeLong out (long (inst-ms valid-end)))
        (.writeLong out -1))

      (encode-fn out doc))
    ::xt/delete
    (let [[_ id] op]
      (.write out (byte 2))
      (encode-fn out id))))

(defn write-clj-tx
  [^DataOutputStream out,
   encode-fn
   tx]
  ;; begin tx
  (.write out (byte 1))
  (.writeLong out (long (::xt/tx-id tx)))
  (.writeLong out (long (inst-ms (::xt/tx-time tx))))
  (.writeInt out (count (::xt/tx-ops tx)))
  (doseq [op (::xt/tx-ops tx)]
    (write-tx-op out encode-fn op tx))
  ;; end tx
  (.write out (byte 0)))

(defn read-clj-tx
  [^DataInputStream in,
   decode-fn]
  (let [tx-id (.readLong in)
        tx-time (Date. (.readLong in))
        tx-op-count (.readInt in)
        op-buf (object-array tx-op-count)
        _ (dotimes [i tx-op-count]
            (case (int (.read in))
              0 nil
              ;; put
              1 (let [valid-start-ms (.readLong in)
                      valid-end-ms (.readLong in)]
                  (aset op-buf i [::xt/put (decode-fn in)
                                  (Date. valid-start-ms)
                                  (when (not= -1 valid-end-ms) (Date. valid-end-ms))]))
              ;; delete
              2 (aset op-buf i [::xt/delete (decode-fn in)])))
        tx-ops (seq op-buf)]

    (when-not (= 0 (.read in)) (throw (ex-info "Corrupt xt stream" {})))

    {::xt/tx-id tx-id
     ::xt/tx-time tx-time
     ::xt/tx-ops tx-ops}))

(defn write-tx-seq [^DataOutputStream out tx-seq]
  (let [symbol-table (HashMap.)
        nippy nippy/freeze-to-out!
        encode-kv (fn [^DataOutputStream out k v]
                    (if-some [sym (.get symbol-table k)]
                      (do (.write out (byte 0))
                          (.writeInt out (int sym)))
                      (do (.write out (byte 1))
                          ;; todo overflow
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

(defn spit-tx-seq [file tx-seq]
  (with-open [out (io/output-stream file)
              dout (DataOutputStream. out)]
    (write-tx-seq dout tx-seq)))

(defn read-tx-seq [^DataInputStream in]
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

(defn build-trace [benchmark file]
  (let [opts (undata-node-opts
               {:index :rocks,
                :log :rocks,
                :docs :rocks})]
    (try
      (with-open [node (xt/start-node opts)]
        (let [f (b2/compile-benchmark benchmark wrap-task)]
          (f node))
        (xt/sync node)
        (spit-tx-seq file (tx-seq node))
        file)
      (finally
        (doseq [[_ {:keys [kv-store]}] opts
                :let [dir (:db-dir kv-store)]
                :when dir]
          (.delete (io/file dir)))))))

(defn run-trace [node file]
  (with-open [in (io/input-stream file)
              din (DataInputStream. in)]
    (run! #(xt/submit-tx node %) (map ::xt/tx-ops (read-tx-seq din)))
    (xt/sync node)))

(defn trace [{:keys [file]}]
  {:title "Trace"
   :seed 0
   :tasks [{:t :call, :stage :trace, :f (fn [{node :sut}] (run-trace node file))}]})

(defn kv-seq [kv-snapshot]
  ((fn kv-seq-chunk [start]
     (lazy-seq
       (with-open [iter (kv/new-iterator kv-snapshot)]
         (let [buf (ArrayList. 4096)
               add (fn [k v]
                     (.add buf [(some-> k mem/->on-heap) (some-> v mem/->on-heap)]))]

           (when-some [k (kv/seek iter start)]
             (add k (kv/value iter))

             (loop [i 0]
               (when (< i 4096)
                 (when-some [k (kv/next iter)]
                   (add k (kv/value iter))
                   (recur (inc i)))))

             (concat
               (seq buf)
               (when-some [k (kv/next iter)]
                 (let [next-start (mem/->on-heap k)]
                   (when (seq next-start)
                     (kv-seq-chunk next-start))))))))))
   (byte-array [0])))

(defn index-checksum [node]
  (when-some [kv-store (-> node :index-store :kv-store)]
    (let [digest (MessageDigest/getInstance "md5")]
      (with-open [snap (kv/new-snapshot kv-store)]
        (doseq [[k v] (kv-seq snap)]
          (.update digest ^bytes k)
          (.update digest ^bytes v)))
      (.toString (BigInteger. 1 (.digest digest)) 16))))

(defn run-trace-check [node file]
  (run-trace node file)
  (Thread/sleep 1000)
  {:index (index-checksum node)})

(comment
  ;; ======
  ;; Running in process
  ;; ======

  (def run-duration "PT30S")
  (def run-duration "PT2M")
  (def run-duration "PT10M")

  (def report1-rocks
    (run-benchmark
      {:node-opts {:index :rocks, :log :rocks, :docs :rocks}
       :benchmark-type :auctionmark
       :benchmark-opts {:duration run-duration}}))

  (require 'xtdb.bench2.report)
  (xtdb.bench2.report/show-html-report
    (xtdb.bench2.report/vs
      "Rocks"
      report1-rocks))

  (def report1-lmdb
    (run-benchmark
      {:node-opts {:index :lmdb, :log :lmdb, :docs :lmdb}
       :benchmark-type :auctionmark
       :benchmark-opts {:duration run-duration}}))

  (xtdb.bench2.report/show-html-report
    (xtdb.bench2.report/vs
      "Rocks"
      report1-rocks
      "LMDB"
      report1-lmdb))

  ;; ======
  ;; TPC-H (temporary while I think)
  ;; ======
  (def sf 0.05)

  (def report-tpch-rocks
    (run-benchmark
      {:node-opts {:index :rocks, :log :rocks, :docs :rocks}
       :benchmark-type :tpch
       :benchmark-opts {:scale-factor sf}}))

  (def report-tpch-lmdb
    (run-benchmark
      {:node-opts {:index :lmdb, :log :lmdb, :docs :lmdb}
       :benchmark-type :tpch
       :benchmark-opts {:scale-factor sf}}))

  (xtdb.bench2.report/show-html-report
    (xtdb.bench2.report/vs
      "Rocks"
      report-tpch-rocks
      "LMDB"
      report-tpch-lmdb))

  ;; ======
  ;; Running in EC2
  ;; ======

  ;; step 1 build system-under-test .jar
  (def jar-file (build-jar {:version "1.22.0", :modules [:lmdb :rocks]}))

  ;; make jar available to download for ec2 nodes
  (def jar-hash (s3-upload-jar jar-file))
  ;; the path to the jar in s3 is given by its hash string
  (s3-jar-path jar-hash)

  ;; step 2 provision resources
  (def ec2-stack-id (str "bench-" (System/currentTimeMillis)))
  (def ec2-stack (ec2/provision ec2-stack-id {:instance "t3.small"}))
  (def ec2 (ec2/handle ec2-stack))

  ;; step 3 setup ec2 for running core1 benchmarks
  (ec2-setup ec2)
  ;; download the jar
  (ec2-get-jar ec2 jar-hash)
  ;; activate this hash (you could grab more than one jar and put it on the box)
  (ec2-use-jar ec2 jar-hash)

  ;; step 4 run your benchmark
  (def report-s3-path (format "s3://xtdb-bench/b2/report/%s.edn" ec2-stack-id))

  ;; todo java opts
  (ec2-run-benchmark
    ec2
    {:run-benchmark-opts
     {:node-opts {:index :rocks, :log :rocks, :docs :rocks}
      :benchmark-type :auctionmark
      :benchmark-opts {:duration run-duration}}
     :report-s3-path report-s3-path})

  ;; step 5 get your report

  (def report-file (File/createTempFile "report" ".edn"))

  (bt/aws "s3" "cp" report-s3-path (.getAbsolutePath report-file))

  (def report2 (edn/read-string (slurp report-file)))

  ;; step 6 visualise your report

  (require 'xtdb.bench2.report)
  (xtdb.bench2.report/show-html-report
    (xtdb.bench2.report/vs
      "Report2"
      report2))

  ;; compare to the earlier in-process report (now imagine running on n nodes with different configs)
  (xtdb.bench2.report/show-html-report
    (xtdb.bench2.report/vs
      "On laptop"
      report1-rocks
      "In EC2"
      report2))

  ;; filter reports to just :oltp stage
  (let [filter-report #(xtdb.bench2.report/stage-only % :oltp)
        report1 (filter-report report1-rocks)
        report2 (filter-report report2)]
    (xtdb.bench2.report/show-html-report
      (xtdb.bench2.report/vs
        "On laptop"
        report1
        "In EC2"
        report2)))


  ;; here is a bigger script comparing a couple of versions

  ;; run 1-21-0
  (def report-1-21-0-path (new-s3-report-path))

  ;; build
  (do
    (def jar-1-21-0 (s3-upload-jar (build-jar {:version "1.21.0", :modules [:rocks]})))
    (ec2-get-jar ec2 jar-1-21-0))

  ;; run
  (do
    (ec2-use-jar ec2 jar-1-21-0)
    (ec2-run-benchmark
      ec2
      {:env {"MALLOC_ARENA_MAX" 2}
       :java-opts ["--add-opens java.base/java.util.concurrent=ALL-UNNAMED"]
       :run-benchmark-opts
       {:node-opts {:index :rocks, :log :rocks, :docs :rocks}
        :benchmark-type :auctionmark
        :benchmark-opts {:duration run-duration}}
       :report-s3-path report-1-21-0-path})

    (def report-1-21-0-file (File/createTempFile "report" ".edn"))
    (bt/aws "s3" "cp" report-1-21-0-path (.getAbsolutePath report-1-21-0-file))
    (def report-1-21-0 (edn/read-string (slurp report-1-21-0-file)))

    )

  ;; run 1-22-0
  (def report-1-22-0-path (new-s3-report-path))

  ;; build
  (do
    (def jar-1-22-0 (s3-upload-jar (build-jar {:version "1.22.0", :modules [:rocks]})))
    (ec2-get-jar ec2 jar-1-22-0))

  ;; run
  (do
    (ec2-use-jar ec2 jar-1-22-0)
    (ec2-run-benchmark
      ec2
      {:run-benchmark-opts
       {:node-opts {:index :rocks, :log :rocks, :docs :rocks}
        :benchmark-type :auctionmark
        :benchmark-opts {:duration run-duration}}
       :report-s3-path report-1-22-0-path})
    (def report-1-22-0-file (File/createTempFile "report" ".edn"))
    (bt/aws "s3" "cp" report-1-22-0-path (.getAbsolutePath report-1-22-0-file))
    (def report-1-22-0 (edn/read-string (slurp report-1-22-0-file))))

  ;; report on both
  (let [filter-report #(xtdb.bench2.report/stage-only % :oltp)
        report1 (filter-report report-1-21-0)
        report2 (filter-report report-1-22-0)]
    (xtdb.bench2.report/show-html-report
      (xtdb.bench2.report/vs
        "1.21.0"
        report1
        "1.22.0"
        report2)))

  ;; ===
  ;; EC2 misc
  ;; ===
  ;; misc tools:
  ;; you can open a repl if something is wrong (close with .close), right now needs 5555 locally to forward over ssh
  (def repl (ec2-repl ec2))
  (.close repl)

  ;; if you lose a handle get it again from the stack
  (def ec2 (ec2/handle (ec2/cfn-stack-describe ec2-stack-id)))

  ;; run something over ssh
  (ec2/ssh ec2 "ls" "-la")

  ;; kill the java process!
  (kill-java ec2)

  ;; find stacks starting "bench-"
  (ec2/cfn-stack-ls)

  (doseq [{:strs [StackName]} (ec2/cfn-stack-ls)]
    (println "run this to delete the stack" (pr-str (list 'ec2/cfn-stack-delete StackName))))

  ;; CLEANUP! delete your node when finished with it
  (ec2/cfn-stack-delete ec2-stack-id)

  )

(comment

  ;; perf1
  (ec2/provision "bench-ingest-perf1" {:instance "m5.large"})

  (def ec2 (ec2/handle (ec2/cfn-stack-describe "bench-ingest-perf1")))

  (ec2-setup ec2)

  (defn dobench [repo version]
    (let [r1-path (new-s3-report-path)
          jar-1 (s3-upload-jar (build-jar {:version version, :repository repo, :modules [:rocks]}))
          _ (ec2-get-jar ec2 jar-1)
          _ (ec2-use-jar ec2 jar-1)
          {:keys [out, err]}
          (binding [bt/*sh-ret* :map]
            (ec2-run-benchmark
              ec2
              {:env {"MALLOC_ARENA_MAX" 2}
               :java-opts ["--add-opens java.base/java.util.concurrent=ALL-UNNAMED"]
               :run-benchmark-opts
               {:node-opts {:index :rocks, :log :rocks, :docs :rocks}
                :benchmark-type :auctionmark
                :benchmark-opts {:duration "PT5M"}}
               :report-s3-path r1-path}))
          r1-file (File/createTempFile "report" ".edn")]
      (println out)
      (println "ERR")
      (println err)
      (bt/aws "s3" "cp" r1-path (.getAbsolutePath r1-file))
      (edn/read-string (slurp r1-file))))

  (def r1 (dobench "git@github.com:xtdb/xtdb.git" "master"))
  (def r2 (dobench "git@github.com:wotbrew/xtdb.git" "ingest-perf1"))

  (require 'xtdb.bench2.report)
  (xtdb.bench2.report/show-html-report
    (xtdb.bench2.report/vs
      "master"
      r1
      "ingest pass"
      r2))

  )

(comment

  ;; perf1, trace
  (ec2/provision "bench-ingest-perf1" {:instance "m5.large"})

  (def ec2 (ec2/handle (ec2/cfn-stack-describe "bench-ingest-perf1")))

  (ec2-setup ec2)

  (defn dobencht [repo version]
    (let [r1-path (new-s3-report-path)
          jar-1 (s3-upload-jar (build-jar {:version version, :repository repo, :modules [:rocks]}))
          _ (ec2-get-jar ec2 jar-1)
          _ (ec2-use-jar ec2 jar-1)
          _ (ec2/ssh ec2 "aws" "s3" "cp" "s3://xtdb-bench/b2/amtrace.bin" "amtrace.bin")
          {:keys [out, err]}
          (binding [bt/*sh-ret* :map]
            (ec2-run-benchmark
              ec2
              {:env {"MALLOC_ARENA_MAX" 2}
               :java-opts ["--add-opens java.base/java.util.concurrent=ALL-UNNAMED"]
               :run-benchmark-opts
               {:node-opts {:index :rocks, :log :rocks, :docs :rocks}
                :benchmark-type :trace
                :benchmark-opts {:file "amtrace.bin"}}
               :report-s3-path r1-path}))
          r1-file (File/createTempFile "report" ".edn")]
      (println out)
      (println "ERR")
      (println err)
      (bt/aws "s3" "cp" r1-path (.getAbsolutePath r1-file))
      (edn/read-string (slurp r1-file))))

  (def r1t (dobencht "git@github.com:xtdb/xtdb.git" "master"))
  (def r2t (dobencht "git@github.com:wotbrew/xtdb.git" "ingest-perf1"))

  (require 'xtdb.bench2.report)
  (xtdb.bench2.report/show-html-report
    (xtdb.bench2.report/vs
      "master"
      r1t
      "ingest pass"
      r2t))

  )

(comment

  (s/check-asserts false)

  (let [opts (undata-node-opts
               {:index :rocks,
                :log :rocks,
                :docs :rocks})]
    (try
      (with-open [node (xt/start-node opts)]
        (run-trace-check node "amtrace.bin"))
      (finally
        (doseq [[_ {:keys [kv-store]}] opts
                :let [dir (:db-dir kv-store)]
                :when dir]
          (.delete (io/file dir))))))

  ;; master/bench fd6ec3fe7f71535fac2a50e55da505d5
  ;; perf1 1b94e67eb0aae841efe21f52d66f63b9

  )
