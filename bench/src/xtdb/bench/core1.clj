(ns xtdb.bench.core1
  (:require [xtdb.api :as xt]
            [xtdb.bench.measurement :as bm]
            [xtdb.bus :as bus]
            [xtdb.bench2 :as b2]
            [xtdb.bench.tools :as bt]
            [xtdb.io :as xio]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh])
  (:import (io.micrometer.core.instrument MeterRegistry Timer Tag Timer$Sample)
           (java.time Duration)
           (java.util.concurrent.atomic AtomicLong)
           (java.io Closeable)))

(defn generate [worker f n]
  (let [doc-seq (repeatedly n (partial f worker))
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
          (.gauge "node.tx" ^Iterable (Tag/of "event" "submitted") submit-counter)
          (.gauge "node.tx" ^Iterable (Tag/of "event" "indexed") indexed-counter)
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

    (reify xt/PXtdb
      (status [_] (xt/status node))
      (tx-committed? [_ submitted-tx] (xt/tx-committed? node submitted-tx))
      (sync [_] (xt/sync node))
      (sync [_ timeout] (xt/sync node timeout))
      (sync [_ tx-time timeout] (xt/sync node tx-time timeout))
      (await-tx-time [_ tx-time] (xt/await-tx-time node tx-time))
      (await-tx-time [_ tx-time timeout] (xt/await-tx-time node tx-time timeout))
      (await-tx [_ tx] (xt/await-tx node tx))
      (await-tx [_ tx timeout] (xt/await-tx node tx timeout))
      (listen [node event-opts f] (xt/listen node event-opts f))
      (latest-completed-tx [_] (xt/latest-completed-tx node))
      (latest-submitted-tx [_] (xt/latest-submitted-tx node))
      (attribute-stats [_] (xt/attribute-stats node))
      (active-queries [_] (xt/active-queries node))
      (recent-queries [_] (xt/recent-queries node))
      (slowest-queries [_] (xt/slowest-queries node))
      xt/PXtdbSubmitClient
      (submit-tx-async [_ tx-ops] (xt/submit-tx-async node tx-ops))
      (submit-tx-async [_ tx-ops opts] (xt/submit-tx-async node tx-ops opts))
      (submit-tx [this tx-ops] (xt/submit-tx this tx-ops {}))
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

(defn resolve-sut [sut loc-fn]
  (let [{:keys [repository
                version
                index
                docs
                log
                jre]
         :or {repository "git@github.com:xtdb/xtdb.git"
              version "master"
              index :rocks
              docs :rocks
              log :rocks
              jre {:t :corretto, :version 17}}} sut
        sha (bt/resolve-sha repository version)]
    (merge
      sut
      {:repository repository
       :version version
       :index index
       :docs docs
       :log log
       :sha sha
       :jre jre
       :jar (loc-fn "sut.jar")})))

(defn install-sha-artifacts-to-m2 [repository sha]
  (let [tmp-dir (xio/create-tmpdir "benchmark-xtdb")]
    (bt/sh-ctx
      {:dir tmp-dir}
      (bt/log "Fetching xtdb ref" sha "from" repository)
      (bt/sh "git" "init")
      (bt/sh "git" "remote" "add" "origin" repository)
      (bt/sh "git" "fetch" "origin" "--depth" "1" sha)
      (bt/sh "git" "checkout" sha)
      (bt/log "Installing jars for build")
      (bt/sh-ctx
        {:env {"XTDB_VERSION" sha}}
        (bt/sh "sh" "lein-sub" "install"))
      (.delete tmp-dir))))

(defn sha-artifacts-exist-in-m2? [sha]
  (.exists (io/file (System/getenv "HOME") ".m2" "repository" "com" "xtdb" "xtdb-core" sha)))

(defn sut-lein-project [{:keys [sha, index, log, docs]}]
  (list
    'defproject 'sut "0-SNAPSHOT"
    :dependencies
    (vec
      (concat [['com.xtdb/xtdb-bench "dev-SNAPSHOT" :exclusions ['com.xtdb]]
               ['com.xtdb/xtdb-core sha]]
              (distinct
                (for [impl [index, log, docs]
                      [nm ver :as dep] (case impl
                                         :rocks [['com.xtdb/xtdb-rocksdb]]
                                         :lmdb [['com.xtdb/xtdb-lmdb]])]
                  (if ver dep [nm sha])))))
    :aot ['xtdb.bench.main2]))

(defn install-self-to-m2 []
  (bt/sh-ctx
    {:dir "bench"}
    (bt/sh "lein" "install")))

(defn provide-sut-requirements [sut & {:keys [use-existing-dev-snapshot]}]
  (let [{:keys [jar, repository, sha]} sut]
    (assert (nil? sh/*sh-dir*) "must be in xtdb project dir!")

    (when-not use-existing-dev-snapshot
      (bt/log "Installing current bench sources to ~/.m2")
      (install-self-to-m2))

    (when-not (sha-artifacts-exist-in-m2? sha)
      (bt/log "Installing target sha to ~/.m2")
      (install-sha-artifacts-to-m2 repository sha))

    (bt/log "Building sut.jar")
    (let [proj-form (sut-lein-project sut)
          tmp-dir (xio/create-tmpdir "sut-lein")
          jar-local (io/file tmp-dir "target" "sut-0-SNAPSHOT-standalone.jar")]
      (spit (io/file tmp-dir "project.clj") (pr-str proj-form))
      (bt/sh-ctx {:dir tmp-dir} (bt/sh "lein" "uberjar"))
      (bt/log "Providing sut.jar")
      (bt/copy {:t :file, :file jar-local} jar))))

(defn sut-node-opts [{:keys [index, log, docs]}]
  (let [rocks-kv (fn [] {:xtdb/module 'xtdb.rocksdb/->kv-store,
                         :db-dir-suffix "kv"
                         :db-dir (xio/create-tmpdir "bench")})
        lmdb-kv (fn [] {:xtdb/module 'xtdblmdb/->kv-store,
                        :db-dir-suffix "kv"
                        :db-dir (xio/create-tmpdir "bench")})]
    {:xtdb/tx-log {:kv-store (case log :rocks (rocks-kv) :lmdb (lmdb-kv))}
     :xtdb/document-store {:kv-store (case docs :rocks (rocks-kv) :lmdb (lmdb-kv))}
     :xtdb/index-store {:kv-store (case index :rocks (rocks-kv) :lmdb (lmdb-kv))}}))

(defn prep [{:keys [sut]}]
  (let [node-opts (sut-node-opts sut)]
    {:start #(xt/start-node node-opts)
     :hook wrap-task}))
