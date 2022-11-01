(ns xtdb.bench.core1
  (:require [xtdb.api :as xt]
            [xtdb.bench.measurement :as bm]
            [xtdb.bus :as bus]
            [xtdb.bench2 :as b2]
            [xtdb.bench.tools :as bt]
            [xtdb.bench.ec2 :as ec2]
            [xtdb.io :as xio]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.edn :as edn])
  (:import (io.micrometer.core.instrument MeterRegistry Timer Tag Timer$Sample)
           (java.time Duration)
           (java.util.concurrent.atomic AtomicLong)
           (java.io Closeable File)))

(set! *warn-on-reflection* false)

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

(defn sut-module-deps [{:keys [sha, index, log, docs]}]
  (distinct
    (for [impl [index, log, docs]
          [nm ver :as dep] (case impl
                             :rocks [['com.xtdb/xtdb-rocksdb]]
                             :lmdb [['com.xtdb/xtdb-lmdb]])]
      (if ver dep [nm sha]))))

(defn sut-lein-project [{:keys [sha] :as sut}]
  (list
    'defproject 'sut "0-SNAPSHOT"
    :dependencies
    (vec
      (concat [['com.xtdb/xtdb-bench "dev-SNAPSHOT" :exclusions ['com.xtdb]]
               ['com.xtdb/xtdb-core sha]]
              (sut-module-deps sut)))))

(defn install-self-to-m2 []
  (bt/sh-ctx
    {:dir "bench"}
    (bt/sh "lein" "install")))

(defn build-sut-jar [sut]
  (let [{:keys [repository, sha, use-existing-dev-snapshot]} sut]
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
          jar-file (io/file tmp-dir "target" "sut-0-SNAPSHOT-standalone.jar")]
      (spit (io/file tmp-dir "project.clj") (pr-str proj-form))
      (bt/sh-ctx {:dir tmp-dir} (bt/sh "lein" "uberjar"))
      jar-file)))

(defn provide-sut-requirements [sut]
  (let [{:keys [jar]} sut
        jar-local (build-sut-jar sut)]
    (bt/copy {:t :file, :file jar-local} jar)))

(defn prep [{:keys [sut]}]
  #_(let [node-opts (sut-node-opts sut)]
    {:start #(xt/start-node node-opts)
     :hook wrap-task}))

(defn run-benchmark
  [{:keys [node-opts
           benchmark-type
           benchmark-opts]}]
  (let [benchmark
        (case benchmark-type
          :auctionmark
          ((requiring-resolve 'xtdb.bench.auctionmark/benchmark) benchmark-opts))
        benchmark-fn (b2/compile-benchmark benchmark wrap-task)]
    (with-open [node (xt/start-node node-opts)]
      (benchmark-fn node))))

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

(defn build-jar
  [{:keys [repository
           version
           index
           docs
           log
           sha
           use-existing-dev-snapshot]
    :or {repository "git@github.com:xtdb/xtdb.git"
         version "master"
         index :rocks
         docs :rocks
         log :rocks}}]
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
                       :index index
                       :docs docs
                       :log log})
          tmp-dir (xio/create-tmpdir "sut-lein")
          jar-file (io/file tmp-dir "target" "sut-0-SNAPSHOT-standalone.jar")]
      (spit (io/file tmp-dir "project.clj") (pr-str proj-form))
      (bt/sh-ctx {:dir tmp-dir} (bt/sh "lein" "uberjar"))
      jar-file)))

;; use s3 for the jar rather than scp, so you do not have to upload it multiple times (s3 to ec2 is fast, rural broadband less so)
;; another cool thing is maybe put to path by content hash and diff to avoid upload if needed
(defn s3-upload-jar [jar-file jar-s3-path]
  (bt/aws "s3" "cp" (.getAbsolutePath (io/file jar-file)) jar-s3-path))

(defn ec2-setup [ec2 s3-jar-path]
  (ec2/await-ssh ec2)
  (ec2/install-packages ec2 ["awscli" "java-17-amazon-corretto-headless"])
  (ec2/ssh ec2 "aws" "s3" "cp" s3-jar-path "sut.jar"))

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

(defn ec2-eval [ec2 & code]
  (ec2/ssh ec2 "java" "-jar" "sut.jar" "-e" (pr-str (pr-str (list* 'do code)))))

(defn ec2-run-benchmark* [run-benchmark-opts report-s3-path]
  (let [report (run-benchmark run-benchmark-opts)
        tmp-file (File/createTempFile "report" ".edn")]
    (spit tmp-file (pr-str report))
    (bt/aws "s3" "cp" (.getAbsolutePath tmp-file) report-s3-path)))

(defn ec2-run-benchmark [ec2 run-benchmark-opts report-s3-path]
  (ec2-eval
    ec2
    `((requiring-resolve 'xtdb.bench.core1/ec2-run-benchmark*) ~run-benchmark-opts ~report-s3-path)))


(comment
  ;; ======
  ;; Running in process
  ;; ======

  ;; easy!

  (def report1
    (run-benchmark
      {:node-opts {}
       :benchmark-type :auctionmark
       :benchmark-opts {:duration "PT30S"}}))

  (require 'xtdb.bench.report)
  (xtdb.bench.report/show-html-report
    (xtdb.bench.report/vs
      "Report1"
      report1))


  ;; ======
  ;; Running in EC2
  ;; ======

  ;; step 1 build system-under-test .jar
  (def jar-file (build-jar {:version "1.22.0"}))
  (def jar-s3-path (format "s3://xtdb-bench/b2/jar/%s.jar" ec2-stack-id))

  ;; make jar available to download for ec2 nodes
  (s3-upload-jar jar-file jar-s3-path)

  ;; step 2 provision resources
  (def ec2-stack-id (str "bench-" (System/currentTimeMillis)))
  (def ec2-stack (ec2/provision ec2-stack-id))
  (def ec2 (ec2/handle ec2-stack))

  ;; step 3 setup ec2 for running core1 benchmarks against sut.jar
  (ec2-setup ec2 jar-s3-path)

  ;; step 4 run your benchmark
  (def report-s3-path (format "s3://xtdb-bench/b2/report/%s.edn" ec2-stack-id))

  (ec2-run-benchmark
    ec2
    {:node-opts {}
     :benchmark-type :auctionmark
     :benchmark-opts {:duration "PT30S"}}
    report-s3-path)

  (def report-file (File/createTempFile "report" ".edn"))
  (bt/aws "s3" "cp" report-s3-path (.getAbsolutePath report-file))

  ;; step 5 get your report
  (def report2 (edn/read-string (slurp report-file)))

  ;; step 6 visualise your report
  ;; TODO PORT VEGA CODE

  (require 'xtdb.bench.report)
  (xtdb.bench.report/show-html-report
    (xtdb.bench.report/vs
      "Report2"
      report2))

  ;; compare to the earlier in-process report (now imagine running on n nodes)
  (xtdb.bench.report/show-html-report
    (xtdb.bench.report/vs
      "On laptop"
      report1
      "In EC2"
      report2))



  ;; CLEANUP! delete your node when finished, later might tag with a max-duration and (ec2/gc)
  (ec2/cfn-stack-delete ec2-stack-id)



  )
