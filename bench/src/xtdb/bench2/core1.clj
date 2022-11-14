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
            [clojure.string :as str])
  (:import (io.micrometer.core.instrument MeterRegistry Timer Tag Timer$Sample)
           (java.time Duration LocalDateTime)
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

(defn undata-node-opts [{:keys [index, log, docs]}]
  (let [rocks-kv (fn [] {:xtdb/module 'xtdb.rocksdb/->kv-store,
                         :db-dir (xio/create-tmpdir "kv")
                         :db-dir-suffix "kv"})
        lmdb-kv (fn [] {:xtdb/module 'xtdb.lmdb/->kv-store,
                        :db-dir (xio/create-tmpdir "kv")
                        :db-dir-suffix "kv"})
        undata (fn [t]
                 (case t
                   nil {}
                   :rocks
                   {:kv-store (rocks-kv)}
                   :lmdb
                   {:kv-store (lmdb-kv)}))]
    {:xtdb/tx-log (undata log)
     :xtdb/document-store (undata docs)
     :xtdb/index-store (undata index)}))

(defn run-benchmark
  [{:keys [node-opts
           benchmark-type
           benchmark-opts]}]
  (let [benchmark
        (case benchmark-type
          :auctionmark
          ((requiring-resolve 'xtdb.bench2.auctionmark/benchmark) benchmark-opts)
          :tpch
          ((requiring-resolve 'xtdb.bench2.tpch/benchmark) benchmark-opts))
        benchmark-fn (b2/compile-benchmark benchmark wrap-task)]
    (with-open [node (xt/start-node (undata-node-opts node-opts))]
      (benchmark-fn node))))

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



(comment
  ;; ======
  ;; Running in process
  ;; ======

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
