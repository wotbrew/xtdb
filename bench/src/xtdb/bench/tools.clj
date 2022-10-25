(ns xtdb.bench.tools
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.shell :as sh]
            [xtdb.bench2 :as b2])
  (:import (java.time.format DateTimeFormatter)
           (java.time LocalDateTime Instant ZoneId)
           (java.io File Closeable)))

(defn timestamp-path [epoch-ms]
  (let [ldt (LocalDateTime/ofInstant (Instant/ofEpochMilli epoch-ms) (ZoneId/of "Europe/London"))]
    (.format ldt (DateTimeFormatter/ofPattern "YYYY/MM/dd/HH-mm-ss-SS"))))

(defn bench-path [epoch-ms, filename]
  ;; unencoded filename but who cares right now
  (str "b2/" (timestamp-path epoch-ms) "/" filename))

(defn bench-loc-fn [env epoch-ms]
  (case (:t env)
    :local
    (fn local-path [filename]
      {:t :file,
       ;; todo tmp-dir prop
       :file (.getAbsolutePath (io/file "/tmp" (bench-path epoch-ms filename)))})
    :ec2
    (fn ec2-path [filename]
      {:t :s3,
       :bucket "xtdb-bench"
       :key (bench-path epoch-ms filename)})))

(defn resolve-env [env sut manifest-loc]
  (case (:t env)
    :local env
    :ec2
    (let [{:keys [instance,
                  ami]
           :or {instance "m1.small"
                ami "ami-0ee415e1b8b71305f"}} env
          {:keys [jre, jar]} sut

          jre-package
          (case [(:t jre) (:version jre)]
            [:corretto 17] "java-17-amazon-corretto-headless")

          jar-path (case (:t jar) :s3 (str "s3://" (:bucket jar) "/" (:key jar)))]
      (merge
        env
        {:t :ec2
         :instance instance
         :ami ami
         :packages [jre-package, "awscli"]
         :script [["aws" "cp" jar-path "sut.jar"]
                  ["java" "-jar" "sut.jar" manifest-loc]]}))))

(defn log [& args] (apply println args))

(defn sh-ctx* [opts f]
  (println "---")
  (let [opts (update opts :env merge {"HOME" (System/getenv "HOME"), "PATH" (System/getenv "PATH")})
        {:keys [dir, env]} opts
        _ (when dir (println "  dir:" (str dir)))
        _ (doseq [[e v] env]
            (case e
              "XTDB_VERSION" (println "  env:" e v)
              (println "  env:" e)))]
    (sh/with-sh-env
      env
      (sh/with-sh-dir
        dir
        (f))))
  (println "---"))

(defmacro sh-ctx [opts & body] `(sh-ctx* ~opts (fn [] ~@body)))

(defn sh [& args]
  (let [command-str (str/join " " args)
        _ (println "  $" command-str)
        {:keys [exit, err, out]} (apply sh/sh args)]
    (when-not (= 0 exit)
      (throw (ex-info (or (not-empty out) "Command failed")
                      {:command command-str
                       :exit exit
                       :out out
                       :err err})))
    out))

(defn aws [& args]
  ;; todo rethink profile, region all that stuff
  (apply sh "aws"
         (concat args ["--output" "json"
                       ;; todo require explicit setup
                       "--region" "eu-west-1"
                       "--profile" "xtdb-bench"])))

(defn s3-copy-path [loc]
  (case (:t loc)
    :s3 (str "s3://" (:bucket loc) "/" (:key loc))
    :file (.getAbsolutePath (io/file (:file loc)))))

(defn copy [from to]
  (case (:t to)
    :s3 (aws "s3" "cp" (s3-copy-path from) (s3-copy-path to))))

(defn resolve-sha [repository ref-spec]
  (first (str/split (sh "git" "ls-remote" repository ref-spec) #"\t")))

(defn resolve-sut [sut loc-fn]
  (case (:t sut)
    :xtdb
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
          sha (resolve-sha repository version)]
      (merge
        sut
        {:repository repository
         :version version
         :index index
         :docs docs
         :log log
         :sha sha
         :jre jre
         :jar (loc-fn "sut.jar")}))))

(defn resolve-req
  [req]
  (let [{:keys [env, sut]} req
        epoch-ms (System/currentTimeMillis)
        loc-fn (bench-loc-fn env epoch-ms)
        resolved-sut (resolve-sut sut loc-fn)
        manifest-loc (loc-fn "manifest.edn")
        resolved-env (resolve-env env resolved-sut manifest-loc)]
    (merge
      req
      {:epoch-ms epoch-ms
       :manifest manifest-loc
       :report (loc-fn "report.edn")
       :status (loc-fn "status.edn")
       :env resolved-env
       :sut resolved-sut})))

(comment

  (def bench-req-example
    {:title "Rocks"
     :t :auctionmark,
     :arg {:duration "PT30M", :thread-count 8}
     :env {:t :ec2, :instance "m1.small"}
     :sut {:t :xtdb,
           :version "1.22.0"
           :index :rocks
           :log :rocks
           :docs :rocks}})

  ;; resolves the request to a resolved request, where ambiguities in the request are removed (e.g branch 2 sha, amis, paths)
  (resolve-req bench-req-example)

  )

(defn run-resolved! [resolved-req]
  (let [{:keys [report, sut, t, args]} resolved-req
        benchmark (case t :auctionmark ((requiring-resolve 'xtdb.bench.auctionmark/benchmark) args))
        {:keys [start, hook]}
        (case (:t sut)
          :xtdb
          {:start @(requiring-resolve 'xtdb.bench.core1/start-sut)
           :hook @(requiring-resolve 'xtdb.bench.core1/wrap-task)})
        run-benchmark (b2/compile-benchmark benchmark hook)]
    (with-open [^Closeable sut (start sut)]
      (let [out (run-benchmark sut)
            tmp (File/createTempFile "report" ".edn")]
        (try
          (spit tmp (pr-str out))
          (copy {:t :file, :file tmp} report)
          (finally (.delete tmp)))))))
