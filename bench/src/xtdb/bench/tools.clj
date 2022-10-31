(ns xtdb.bench.tools
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.shell :as sh]
            [xtdb.bench2 :as b2]
            [clojure.data.json :as json])
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
    :ec2 ((requiring-resolve 'xtdb.bench.ec2/loc-fn) env epoch-ms)))

(defn resolve-env [env sut]
  (case (:t env)
    :local env
    :ec2 ((requiring-resolve 'xtdb.bench.ec2/resolve-env) env sut)))

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

(def probably-running-in-ec2 (= "ec2-user" (System/getenv "USER")))

(defn aws [& args]
  (let [cmd (concat
              args
              ["--output" "json"
               ;; todo require explicit setup
               "--region" "eu-west-1"]
              (when-not probably-running-in-ec2
                ["--profile" "xtdb-bench"]))
        out (apply sh "aws" cmd)]
    (try (json/read-str out) (catch Throwable _ out))))

(defn s3-cli-path [loc]
  (case (:t loc)
    :s3 (str "s3://" (:bucket loc) "/" (:key loc))
    :file (.getAbsolutePath (io/file (:file loc)))))

(defn copy [from to]
  (case (:t to)
    :s3 (aws "s3" "cp" (s3-cli-path from) (s3-cli-path to))))

(defn resolve-sha [repository ref-spec]
  (first (str/split (sh "git" "ls-remote" repository ref-spec) #"\t")))

(defn resolve-sut [sut loc-fn]
  (case (:t sut)
    :xtdb ((requiring-resolve 'xtdb.bench.core1/resolve-sut) sut loc-fn)))

(defn resolve-req
  [req]
  (let [{:keys [env, sut]} req
        epoch-ms (System/currentTimeMillis)
        loc-fn (bench-loc-fn env epoch-ms)
        resolved-sut (resolve-sut sut loc-fn)
        manifest-loc (loc-fn "manifest.edn")
        resolved-env (resolve-env env resolved-sut)]
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
     :args {:duration "PT30M", :thread-count 8}
     :env {:t :ec2, :instance "m1.small"}
     :sut {:t :xtdb,
           :version "1.22.0"
           :index :rocks
           :log :rocks
           :docs :rocks}})

  ;; todo this should also work? or should we just in-process tools that look a bit like this?
  {:title "LMDB"
   :t :auctionmark
   :args {:duration "PT30S"}
   :env {:t :local}
   :sut {:t :xtdb,
         :version "1.22.0"
         :index :rocks
         :log :rocks
         :docs :rocks}}

  ;; resolves the request to a resolved request, where ambiguities in the request are removed (e.g branch 2 sha, amis, paths)
  (resolve-req bench-req-example))

(defn run-resolved! [resolved-req]
  (let [{:keys [report, sut, t, args]} resolved-req
        benchmark (case t :auctionmark ((requiring-resolve 'xtdb.bench.auctionmark/benchmark) args))
        {:keys [start, hook]}
        (case (:t sut)
          :xtdb ((requiring-resolve 'xtdb.bench.core1/prep) resolved-req))
        run-benchmark (b2/compile-benchmark benchmark hook)]
    (with-open [^Closeable sut (start)]
      (let [out (run-benchmark sut)
            tmp (File/createTempFile "report" ".edn")]
        (try
          (spit tmp (pr-str out))
          (copy {:t :file, :file tmp} report)
          (finally (.delete tmp)))))))

(defn setup! [req]
  (let [{:keys [manifest, sut, env] :as resolved} (resolve-req req)
        manifest-file (File/createTempFile "manifest" ".edn")]

    (spit manifest-file (pr-str resolved))
    (copy {:t :file, :file manifest-file} manifest)

    (case (:t sut)
      :xtdb ((requiring-resolve 'xtdb.bench.core1/provide-sut-requirements) sut))

    {:resolved-req resolved
     :handle (case (:t env)
               :ec2 ((requiring-resolve 'xtdb.bench.ec2/setup!) resolved))}))
