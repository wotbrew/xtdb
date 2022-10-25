(ns xtdb.bench.tools
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.shell :as sh]
            [xtdb.io :as xio])
  (:import (java.time.format DateTimeFormatter)
           (java.time LocalDateTime Instant ZoneId)))

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

(defn build-io-fns
  "Returns a set of fns for doing IO, running processes and stuff.

  Captures common concerns for bench builds such as error handling, logging, cleanup so the scripts are less of a mess!

  Returns a map of functions:

  :sh - like clojure.java.shell/sh, returns a string out. Takes an optional map first instead of needing to splice options onto the end of the command args.
     e.g (sh {:env {\"SOME_VAR\" \"42\"}} \"my\" \"command\" \"args\")
  :cd - changes the default working directory
  :log - like println
  :home-file returns a file relative to the home dir
  :temp-dir returns a tmp-dir for the given string key, creating it for the first call. "
  []
  (let [working-dir (atom nil)
        cd (fn cd [dir-name] (println "  $ cd" (reset! working-dir (str dir-name))))
        sh (fn sh [opts? & args]
             (let [opts (when (map? opts?) opts?)
                   args (if (map? opts?) args (cons opts? args))
                   {:keys [dir, env]} opts
                   _ (when dir (println "  dir:" dir))
                   _ (doseq [[e] env] (println "  env:" e))
                   command-str (str/join " " args)
                   _ (println "  $" command-str)
                   use-dir (or dir @working-dir)
                   opts (update opts :env merge {"HOME" (System/getenv "HOME")
                                                 "PATH" (System/getenv "PATH")})
                   opts-to-sh (if use-dir (assoc opts :dir use-dir) opts)
                   {:keys [exit, err, out]} (apply sh/sh (apply concat args opts-to-sh))]
               (when-not (= 0 exit)
                 (throw (ex-info (or (not-empty out) "Command failed")
                                 {:command command-str
                                  :exit exit
                                  :out out
                                  :err err})))
               out))
        aws (fn [& args]
              (apply sh "aws" (concat args
                                      ["--output" "json"
                                       ;; todo require explicit setup
                                       "--region" "eu-west-1"
                                       "--profile" "juxt"])))
        log println]
    {:cd cd
     :log log
     :sh sh
     :aws aws
     :home-file (partial io/file (System/getenv "HOME"))
     :temp-dir (memoize (partial xio/create-tmpdir))}))

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
  ;; todo profile, region all that stuff
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

  ;; resolves the request to manifest, where ambiguities in the request are removed (e.g branch 2 sha, amis, paths)
  (resolve-req bench-req-example)



  )
