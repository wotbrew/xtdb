(ns xtdb.bench2.tools
  (:require [clojure.string :as str]
            [clojure.java.shell :as sh]
            [clojure.data.json :as json]))

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

(def ^:dynamic *sh-ret* :out)

(defn sh [& args]
  (let [command-str (str/join " " args)
        _ (println "  $" command-str)
        {:keys [exit, err, out] :as m} (apply sh/sh args)]
    (case *sh-ret*
      :out
      (do
        (when-not (= 0 exit)
          (throw (ex-info (or (not-empty out) "Command failed")
                          {:command command-str
                           :exit exit
                           :out out
                           :err err})))
        out)
      :map m)))

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

(defn resolve-sha [repository ref-spec]
  (first (str/split (sh "git" "ls-remote" repository ref-spec) #"\t")))
