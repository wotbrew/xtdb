(ns xtdb.bench.ec2
  (:require [clojure.data.json :as json]
            [xtdb.bench.tools :as bt]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.time LocalDateTime Instant ZoneId Duration)
           (java.time.format DateTimeFormatter)
           (java.io File StringWriter)
           (clojure.lang ExceptionInfo)))

(defn ec2-stack-create-cli-input [env id]
  {"StackName" id
   "TemplateBody" (slurp (or (io/resource "xtdb-ec2.yml")
                             (io/file "bench/resources/xtdb-ec2.yml")))
   "Parameters" (vec (for [[k v]
                           {"BenchmarkId" id
                            "InstanceType" (:instance env)
                            "InstanceAMI" (:ami env)}
                           :when v]
                       {"ParameterKey" k
                        "ParameterValue" v}))
   "OnFailure" "DELETE"
   ;; yagni?! not sure if already tagged - verify
   "Tags" (vec (for [[k v] {"BenchmarkId" id}]
                 {"Key" k "Value" v}))})

(defn ec2-stack-create [env id]
  (bt/aws "cloudformation" "create-stack" "--cli-input-json" (json/write-str (ec2-stack-create-cli-input env id))))

(defn ec2-stack-ls []
  (bt/aws "cloudformation" "list-stacks"
          #_#_"--stack-status-filter" "CREATE_COMPLETE"
          "--query" (format "StackSummaries[?starts_with(StackName, `%s`)]" "bench-")))

(defn ec2-stack-delete [stack-name]
  (bt/aws "cloudformation" "delete-stack" "--stack-name" stack-name))

(defn loc-fn [_env epoch-ms]
  (fn [filename]
    {:t :s3,
     :bucket "xtdb-bench"
     :key (bt/bench-path epoch-ms filename)}))

(defn resolve-env [env sut manifest-loc]
  (let [{:keys [instance,
                ami]
         :or {instance "m1.small"
              ami "ami-0ee415e1b8b71305f"}} env
        {:keys [jre, jar]} sut

        jre-package
        (case [(:t jre) (:version jre)]
          [:corretto 17] "java-17-amazon-corretto-headless")]
    (merge
      env
      {:instance instance
       :ami ami
       :packages [jre-package "awscli"]})))

(defn ec2-stack-describe [id]
  (-> (bt/aws "cloudformation" "describe-stacks" "--stack-name" id)
      json/read-str
      (get "Stacks")
      first))

(defn output-map [stack]
  (reduce #(assoc %1 (%2 "OutputKey") (%2 "OutputValue")) {} (get stack "Outputs")))

(defn- ssm-get-private-key [key-pair-id]
  (let [key-name (str "/ec2/keypair/" key-pair-id)
        param (when key-pair-id (json/read-str (bt/aws "ssm" "get-parameter" "--name" key-name "--with-decryption")))]
    (get-in param ["Parameter" "Value"])))

(defn ssh-remote [stack]
  (let [{public-dns-name "PublicDnsName"
         key-pair-id "KeyPairId"}
        (output-map stack)

        private-key (ssm-get-private-key key-pair-id)
        key-file (File/createTempFile "bench-pk" ".pem")
        _ (spit key-file private-key)
        _ (bt/sh "chmod" "400" (.getAbsolutePath key-file))]
    {:user "ec2-user"
     :host public-dns-name
     :key-file key-file}))

(defn ssh-remote-cmd [{:keys [user, host, key-file]}]
  ["ssh"
   "-oStrictHostKeyChecking=no"
   "-i" (.getAbsolutePath (io/file key-file))
   (str user "@" host)])

(defn ssh [ssh-remote cmd & cmd-args]
  (apply bt/sh (concat (ssh-remote-cmd ssh-remote) [cmd] cmd-args)))

(defn aws-id [epoch-ms]
  (str "bench-" (-> (LocalDateTime/ofInstant (Instant/ofEpochMilli epoch-ms)
                                             (ZoneId/of "Europe/London"))
                    (.format (DateTimeFormatter/ofPattern "YYYY-MM-dd-HH-mm-ss-SS")))))

(defn provision-infra [resolved-req]
  (let [id (aws-id (:epoch-ms resolved-req))
        wait-duration (Duration/ofMinutes 5)
        sleep-duration (Duration/ofSeconds 30)
        stack (atom nil)
        poll-stack #(reset! stack (ec2-stack-describe id))]

    (ec2-stack-create (:env resolved-req) id)

    (bt/log "Waiting for stack, this may take a while")
    (bt/log "  id:" id)

    (loop [wait-until (+ (System/currentTimeMillis) (.toMillis wait-duration))]
      (Thread/sleep (.toMillis sleep-duration))
      (poll-stack)
      (when-not (= "CREATE_COMPLETE" (get @stack "StackStatus"))
        (when (< (System/currentTimeMillis) wait-until)
          (recur wait-until))))

    (when-not (= "CREATE_COMPLETE" (get @stack "StackStatus"))
      (throw (ex-info "Timed out waiting for stack" {:stack-timeout true :stack-name id})))

    (bt/log "Stack created")
    (bt/log "  id:" id)

    {:id id
     :stack @stack}))

(defn setup! [resolved-req]
  (let [{:keys [manifest, sut, env]} resolved-req
        {:keys [jar]} sut
        {:keys [id, stack]} (provision-infra resolved-req)
        sr (ssh-remote stack)
        ssh-wait-duration (Duration/ofMinutes 2)
        ssh-wait-until (+ (System/currentTimeMillis) (.toMillis ssh-wait-duration))]

    (trampoline
      (fn try-ssh []
        (try
          (binding [*out* (StringWriter.)] (ssh sr "echo" "ping"))
          (catch ExceptionInfo e
            (let [{:keys [err]} (ex-data e)]
              (if (and (string? err)
                       (str/includes? err "Connection refused")
                       (< (System/currentTimeMillis) ssh-wait-until))
                (do (Thread/sleep 1000)
                    try-ssh)
                (throw e)))))))

    (when-some [packages (seq (:packages env))]
      (bt/log "Installing packages, this may take a while")
      (apply ssh sr "sudo" "yum" "install" "-y" packages))

    (bt/log "Downloading sut.jar")
    (ssh sr "aws" "s3" "cp" (bt/s3-cli-path jar) "sut.jar")

    (bt/log "Downloading manifest.edn")
    (ssh sr "aws" "s3" "cp" (bt/s3-cli-path manifest) "manifest.edn")

    {:id id
     :stack stack
     :ssh-remote sr
     :run-cmd ["java" "-jar" "sut.jar" "manifest.edn"]}))
