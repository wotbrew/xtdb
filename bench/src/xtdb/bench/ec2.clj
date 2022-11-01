(ns xtdb.bench.ec2
  (:require [clojure.data.json :as json]
            [xtdb.bench.tools :as bt]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.time LocalDateTime Instant ZoneId Duration)
           (java.time.format DateTimeFormatter)
           (java.io File StringWriter Closeable)
           (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* false)

(defn cfn-stack-create-cli-input [env id]
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
   ;; todo lifetime for gc task
   "Tags" (vec (for [[k v] {"BenchmarkId" id}]
                 {"Key" k "Value" v}))})

(defn cfn-stack-create [env id]
  (bt/aws "cloudformation" "create-stack" "--cli-input-json" (json/write-str (cfn-stack-create-cli-input env id))))

(defn cfn-stack-ls []
  (bt/aws "cloudformation" "list-stacks"
          "--stack-status-filter" "CREATE_COMPLETE"
          "--query" (format "StackSummaries[?starts_with(StackName, `%s`)]" "bench-")))

(defn cfn-stack-delete [stack-name]
  (bt/aws "cloudformation" "delete-stack" "--stack-name" stack-name))

(defn loc-fn [_env epoch-ms]
  (fn [filename]
    {:t :s3,
     :bucket "xtdb-bench"
     :key (bt/bench-path epoch-ms filename)}))

(defn resolve-env [env sut]
  (let [{:keys [instance,
                ami]
         :or {instance "m1.small"
              ami "ami-0ee415e1b8b71305f"}} env
        {:keys [jre]} sut

        jre-package
        (case [(:t jre) (:version jre)]
          [:corretto 17] "java-17-amazon-corretto-headless")]
    (merge
      env
      {:instance instance
       :ami ami
       :packages [jre-package "awscli"]})))

(defn cfn-stack-describe [id]
  (-> (bt/aws "cloudformation" "describe-stacks" "--stack-name" id)
      (get "Stacks")
      first))

(defn output-map [stack]
  (reduce #(assoc %1 (%2 "OutputKey") (%2 "OutputValue")) {} (get stack "Outputs")))

(defn- ssm-get-private-key [key-pair-id]
  (let [key-name (str "/ec2/keypair/" key-pair-id)
        param (when key-pair-id (bt/aws "ssm" "get-parameter" "--name" key-name "--with-decryption"))]
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

(defrecord Ec2Handle [id stack user host key-file]
  Closeable
  (close [_]
    (cfn-stack-delete id)))

(defn ssh-cmd [{:keys [user, host, key-file]}]
  ["ssh"
   "-oStrictHostKeyChecking=no"
   "-i" (.getAbsolutePath (io/file key-file))
   (str user "@" host)])

(defn ssh [ec2 cmd & cmd-args]
  (apply bt/sh (concat (ssh-cmd ec2) [cmd] cmd-args)))

(defn aws-id [epoch-ms]
  (str "bench-" (-> (LocalDateTime/ofInstant (Instant/ofEpochMilli epoch-ms)
                                             (ZoneId/of "Europe/London"))
                    (.format (DateTimeFormatter/ofPattern "YYYY-MM-dd-HH-mm-ss-SS")))))

(defn provision! ^Ec2Handle [resolved-req]
  (let [id (aws-id (:epoch-ms resolved-req))
        wait-duration (Duration/ofMinutes 5)
        sleep-duration (Duration/ofSeconds 30)
        stack (atom nil)
        poll-stack #(reset! stack (cfn-stack-describe id))]

    (cfn-stack-create (:env resolved-req) id)

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

    (map->Ec2Handle (merge {:id id :stack @stack} (ssh-remote @stack)))))

(defn await-ssh [ec2]
  (let [ssh-wait-duration (Duration/ofMinutes 2)
        ssh-wait-until (+ (System/currentTimeMillis) (.toMillis ssh-wait-duration))]
    (trampoline
      (fn try-ssh []
        (try
          (binding [*out* (StringWriter.)] (ssh ec2 "echo" "ping"))
          (catch ExceptionInfo e
            (let [{:keys [err]} (ex-data e)]
              (if (and (string? err)
                       (str/includes? err "Connection refused")
                       (< (System/currentTimeMillis) ssh-wait-until))
                (do (Thread/sleep 1000)
                    try-ssh)
                (throw e)))))))))

(defn install-packages [ec2 packages]
  (when (seq packages)
    (apply ssh ec2 "sudo" "yum" "install" "-y" packages)))

(defn setup!
  ([resolved-req] (setup! resolved-req (provision! resolved-req)))
  ([resolved-req ec2]
   (let [{:keys [manifest, sut, env]} resolved-req
         {:keys [jar]} sut]

     (await-ssh ec2)

     (when-some [packages (seq (:packages env))]
       (bt/log "Installing packages, this may take a while")
       (apply ssh ec2 "sudo" "yum" "install" "-y" packages))

     (bt/log "Downloading sut.jar")
     (ssh ec2 "aws" "s3" "cp" (bt/s3-cli-path jar) "sut.jar")

     (bt/log "Downloading manifest.edn")
     (ssh ec2 "aws" "s3" "cp" (bt/s3-cli-path manifest) "manifest.edn")

     ec2)))

(defn clj [ec2 & code]
  (ssh ec2
       "java"
       "-jar" "sut.jar"
       "-e" (pr-str (pr-str (list* 'do code)))))

(defn ssh-fwd ^Process [{:keys [user, host, key-file, local-port, remote-port]}]
  (-> (doto (ProcessBuilder. ["ssh"
                              "-oStrictHostKeyChecking=no"
                              "-i" (.getAbsolutePath (io/file key-file))
                              "-NL"
                              (format "%s:localhost:%s" local-port remote-port)
                              (str user "@" host)])
        (.inheritIO))
      (.start)))

(def ^:redef repls [])

(defn kill-java [ec2] (binding [bt/*sh-ret* :map] (ssh ec2 "pkill" "java")))

(defrecord ReplProcess [ec2 ^Process fwd-process]
  Closeable
  (close [_]
    (.destroy fwd-process)
    (kill-java ec2)))

(defn repl [ec2]
  (let [_
        (->
          (ProcessBuilder.
            (->> (concat
                   (ssh-cmd ec2)
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
        (ssh-fwd (assoc ec2 :local-port 5555, :remote-port 5555))]

    (bt/log "Forwarded localhost:5555 to the ec2 box, connect to it as a socket REPL.")

    (let [ret (->ReplProcess ec2 fwd-proc)]
      (alter-var-root #'repls conj ret)
      ret)))

(defn provision
  [id {:keys [ami, instance]
       :or {ami "ami-0ee415e1b8b71305f"
            instance "m1.small"}}]
  (let [wait-duration (Duration/ofMinutes 5)
        sleep-duration (Duration/ofSeconds 30)
        stack (atom nil)
        poll-stack #(reset! stack (cfn-stack-describe id))]

    (cfn-stack-create {:ami ami, :instance instance} id)

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

    @stack))

(defn handle ^Ec2Handle [stack]
  (let [{id "StackName"} stack]
    (map->Ec2Handle (merge {:id id, :stack stack} (ssh-remote stack)))))
