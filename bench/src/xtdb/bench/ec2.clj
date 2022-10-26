(ns xtdb.bench.ec2
  (:require [clojure.data.json :as json]
            [xtdb.bench.tools :as bt])
  (:import (java.time LocalDateTime Instant ZoneId)
           (java.time.format DateTimeFormatter)))

(defn cfn-template [{:keys [ami, instance]}]
  {"AWSTemplateFormatVersion" "2010-09-09"
   "Description" "benchmark"
   "Resources" {"Runner" {"Type" "AWS::EC2::Instance"
                          "Properties" {"ImageId" ami
                                        "InstanceType" instance}}}})

(defn stack-name [id]
  (str "bench-" id))

(defn ec2-stack-create-cli-input [env id]
  {"StackName" (stack-name id)
   "TemplateBody" (json/write-str (cfn-template env))
   "Parameters" []
   "OnFailure" "DELETE"
   ;; yagni?! not sure if already tagged - verify
   "Tags" (vec (for [[k v] {"BenchmarkStack" (stack-name id)}]
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
          [:corretto 17] "java-17-amazon-corretto-headless")

        jar-path (bt/s3-cli-path jar)
        manifest-path (bt/s3-cli-path manifest-loc)]
    (merge
      env
      {:instance instance
       :ami ami
       :packages [jre-package, "awscli"]
       :script [["aws" "cp" jar-path "sut.jar"]
                ["aws" "cp" manifest-path "manifest.edn"]
                ["java" "-jar" "sut.jar" "manifest.edn"]]})))

(defn run-provided! [resolved-req]
  (let [id (-> (LocalDateTime/ofInstant (Instant/ofEpochMilli (:epoch-ms resolved-req))
                                        (ZoneId/of "Europe/London"))
               (.format (DateTimeFormatter/ofPattern "YYYY-MM-dd-HH-mm-ss-SS")))]
    (ec2-stack-create (:env resolved-req) id)))
