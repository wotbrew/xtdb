(ns xtdb.bench2.watdiv
  (:require [xtdb.bench.watdiv :as watdiv]
            [xtdb.api :as xt]))

(def queries
  watdiv/watdiv-stress-100-1-sparql)

(defn benchmark [{}]
  {:title "WatDiv"
   :seed 0
   :tasks [{:t :call,
            :stage :load,
            :f #(xt/await-tx (:sut %) (:last-tx (watdiv/submit-watdiv! (:sut %))))}
           ]})
