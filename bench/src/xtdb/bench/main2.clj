(ns xtdb.bench.main2
  (:gen-class))

(defn -main [& [manifest-file]]
  (let [manifest ((requiring-resolve 'clojure.edn/read-string) (slurp manifest-file))]
    ((requiring-resolve 'xtdb.bench.tools/run-resolved!) manifest)))
