(ns ^:no-doc xtdb.cache
  (:require [xtdb.cache.caffeine :as caffeine]
            [xtdb.system :as sys])
  (:import xtdb.cache.ICache))

(defn compute-if-absent [^ICache cache k stored-key-fn f]
  (.computeIfAbsent cache k stored-key-fn f))

(defn evict [^ICache cache k]
  (.evict cache k))

(defn ->cache
  {::sys/args {:cache-size {:doc "Cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}}}
  ^xtdb.cache.ICache [opts]
  (caffeine/->caffeine-cache (assoc opts :cache-size (-> #'->cache meta ::sys/args :cache-size :default))))
