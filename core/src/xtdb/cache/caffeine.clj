(ns ^:no-doc xtdb.cache.caffeine
  "Provides an implementation of ICache for github.com/ben-manes/caffeine (high performance java caching library)."
  (:require [xtdb.system :as sys])
  (:import (com.github.benmanes.caffeine.cache Cache Caffeine)
           (java.util.function Function)
           (xtdb.cache ICache)
           (clojure.lang IDeref)))

;; entry box to allow nulls, caffeine does permit ILookup (get cache k default), nulls have special handling
(deftype Entry [obj] IDeref (deref [_] obj))

(deftype Proxy [^Cache cache]
  ICache
  (computeIfAbsent [_ k stored-key-fn f]
    (let [stored-key (stored-key-fn k)
          compute (reify Function (apply [_ stored-key] (->Entry (f stored-key))))]
      @(.get cache stored-key compute)))
  (evict [_ k] (.invalidate cache k))
  (valAt [_ k] (some-> (.getIfPresent cache k) deref))
  (valAt [_ k default]
    (if-some [ret (.getIfPresent cache k)]
      @ret
      default))
  (count [_] (.estimatedSize ^Cache cache))
  (close [_] (.cleanUp ^Cache cache) (.invalidateAll cache)))

(defn ->caffeine-cache
  {::sys/args {:cache-size {:doc "Maximum number of cache entries", :spec ::sys/nat-int32}}}
  [{:keys [cache-size]}]
  (-> (Caffeine/newBuilder)
      ;; although caffeine can hold more than Integer/MAX_VALUE entries, our caches are bound to int, because of the (count) interface expectation
      (.maximumSize (int cache-size))
      .build
      ->Proxy))
