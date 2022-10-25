(ns xtdb.bench2
  (:require [clojure.walk :as walk])
  (:import (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent ConcurrentHashMap)
           (java.util Random Comparator)
           (com.google.common.collect MinMaxPriorityQueue)
           (java.util.function Function)
           (java.time Instant Duration Clock)))

(set! *warn-on-reflection* false)

(defrecord Worker [sut random domain-state custom-state clock reports])

(defn current-timestamp ^Instant [worker]
  (.instant ^Clock (:clock worker)))

(defn counter ^AtomicLong [worker domain]
  (.computeIfAbsent ^ConcurrentHashMap (:domain-state worker) domain (reify Function (apply [_ _] (AtomicLong.)))))

(defn rng ^Random [worker] (:random worker))

(defn id
  "Defines some function of a continuous integer domain, literally just identity, but with uh... identity, e.g
   the function returned is a different closure every time."
  []
  (fn [n] n))

(defn increment [worker domain] (domain (.getAndIncrement (counter worker domain))))

(defn- nat-or-nil [n] (when (nat-int? n) n))

(defn sample-gaussian [worker domain]
  (let [random (rng worker)
        long-counter (counter worker domain)]
    (some-> (min (dec (.get long-counter)) (Math/round (* (.get long-counter) (* 0.5 (+ 1.0 (.nextGaussian random))))))
            long
            nat-or-nil
            domain)))

(defn sample-flat [worker domain]
  (let [random (rng worker)
        long-counter (counter worker domain)]
    (some-> (min (dec (.get long-counter)) (Math/round (* (.get long-counter) (.nextDouble random))))
            long
            nat-or-nil
            domain)))

(defn weighted-sample-fn
  "Aliased random sampler:

  https://www.peterstefek.me/alias-method.html

  Given a seq of [item weight] pairs, return a function who when given a Random will return an item according to the weight distribution."
  [weighted-items]
  (case (count weighted-items)
    0 (constantly nil)
    1 (constantly (ffirst weighted-items))
    (let [total (reduce + (map second weighted-items))
          normalized-items (mapv (fn [[item weight]] [item (double (/ weight total))]) weighted-items)
          len (count normalized-items)
          pq (doto (.create (MinMaxPriorityQueue/orderedBy ^Comparator (fn [[_ w] [_ w2]] (compare w w2)))) (.addAll normalized-items))
          avg (/ 1.0 len)
          parts (object-array len)
          epsilon 0.00001]
      (dotimes [i len]
        (let [[smallest small-weight] (.pollFirst pq)
              overfill (- avg small-weight)]
          (if (< epsilon overfill)
            (let [[largest large-weight] (.pollLast pq)
                  new-weight (- large-weight overfill)]
              (when (< epsilon new-weight)
                (.add pq [largest new-weight]))
              (aset parts i [small-weight smallest largest]))
            (aset parts i [small-weight smallest smallest]))))
      ^{:table parts}
      (fn sample-weighting [^Random random]
        (let [i (.nextInt random len)
              [split small large] (aget parts i)]
          (if (< (.nextDouble random) (double split)) small large))))))

(defn random-seq [worker opts f & args]
  (let [{:keys [min, max, unique]} opts]
    (->> (repeatedly #(apply f worker args))
         (take (+ min (.nextInt (rng worker) (- max min))))
         ((if unique distinct identity)))))

(defn random-str
  ([worker] (random-str worker 1 100))
  ([worker min-len max-len]
   (let [random (rng worker)
         len (max 0 (+ min-len (.nextInt random max-len)))
         buf (byte-array (* 2 len))
         _ (.nextBytes random buf)]
     (.toString (BigInteger. 1 buf) 16))))

(defn random-nth [worker coll]
  (when (seq coll)
    (let [idx (.nextInt (rng worker) (count coll))]
      (nth coll idx nil))))

(defn- await-threads [threads ^Duration wait]
  (when-some [t (first threads)]
    (.join t (.toMillis wait)))

  ;; warn/alert?
  (doseq [^Thread thread (filter #(.isAlive ^Thread %) threads)]
    (.interrupt thread)))

(defn compile-benchmark [benchmark hook]
  (let [seed (:seed benchmark 0)
        lift-f (fn [f] (if (vector? f) #(apply (first f) % (rest f)) f))
        compile-task
        (fn compile-task [{:keys [t] :as task}]
          (hook
            task
            (case t
              :do
              (let [{:keys [tasks]} task]
                (let [fns (mapv compile-task tasks)]
                  (fn run-do [worker]
                    (doseq [f fns]
                      (f worker)))))

              :call
              (let [{:keys [f]} task]
                (lift-f f))

              :pool
              (let [{:keys [^Duration duration
                            ^Duration think
                            ^Duration join-wait
                            thread-count
                            pooled-task]} task

                    think-ms (.toMillis (or think Duration/ZERO))
                    sleep (if (pos? think-ms) #(Thread/sleep think-ms) (constantly nil))
                    f (compile-task pooled-task)

                    thread-loop
                    (fn run-pool-thread-loop [worker]
                      (let [^Clock clock (:clock worker)]
                        (loop [wait-until (+ (.millis clock) (.toMillis duration))]
                          (f worker)
                          (when (< (.millis clock) wait-until)
                            (sleep)
                            (recur wait-until)))))

                    start-thread
                    (fn [root-worker i]
                      (let [worker (assoc root-worker :random (Random. (.nextLong ^Random (:random root-worker))))]
                        (doto (Thread. ^Runnable (fn [] (thread-loop worker)))
                          (.start))))]

                (fn run-pool [worker]
                  (let [running-threads (mapv #(start-thread worker %) (range thread-count))]
                    (await-threads running-threads (.plus duration join-wait)))))

              :concurrently
              (let [{:keys [^Duration duration,
                            ^Duration join-wait,
                            thread-tasks]} task

                    thread-task-fns (mapv compile-task thread-tasks)
                    start-thread
                    (fn [root-worker i f]
                      (let [worker (assoc root-worker :random (Random. (.nextLong ^Random (:random root-worker))))]
                        (doto (Thread. ^Runnable (fn [] (f worker)))
                          (.start))))]
                (fn run-concurrently [worker]
                  (let [running-threads (vec (map-indexed #(start-thread worker %1 %2) thread-task-fns))]
                    (await-threads running-threads (.plus duration join-wait)))))

              :pick-weighted
              (let [{:keys [choices]} task
                    sample-fn (weighted-sample-fn (mapv (fn [[task weight]] [(compile-task task) weight]) choices))]
                (if (empty? choices)
                  (constantly nil)
                  (fn run-pick-weighted [worker]
                    (let [f (sample-fn (rng worker))]
                      (f worker)))))

              :freq-job
              (let [{:keys [^Duration duration,
                            ^Duration freq,
                            job-task]} task
                    f (compile-task job-task)
                    duration-ms (.toMillis (or duration Duration/ZERO))
                    freq-ms (.toMillis (or freq Duration/ZERO))
                    sleep (if (pos? freq-ms) #(Thread/sleep freq-ms) (constantly nil))]
                (fn run-freq-job [worker]
                  (loop [wait-until (+ (.millis ^Clock (:clock worker)) duration-ms)]
                    (f worker)
                    (when (< (.millis (:clock worker)) wait-until)
                      (sleep)
                      (recur wait-until))))))))
        fns (mapv compile-task (:tasks benchmark))]
    (fn run-benchmark [sut]
      (let [clock (Clock/systemUTC)
            domain-state (ConcurrentHashMap.)
            custom-state (ConcurrentHashMap.)
            root-random (Random. seed)
            reports (atom [])
            worker (->Worker sut root-random domain-state custom-state clock reports)]
        (doseq [f fns]
          (f worker))
        @reports))))

(defn add-report [worker report]
  (swap! (:reports worker) conj report))
