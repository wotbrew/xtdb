(ns auction-mark
  (:require [xtdb.api :as xt]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import (java.time Instant Clock Duration)
           (java.util Random Comparator)
           (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function)
           (com.google.common.collect MinMaxPriorityQueue)))

(defrecord Worker [node random domain-state custom-state clock])

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

(defn random-price [worker] (.nextDouble (rng worker)))

(def user-id (partial str "u_"))
(def region-id (partial str "r_"))
(def item-id (partial str "i_"))
(def category-id (partial str "c_"))
(def global-attribute-group-id (partial str "gag_"))
(def global-attribute-value-id (partial str "gav_"))

(def user-attribute (id))
(def item-name (id))
(def item-description (id))
(def initial-price (id))
(def reserve-price (id))
(def buy-now (id))
(def item-attributes-blob (id))
(def item-image-path (id))
(def auction-start-date (id))

(defn proc-new-user
  "Creates a new USER record. The rating and balance are both set to zero.

  The benchmark randomly selects id from a pool of region ids as an input for u_r_id parameter using flat distribution."
  [worker]
  (let [u_id (increment worker user-id)]
    (->> [[::xt/put {:xt/id u_id
                     :u_id u_id
                     :u_r_id (sample-flat worker region-id)
                     :u_rating 0
                     :u_balance 0.0
                     :u_created (current-timestamp worker)
                     :u_sattr0 (random-str worker)
                     :u_sattr1 (random-str worker)
                     :u_sattr2 (random-str worker)
                     :u_sattr3 (random-str worker)
                     :u_sattr4 (random-str worker)
                     :u_sattr5 (random-str worker)
                     :u_sattr6 (random-str worker)
                     :u_sattr7 (random-str worker)}]]
         (xt/submit-tx (:node worker)))))

(def tx-fn-apply-seller-fee
  '(fn [ctx u_id]
     (let [db (xtdb.api/db ctx)
           u (xt/entity db u_id)]
       (if u
         [[::xt/put (update u :u_balance dec)]]
         []))))

(def tx-fn-new-bids
  '(fn [ctx i_id]
     (let [db (xt/db ctx)
           [i nbids]
           (first
             (xt/q db '[:find ?i, ?nbids
                        :in [?iid]
                        :where
                        [?i :i_id ?iid]
                        [?i :i_num_bids ?nbids]
                        [?i :i_status 0]] i_id))]
       ;; increment number of bids
       (when i
         [[::xt/put {:xt/id i, :i_num_bids (inc nbids)}]])

       )))

(defn- sample-category-id [worker]
  (if-some [weighting (::category-weighting (:custom-state worker))]
    (weighting (rng worker))
    (sample-gaussian worker category-id)))

(defn proc-new-item
  "Insert a new ITEM record for a user.

  The benchmark client provides all the preliminary information required for the new item, as well as optional information to create derivative image and attribute records.
  After inserting the new ITEM record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and ITEM IMAGE.

  After these records are inserted, the transaction then updates the USER record to add the listing fee to the seller’s balance.

  The benchmark randomly selects id from a pool of users as an input for u_id parameter using Gaussian distribution. A c_id parameter is randomly selected using a flat histogram from the real auction site’s item category statistic."
  [worker]
  (let [i_id-raw (.getAndIncrement (counter worker item-id))
        i_id (item-id i_id-raw)
        u_id (sample-gaussian worker user-id)
        c_id (sample-category-id worker)
        name (random-str worker)
        description (random-str worker)
        initial-price (random-price worker)
        attributes (random-str worker)
        gag-ids (remove nil? (random-seq worker {:min 0, :max 16, :unique true} sample-flat global-attribute-group-id))
        gav-ids (remove nil? (random-seq worker {:min 0, :max 16, :unique true} sample-flat global-attribute-value-id))
        images (random-seq worker {:min 0, :max 16, :unique true} random-str)
        start-date (current-timestamp worker)
        ;; up to 42 days
        end-date (.plusSeconds ^Instant start-date (* 60 60 24 (* (inc (.nextInt (rng worker) 42)))))
        ;; append attribute names to desc
        description-with-attributes
        (let [q '[:find ?gag-name ?gav-name
                  ;; todo check this param syntax
                  :in [?gag-id ...] [?gav-id ...]
                  :where
                  [?gav-gag-id :gav_gag_id ?gag-id]
                  [?gag-id :gag_name ?gag-name]
                  [?gav-id :gav_name ?gav-name]]]
          (->> (xt/q (xt/db (:node worker)) q gag-ids gav-ids)
               (str/join " ")
               (str description " ")))]

    (->> (concat
           [[::xt/put {:xt/id i_id
                       :i_id i_id
                       :i_u_id u_id
                       :i_c_id c_id
                       :i_name name
                       :i_description description-with-attributes
                       :i_user_attributes attributes
                       :i_initial_price initial-price
                       :i_num_bids 0
                       :i_num_images (count images)
                       :i_num_global_attrs (count gav-ids)
                       :i_start_date start-date
                       :i_end_date end-date
                       ;; open
                       :i_status 0}]]
           ;; todo fill
           (for [[i image] (map-indexed vector images)
                 :let [ii_id (bit-or (bit-shift-left i 60) (bit-and i_id-raw 0x0FFFFFFFFFFFFFFF))]]
             [::xt/put {:xt/id (str "ii_" ii_id)
                        :ii_id ii_id
                        :ii_i_id i_id
                        :ii_u_id u_id
                        :ii_path image}])
           ;; reg tx fn?
           [[::xt/fn :apply-seller-fee u_id]])
         (xt/submit-tx (:node worker)))))

(defn proc-new-bid!
  [worker])

(defn read-category-tsv []
  (let [cat-tsv-rows
        (with-open [rdr (io/reader (io/resource "auctionmark-categories.tsv"))]
          (vec (for [line (line-seq rdr)
                     :let [split (str/split line #"\t")
                           cat-parts (butlast split)
                           item-count (last split)
                           parts (remove str/blank? cat-parts)]]
                 {:parts (vec parts)
                  :item-count (parse-long item-count)})))
        extract-cats
        (fn extract-cats [parts]
          (when (seq parts)
            (cons parts (extract-cats (pop parts)))))
        all-paths (into #{} (comp (map :parts) (mapcat extract-cats)) cat-tsv-rows)
        path-i (into {} (map-indexed (fn [i x] [x i])) all-paths)
        trie (reduce #(assoc-in %1 (:parts %2) (:item-count %2)) {} cat-tsv-rows)
        trie-node-item-count (fn trie-node-item-count [path]
                               (let [n (get-in trie path)]
                                 (if (integer? n)
                                   n
                                   (reduce + 0 (map trie-node-item-count (keys n))))))]
    (->> (for [[path i] path-i]
           [(category-id i)
            {:i i
             :id (category-id i)
             :category-name (str/join "/" path)
             :parent (category-id (path-i i))
             :item-count (trie-node-item-count path)}])
         (into {}))))

(defn- load-categories-tsv [worker]
  (let [cats (read-category-tsv)
        {:keys [^ConcurrentHashMap custom-state]} worker]
    ;; squirrel these data-structures away for later (see category-generator, sample-category-id)
    (.putAll custom-state {::categories cats
                           ::category-weighting (weighted-sample-fn (map (juxt :id :item-count) (vals cats)))})))

(defn generate-region [worker]
  (let [r-id (increment worker category-id)]
    {:xt/id r-id
     :r_id r-id
     :r_name (random-str worker 6 32)}))

(defn generate-category [worker]
  (let [{::keys [categories]} (:custom-state worker)
        c-id (increment worker category-id)
        {:keys [category-name, parent]} (categories c-id)]
    {:xt/id c-id
     :c_id c-id
     :c_parent_id (when (seq parent) (:id (categories parent)))
     :c_name (or category-name (random-str worker 6 32))}))

(def auction-mark
  {:loaders
   [[:f #'load-categories-tsv]
    [:generator #'generate-region {:n 75}]
    [:generator #'generate-category {:n 16908}]]

   :fns {:apply-seller-fee #'tx-fn-apply-seller-fee}

   :tasks
   [[:proc #'proc-new-user {}]
    [:proc #'proc-new-item {:weight 2}]]})

(defn setup [node benchmark]
  (let [{:keys [loaders, fns]} benchmark
        clock (Clock/systemUTC)
        seed 0
        random (Random. seed)
        domain-state (ConcurrentHashMap.)
        custom-state (ConcurrentHashMap.)
        worker
        (map->Worker
          {:node node,
           :random random,
           :domain-state domain-state
           :custom-state custom-state
           :clock clock})]

    (log/info "Inserting transaction functions")
    (->> (for [[id fvar] fns]
           ;; todo wrap with histo trace
           [::xt/put {:xt/id id, :xt/fn @fvar}])
         (xt/submit-tx node))

    (log/info "Loading data")
    (let [run-loader
          (fn [[t & args]]
            (case t
              :f (apply (first args) worker (rest args))
              :generator
              (let [[f opts] args
                    {:keys [n] :or {n 0}} opts
                    doc-seq (repeatedly n (partial f worker))
                    partition-count 512]
                (doseq [chunk (partition-all partition-count doc-seq)]
                  (xt/submit-tx node (mapv (partial vector ::xt/put) chunk))))))]
      (run! run-loader loaders)
      (xt/sync node)
      {:node node,
       :domain-state (into {} domain-state)
       :custom-state (into {} custom-state)
       :benchmark benchmark
       :seed seed})))

(defn run
  [{:keys [node,
           domain-state,
           custom-state,
           benchmark,
           threads,
           seed
           duration
           think-duration]
    :or {domain-state {}
         threads (+ 2 (.availableProcessors (Runtime/getRuntime)))
         seed 0
         duration (Duration/parse "PT30S")
         think-duration Duration/ZERO}}]
  (let [domain-state (doto (ConcurrentHashMap.) (.putAll domain-state))
        custom-state (doto (ConcurrentHashMap.) (.putAll custom-state))

        ;; mix seed
        random (Random. (.nextLong (Random. seed)))
        clock (Clock/systemUTC)

        uow-ctr (atom -1)
        uow-defs
        (for [[t & args] (:tasks benchmark)
              :let [uow-id (swap! uow-ctr inc)]]
          (case t
            :proc
            (let [[f opts] args
                  {:keys [weight]
                   :or {weight 1}} opts]
              {:uow-id uow-id
               :uow-name (str uow-id "-" (if (var? f) (.toSymbol ^clojure.lang.Var f) "anon"))
               :weight weight
               :proc f
               :counter (AtomicLong.)})))

        pooled-uow-defs uow-defs

        _ (def pooled-uow-defs uow-defs)

        sample-pooled-uow
        (weighted-sample-fn (map (juxt identity :weight) pooled-uow-defs))

        think-ms (.toMillis ^Duration think-duration)

        thread-defs
        (for [thread-id (range threads)
              :let [thread-name (str (:name benchmark "benchmark") " " (format "[%s/%s]" (inc thread-id) threads))
                    random (Random. (.nextLong random))]]
          {:thread-id thread-id
           :thread-name thread-name
           :think (fn think [worker] (Thread/sleep think-ms) (sample-pooled-uow (rng worker)))
           :random random})

        thread-loop
        (fn [{:keys [random, think]}]
          (let [thread (Thread/currentThread)
                worker (map->Worker {:node node
                                     :random random
                                     :domain-state domain-state
                                     :custom-state custom-state
                                     :clock clock})]
            (while (not (.isInterrupted thread))
              (when-some [{:keys [uow-name, ^AtomicLong counter, proc]} (think worker)]
                (try
                  (proc worker)
                  (.incrementAndGet counter)
                  (catch InterruptedException _ (.interrupt thread))
                  (catch Throwable e
                    (log/errorf e "Benchmark proc uncaught exception (uow %s), exiting thread" uow-name)
                    (.interrupt thread)))))))

        start-thread
        (fn [{:keys [thread-name] :as thread-def}]
          (let [bindings (get-thread-bindings)]
            (doto (Thread. ^Runnable
                           (fn []
                             (push-thread-bindings bindings)
                             (thread-loop thread-def))
                           (str thread-name))
              .start)))

        threads-delay (delay (mapv start-thread thread-defs))
        duration-ms (.toMillis ^Duration duration)

        run-start-ms (atom (System/currentTimeMillis))
        run-time-ms (atom nil)]

    ;; start the benchmark
    (try
      @threads-delay
      (let [start (reset! run-start-ms (System/currentTimeMillis))]
        (Thread/sleep duration-ms)
        (reset! run-time-ms (- (System/currentTimeMillis) start)))
      (finally
        (when (realized? threads-delay)
          (run! #(.interrupt ^Thread %) @threads-delay)
          (run! #(.join ^Thread %) @threads-delay))))

    (when-not @run-time-ms
      (reset! run-time-ms (- (System/currentTimeMillis) @run-start-ms)))

    (with-meta
      {:run-time-ms @run-time-ms
       :transactions (vec (for [{:keys [proc, ^AtomicLong counter]} uow-defs]
                            {:name (str proc)
                             :count (.get counter)
                             :tps (double (/ (.get counter) @run-time-ms))}))}
      {:node node, :domain-state domain-state, :custom-state custom-state})))

(defn bench [benchmark & {:keys [run-opts, node-opts], :or {run-opts {}, node-opts {}}}]
  (log/info "Starting node")
  (with-open [node (xt/start-node node-opts)]
    (log/info "Starting setup")
    (let [bench-state (setup node benchmark)]
      (log/info "Starting run")
      (:results (run (merge bench-state run-opts))))))
