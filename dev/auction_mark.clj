(ns auction-mark
  (:require [xtdb.api :as xt]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [xtdb.io :as xio]
            [juxt.clojars-mirrors.hiccup.v2v0v0-alpha2.hiccup2.core :as hiccup2]
            [clojure.data.json :as json])
  (:import (java.time Instant Clock Duration)
           (java.util Random Comparator ArrayList)
           (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function Supplier)
           (com.google.common.collect MinMaxPriorityQueue)
           (java.io Closeable File)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmMemoryMetrics JvmGcMetrics JvmThreadMetrics JvmHeapPressureMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           (io.micrometer.core.instrument Meter Measurement MeterRegistry Timer Tag Gauge)))

(set! *warn-on-reflection* false)

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
(def item-bid-id (partial str "ib_"))
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
  '(fn apply-seller-fee [ctx u_id]
     (let [db (xtdb.api/db ctx)
           u (xtdb.api/entity db u_id)]
       (if u
         [[:xtdb.api/put (update u :u_balance dec)]]
         []))))

(def tx-fn-new-bid
  "Transaction function.

  Enters a new bid for an item"
  '(fn new-bid [ctx {:keys [i_id
                            u_id
                            i_buyer_id
                            bid
                            max-bid
                            ;; pass in from ctr rather than select-max+1 so ctr gets incremented
                            new-bid-id
                            ;; 'current timestamp'
                            now]}]
     (let [db (xtdb.api/db ctx)

           ;; current max bid id
           [imb imb_ib_id]
           (-> (quote [:find ?imb, ?imb_ib_id
                       :in [?i_id]
                       :where
                       [?imb :imb_i_id ?i_id]
                       [?imb :imb_u_id ?u_id]
                       [?imb :imb_ib_id ?imb_ib_id]])
               (as-> q (xtdb.api/q db q i_id))
               first)

           ;; current number of bids
           [i nbids]
           (-> (quote [:find ?i, ?nbids
                       :in [?i_id]
                       :where
                       [?i :i_id ?iid]
                       [?i :i_num_bids ?nbids]
                       [?i :i_status 0]])
               (as-> q (xtdb.api/q db q i_id))
               first)

           ;; current bid/max
           [curr-bid, curr-max]
           (when imb_ib_id
             (-> (quote [:find ?bid ?max
                         :in [?imb_ib_id]
                         :where
                         [?ib :ib_id ?imb_ib_id]
                         [?ib :ib_bid ?bid]
                         [?ib :ib_max_bid ?max-bid]])
                 (as-> q (xtdb.api/q db q imb_ib_id))
                 first))

           new-bid-win (or (nil? imb_ib_id) (< curr-max max-bid))
           new-bid (if (and new-bid-win curr-max (< bid curr-max) curr-max) curr-max bid)
           upd-curr-bid (and curr-bid (not new-bid-win) (< curr-bid bid))]

       (cond->
         []
         ;; increment number of bids on item
         i
         (conj [:xtdb.api/put (assoc (xtdb.api/entity db i) :i_num_bids (inc nbids))])

         ;; if new bid exceeds old, bump it
         upd-curr-bid
         (conj [:xtdb.api/put (assoc (xtdb.api/entity db imb) :imb_bid bid)])

         ;; we exceed the old max, win the bid.
         (and curr-bid new-bid-win)
         (conj [:xtdb.api/put (assoc (xtdb.api/entity db imb) :imb_ib_id new-bid-id
                                                              :imb_ib_u_id u_id :imb_updated now)])
         ;; no previous max bid, insert new max bid
         (nil? imb_ib_id)
         (conj [:xtdb.api/put {:xt/id new-bid-id
                               :imb_i_id i_id
                               :imb_u_id u_id
                               :imb_ib_id new-bid-id
                               :imb_ib_i_id i_id
                               :imb_ib_u_id u_id
                               :imb_created now
                               :imb_updated now}])

         :always
         ;; add new bid
         (conj [:xtdb.api/put {:xt/id new-bid-id
                               :ib_id new-bid-id
                               :ib_i_id i_id
                               :ib_u_id u_id
                               :ib_buyer_id i_buyer_id
                               :ib_bid new-bid
                               :ib_max_bid max-bid
                               :ib_created_at now
                               :ib_updated now}])))))

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
                       :i_status :open}]]
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

;; represents a probable state of an item that can be sampled randomly
(defrecord ItemSample [i_id, i_u_id, i_status, i_end_date, i_num_bids])

(defn- project-item-status
  [i_status, ^Instant i_end_date, i_num_bids, ^Instant now]
  (let [remaining (- (.toEpochMilli i_end_date) (.toEpochMilli now))
        item-ending-soon-ms (* 36000 1000)]
    (cond
      (<= remaining 0) :closed
      (< remaining item-ending-soon-ms) :ending-soon
      (and (pos? i_num_bids) (not= :closed i_status)) :waiting-for-purchase
      :else i_status)))

(defn item-status-groups [db ^Instant now]
  (with-open [items (xt/open-q db
                               '[:find ?i, ?i_u_id, ?i_status, ?i_end_date, ?i_num_bids
                                 :where
                                 [?i :i_id ?i_id]
                                 [?i :i_u_id ?i_u_id]
                                 [?i :i_status ?i_status]
                                 [?i :i_end_date ?i_end_date]
                                 [?i :i_num_bids ?i_num_bids]])]
    (let [all (ArrayList.)
          open (ArrayList.)
          ending-soon (ArrayList.)
          waiting-for-purchase (ArrayList.)
          closed (ArrayList.)]
      (doseq [[i_id i_u_id i_status ^Instant i_end_date i_num_bids] (iterator-seq items)
              :let [projected-status (project-item-status i_status i_end_date i_num_bids now)

                    ^ArrayList alist
                    (case projected-status
                      :open open
                      :closed closed
                      :waiting-for-purchase waiting-for-purchase
                      :ending-soon ending-soon)

                    item-sample (->ItemSample i_id i_u_id i_status i_end_date i_num_bids)]]
        (.add all item-sample)
        (.add alist item-sample))
      {:all (vec all)
       :open (vec open)
       :ending-soon (vec ending-soon)
       :waiting-for-purchase (vec waiting-for-purchase)
       :closed (vec closed)})))

;; do every now and again to provide inputs for item-dependent computations
(defn index-item-status-groups [worker]
  (let [{:keys [node, ^ConcurrentHashMap custom-state, ^Clock clock]} worker
        now (.instant clock)]
    (with-open [db (xt/open-db node)]
      (.putAll custom-state {:item-status-groups (item-status-groups db now)}))))

(defn random-nth [worker coll]
  (when (seq coll)
    (let [idx (.nextInt (rng worker) (count coll))]
      (nth coll idx nil))))

(defn random-item [worker & {:keys [status] :or {status :all}}]
  (let [isg (-> worker :custom-state :item-status-groups (get status) vec)
        item (random-nth worker isg)]
    item))

(defn- generate-new-bid-params [worker]
  (let [{:keys [i_id, i_u_id]} (random-item worker :status :open)
        i_buyer_id (sample-gaussian worker user-id)]
    (if (and i_buyer_id (= i_buyer_id i_u_id))
      (generate-new-bid-params worker)
      {:i_id i_id,
       :i_u_id i_u_id,
       :ib_buyer_id i_buyer_id
       :bid (random-price worker)
       :max-bid (random-price worker)
       :new-bid-id (increment worker item-bid-id)
       :now (current-timestamp worker)})))

(defn proc-new-bid [worker]
  (let [params (generate-new-bid-params worker)]
    (when (and (:i_id params) (:i_u_id params))
      (xt/submit-tx (:node worker) [[::xt/fn :new-bid params]]))))

(defn proc-get-item [worker]
  (let [{:keys [node]} worker
        ;; the benchbase project uses a profile that keeps item pairs around
        ;; selects only closed items for a particular user profile (they are sampled together)
        ;; right now this is a totally random sample with one less join than we need.
        i_id (sample-flat worker item-id)
        q '[:find (pull ?i [:i_id, :i_u_id, :i_initial_price, :i_current_price])
            :in [?iid]
            :where
            [?i :i_id ?iid]
            [?i :i_status 0]]]
    (xt/q (xt/db node) q i_id)))

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

   :fns
   {:apply-seller-fee #'tx-fn-apply-seller-fee
    :new-bid #'tx-fn-new-bid}

   :tasks
   [[:proc #'proc-new-user {:weight 0.5}]
    [:proc #'proc-new-item {:weight 1.0}]
    [:proc #'proc-get-item {:weight 12.0}]
    [:proc #'proc-new-bid {:weight 2.0}]
    [:job #'index-item-status-groups {:freq {:ratio 0.2}}]]})

;; measurement

;; histogram (global and per proc)
;; eager query latency (q)
;; lazy query open latency (open-q)
;; lazy query open lifetime (open-q to close)
;; submit latency
;; transaction e2e latency (e.g submit-2-index)

;; transaction is the tricky one
;; will have to put the tx-id in a sorted set and have a listener thread mark it completed if it is below or equal to the last tx-id.

(def ^:dynamic *uow* nil)

(defn meter-reg ^MeterRegistry []
  (let [meter-reg (SimpleMeterRegistry.)
        bind-metrics (fn [& metrics] (run! #(.bindTo % meter-reg) metrics))]
    (bind-metrics
      (ClassLoaderMetrics.)
      (JvmMemoryMetrics.)
      (JvmHeapPressureMetrics.)
      (JvmGcMetrics.)
      (ProcessorMetrics.)
      (JvmThreadMetrics.))
    meter-reg))

(defrecord MeterSample [meter time-ms statistic value])

(defn meter-sampler
  "Not thread safe, call to take a sample of meters in the given registry.
  Should be called on a sampler thread as part of a benchmark/load run.

  Can be called (sampler :summarize) for a data summary of the captured metric time series."
  [^MeterRegistry meter-reg]
  ;; most naive impl possible right now - can simply vary the sample rate according to duration / dimensionality
  ;; if memory is an issue
  (let [sample-list (ArrayList.)]
    (fn send-msg
      ([] (send-msg :sample))
      ([msg]
       (case msg
         :summarize
         (vec (for [[meter-name samples]
                    (group-by
                      (fn [{:keys [^Meter meter]}]
                        (-> meter .getId .getName))
                      sample-list)
                    [^Meter meter samples] (group-by :meter samples)
                    :let [series (->> meter .getId .getTagsAsIterable
                                      (map (fn [^Tag t] (str (.getKey t) "=" (.getValue t))))
                                      (str/join ", "))
                          series (not-empty series)]
                    [statistic samples] (group-by :statistic samples)]
                ;; todo count->rate automatically on non-neg deriv transform (can be a new 'statistic' dimension of counts)
                {:id (str/join " " [meter-name statistic series])
                 :metric (str/join " " [meter-name statistic])
                 :meter meter-name
                 :unit (if (= "count" statistic)
                         "count"
                         (-> meter .getId .getBaseUnit))
                 :series series
                 :statistic statistic
                 :samples (mapv (fn [{:keys [value, time-ms]}]
                                  {:value value
                                   :time-ms time-ms})
                                samples)}))
         :sample
         (let [time-ms (System/currentTimeMillis)]
           (doseq [^Meter meter (.getMeters meter-reg)
                   ^Measurement measurement (.measure meter)]
             (.add sample-list (->MeterSample meter time-ms (.getTagValueRepresentation (.getStatistic measurement)) (.getValue measurement))))))))))

(def percentiles
  [0.75 0.85 0.95 0.98 0.99 0.999])

(defn install-proxy-node-meters!
  [^MeterRegistry meter-reg]
  (let [timer #(-> (Timer/builder %)
                   (.minimumExpectedValue (Duration/ofNanos 1))
                   (.maximumExpectedValue (Duration/ofMinutes 2))
                   (.publishPercentiles (double-array percentiles))
                   (.register meter-reg))]
    {:submit-tx-timer (timer "node.submit-tx")}))

(defn new-fn-gauge
  ([reg meter-name f] (new-fn-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f opts]
   (-> (Gauge/builder
         meter-name
         (reify Supplier
           (get [_] (f))))
       (cond-> (:unit opts) (.baseUnit (str (:unit opts))))
       (.register reg))))

(defn bench-proxy [node ^MeterRegistry meter-reg]
  (let [last-submitted (atom nil)
        last-completed (atom nil)

        submit-counter (AtomicLong.)
        indexed-counter (AtomicLong.)

        {:keys [^Timer submit-tx-timer]}
        (install-proxy-node-meters! meter-reg)

        fn-gauge (partial new-fn-gauge meter-reg)

        in-cb (fn [tx]
                (reset! last-completed tx)
                (.getAndIncrement indexed-counter)
                nil)

        listener (xt/listen node {::xt/event-type ::xt/indexed-tx} in-cb)

        compute-lag-nanos
        (fn []
          (or
            (when-some [[{::xt/keys [tx-id]} ms] @last-submitted]
              (when-some [{completed-tx-id ::xt/tx-id
                           completed-tx-time ::xt/tx-time} @last-completed]
                (when (< completed-tx-id tx-id)
                  (* (long 1e6) (- ms (inst-ms completed-tx-time))))))
            0))

        compute-lag-count
        (fn [] (- (.get submit-counter) (.get indexed-counter)))]

    (fn-gauge "node.tx.lag.transactions" compute-lag-count {:unit "transactions"})
    (fn-gauge "node.tx.lag.delay" (comp #(/ % 1e9) compute-lag-nanos) {:unit "seconds"})
    (fn-gauge "node.kv.keys" #(:xtdb.kv/estimate-num-keys (xt/status node) 0) {:unit "count"})
    (fn-gauge "node.kv.size" #(:xtdb.kv/size (xt/status node) 0) {:unit "bytes"})

    (reify xt/PXtdb
      (status [_] (xt/status node))
      (tx-committed? [_ submitted-tx] (xt/tx-committed? node submitted-tx))
      (sync [_] (xt/sync node))
      (sync [_ timeout] (xt/sync node timeout))
      (sync [_ tx-time timeout] (xt/sync node tx-time timeout))
      (await-tx-time [_ tx-time] (xt/await-tx-time node tx-time))
      (await-tx-time [_ tx-time timeout] (xt/await-tx-time node tx-time timeout))
      (await-tx [_ tx] (xt/await-tx node tx))
      (await-tx [_ tx timeout] (xt/await-tx node tx timeout))
      (listen [node event-opts f] (xt/listen node event-opts f))
      (latest-completed-tx [_] (xt/latest-completed-tx node))
      (latest-submitted-tx [_] (xt/latest-submitted-tx node))
      (attribute-stats [_] (xt/attribute-stats node))
      (active-queries [_] (xt/active-queries node))
      (recent-queries [_] (xt/recent-queries node))
      (slowest-queries [_] (xt/slowest-queries node))
      xt/PXtdbSubmitClient
      (submit-tx-async [_ tx-ops] (xt/submit-tx-async node tx-ops))
      (submit-tx-async [_ tx-ops opts] (xt/submit-tx-async node tx-ops opts))
      (submit-tx [this tx-ops] (xt/submit-tx this tx-ops {}))
      (submit-tx [_ tx-ops opts]
        (let [ret (.recordCallable ^Timer submit-tx-timer ^Callable (fn [] (xt/submit-tx node tx-ops opts)))]
          (reset! last-submitted [ret (System/currentTimeMillis)])
          (.incrementAndGet submit-counter)
          ret))
      (open-tx-log [_ after-tx-id with-ops?] (xt/open-tx-log node after-tx-id with-ops?))
      xt/DBProvider
      (db [_] (xt/db node))
      (db [_ valid-time-or-basis] (xt/db node valid-time-or-basis))
      (open-db [_] (xt/open-db node))
      (open-db [_ valid-time-or-basis] (xt/open-db node valid-time-or-basis))
      Closeable
      (close [_] (.close listener)))))

(defn setup [node benchmark]
  (let [{:keys [loaders, fns]} benchmark
        clock (Clock/systemUTC)
        seed 0
        random (Random. seed)
        domain-state (ConcurrentHashMap.)
        custom-state (ConcurrentHashMap.)
        node-proxy node
        worker
        (map->Worker
          {:node node-proxy,
           :random random,
           :domain-state domain-state
           :custom-state custom-state
           :clock clock})]

    (log/info "Inserting transaction functions")
    (->> (for [[id fvar] fns]
           [::xt/put {:xt/id id, :xt/fn @fvar}])
         (xt/submit-tx node-proxy))

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
                  (xt/submit-tx node-proxy (mapv (partial vector ::xt/put) chunk))))))]
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
         think-duration Duration/ZERO}
    :as args}]
  (let [domain-state (doto (ConcurrentHashMap.) (.putAll domain-state))
        custom-state (doto (ConcurrentHashMap.) (.putAll custom-state))

        meter-reg (meter-reg)
        meter-sampler (meter-sampler meter-reg)

        sample-meters #(meter-sampler :sample)
        summarize-metrics #(meter-sampler :summarize)

        node-proxy (bench-proxy node meter-reg)

        ;; mix seed
        random (Random. (.nextLong (Random. seed)))
        clock (Clock/systemUTC)

        uow-ctr (atom -1)
        uow-defs*
        (for [[t & args] (:tasks benchmark)
              :let [uow-id (swap! uow-ctr inc)]]
          (case t
            :job
            (let [[f opts] args
                  {:keys [freq]
                   :or {freq {:ratio 0.1}}} opts
                  uow-name (if (var? f) (name (.toSymbol ^clojure.lang.Var f)) "anon")]
              {:uow-id uow-id
               :uow-name uow-name
               :freq freq
               :proc f
               :counter (AtomicLong.)})

            :proc
            (let [[f opts] args
                  {:keys [weight]
                   :or {weight 1}} opts
                  uow-name (if (var? f) (name (.toSymbol ^clojure.lang.Var f)) "anon")]
              {:uow-id uow-id
               :uow-name uow-name
               :pool true
               :weight weight
               :proc f
               :counter (AtomicLong.)})))

        instrument-uow
        (fn [{:keys [uow-name, proc] :as uow}]
          (let [timer
                (-> (Timer/builder (str "bench." uow-name))
                    (.publishPercentiles (double-array percentiles))
                    (.maximumExpectedValue duration)
                    (.minimumExpectedValue (Duration/ofNanos 1))
                    (.register meter-reg))]
            (assoc uow :proc (fn instrumented-proc [worker] (.recordCallable ^Timer timer ^Callable (fn [] (proc worker)))))))

        uow-defs (map instrument-uow uow-defs*)

        pooled-uow-defs
        (filter :pool uow-defs)

        job-defs
        (remove :pool uow-defs)

        sample-pooled-uow
        (weighted-sample-fn (map (juxt identity :weight) pooled-uow-defs))

        think-ms (.toMillis ^Duration think-duration)

        thread-defs
        (concat
          (for [{:keys [uow-name, freq] :as job} job-defs
                :let [{:keys [ratio]} freq
                      ms (long (* ratio (.toMillis ^Duration duration)))
                      think-first (atom false)]]
            {:thread-name uow-name
             :think (fn think-job [_]
                      (when @think-first (when (pos? ms) (Thread/sleep ms)))
                      (reset! think-first true)
                      job)
             :random random})
          (for [thread-id (range threads)
                :let [thread-name (str (:name benchmark "benchmark-pool") " " (format "[%s/%s]" (inc thread-id) threads))
                      random (Random. (.nextLong random))]]
            {:thread-name thread-name
             :think (fn think-proc [worker] (Thread/sleep think-ms) (sample-pooled-uow (rng worker)))
             :random random}))

        thread-loop
        (fn [{:keys [random, think]}]
          (let [thread (Thread/currentThread)
                worker (map->Worker {:node node-proxy
                                     :random random
                                     :domain-state domain-state
                                     :custom-state custom-state
                                     :clock clock})]
            (while (not (.isInterrupted thread))
              (try
                (when-some [{:keys [uow-name, ^AtomicLong counter, proc] :as uow} (think worker)]
                  (try
                    (binding [*uow* uow]
                      (proc worker))
                    (.incrementAndGet counter)
                    (catch InterruptedException _ (.interrupt thread))
                    (catch Throwable e
                      (log/errorf e "Benchmark proc uncaught exception (uow %s), exiting thread" uow-name)
                      (.interrupt thread))))
                (catch InterruptedException _
                  (.interrupt thread))))))

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

        run-start-nanos (atom (System/nanoTime))
        run-time-nanos (atom nil)

        calc-rps
        (fn [n]
          (* 1e9 (/ n @run-time-nanos)))

        sample-rate-ms (max 1000 (/ duration-ms 60))]

    ;; start the benchmark
    (try
      (System/gc)
      @threads-delay
      (let [start (reset! run-start-nanos (System/nanoTime))
            desired-end (long (+ (.toNanos duration) (System/nanoTime)))]
        (while (< (System/nanoTime) desired-end)
          (sample-meters)
          (Thread/sleep sample-rate-ms))
        (reset! run-time-nanos (- (System/nanoTime) start)))
      (finally
        (when (realized? threads-delay)
          (run! #(.interrupt %) @threads-delay)
          (run! #(.join %) @threads-delay))
        (.close node-proxy)))

    (when-not @run-time-nanos
      (reset! run-time-nanos (- (System/nanoTime) @run-start-nanos)))

    (with-meta
      {:duration @run-time-nanos
       :metrics (summarize-metrics)
       :stats (:stats node-proxy)
       :rps (calc-rps (reduce + 0N (map #(.get (:counter %)) uow-defs)))
       :transactions (vec (for [{:keys [uow-name, ^AtomicLong counter]} uow-defs]
                            {:name uow-name,
                             :rps (calc-rps (.get counter))
                             :count (.get counter)}))}
      {:node node,
       :args, args,
       :domain-state domain-state,
       :custom-state custom-state})))

(defn run-more [results]
  (let [{:keys [args, domain-state, custom-state]} (meta results)]
    (run (merge args {:domain-state domain-state, :custom-state custom-state}))))

(defn post-bench-worker [results]
  (let [{:keys [node, domain-state, custom-state]} (meta results)]
    (assert (and node domain-state custom-state))
    (->Worker node (Random.) domain-state custom-state (Clock/systemUTC))))

(defn bench [benchmark & {:keys [run-opts,
                                 node-opts],
                          :or {run-opts {}, node-opts {}}}]
  (log/info "Starting node")
  (with-open [node (xt/start-node node-opts)]
    (log/info "Starting setup")
    (let [bench-state (setup node benchmark)]
      (log/info "Starting run")
      (run (merge bench-state run-opts)))))

(defn rocks-opts []
  (let [kv (fn [] {:xtdb/module 'xtdb.rocksdb/->kv-store,
                   :db-dir-suffix "rocksdb"
                   :db-dir (xio/create-tmpdir "auction-mark")})]
    {:xtdb/tx-log {:kv-store (kv)}
     :xtdb/document-store {:kv-store (kv)}
     :xtdb/index-store {:kv-store (kv)}}))

(defn lmdb-opts []
  (let [kv (fn [] {:xtdb/module 'xtdb.lmdb/->kv-store,
                   :db-dir-suffix "lmdb"
                   :db-dir (xio/create-tmpdir "auction-mark")})]
    {:xtdb/tx-log {:kv-store (kv)}
     :xtdb/document-store {:kv-store (kv)}
     :xtdb/index-store {:kv-store (kv)}}))

(defn rocks-node [] (xt/start-node (rocks-opts)))

(defn tx-view [node tx-id]
  (with-open [cur (xt/open-tx-log node (- tx-id 100) true)]
    (->> (iterator-seq cur)
         (take 1000)
         (some #(when (= tx-id (::xt/tx-id %))
                  %)))))

(defn pretty-nanos [nanos]
  (str (Duration/ofNanos nanos)))

;; todo requests over time

(defn find-transaction [rs name]
  (some #(when (= name (:name %)) %) (:transactions rs)))

(defn vega-plots [metric-data]
  (vec
    (for [[metric metric-data] (sort-by key (group-by :metric metric-data))]
      {:title metric
       :hconcat (vec (for [[[_statistic unit] metric-data] (sort-by key (group-by (juxt :statistic :unit) metric-data))
                           :let [data {:values (vec (for [{:keys [vs-label, series, samples]} metric-data
                                                          {:keys [time-ms, value]} samples
                                                          :when (Double/isFinite value)]
                                                      {:time (str time-ms)
                                                       :facet vs-label
                                                       :series series
                                                       :value value}))
                                       :format {:parse {:time "utc:'%Q'"}}}
                                 any-series (some (comp not-empty :series) metric-data)
                                 spec {:mark {:type "area", :line true}
                                       :encoding {:x {:field "time"
                                                      :type "temporal"
                                                      :title "Time"}
                                                  :y {:field "value"
                                                      :type "quantitative"
                                                      :title (or unit "Value")}
                                                  :color
                                                  (when (and any-series (< 1 (count metric-data)))
                                                    {:field "series",
                                                     :legend {}
                                                     :type "nominal"})}}]]
                       (if (some :vs-label metric-data)
                         {:data data
                          :facet {:column {:field "facet"}
                                  :header {:title nil}}
                          :spec spec}
                         (assoc spec :data data))))})))

(defn group-metrics [rs]
  (let [{:keys [metrics]} rs

        group-fn
        (fn [{:keys [meter]}]
          (condp #(str/starts-with? %2 %1) meter
            "bench." "001 - Benchmark"
            "node." "002 - XTDB Node"
            "jvm.gc" "003 - JVM Memory / GC"
            "jvm.memory" "003 - JVM Memory / GC"
            "jvm.buffer" "004 - JVM Buffer"
            "system." "005 - System / Process"
            "process." "005 - System / Process"
            "006 - Other"))

        metric-groups (group-by group-fn metrics)]

    metric-groups))

(defn hiccup-report [title result & more]
  (let [id-from-thing
        (let [ctr (atom 0)]
          (memoize (fn [_] (str "id" (swap! ctr inc)))))]
    (list
      [:html
       [:head
        [:title title]
        [:meta {:charset "utf-8"}]
        [:script {:src "https://cdn.jsdelivr.net/npm/vega@5.22.1"}]
        [:script {:src "https://cdn.jsdelivr.net/npm/vega-lite@5.6.0"}]
        [:script {:src "https://cdn.jsdelivr.net/npm/vega-embed@6.21.0"}]
        [:style {:media "screen"}
         ".vega-actions a {
          margin-right: 5px;
        }"]]
       [:body
        [:h1 title]
        (for [[group metric-data] (sort-by key (group-metrics result))]
          (list [:h2 group]
                (for [meter (sort (set (map :meter metric-data)))]
                  [:div {:id (id-from-thing meter)}])))
        [:script
         (->> (for [[meter metric-data] (group-by :meter (:metrics result))]
                (format "vegaEmbed('#%s', %s);" (id-from-thing meter)
                        (json/write-str
                          {:hconcat (vega-plots metric-data)})))
              (str/join "\n")
              hiccup2/raw)]]])))

(defn show-html-report [title rs]
  (let [f (File/createTempFile "xtdb-benchmark-report" ".html")]
    (spit f (hiccup2/html
              {}
              (hiccup-report title rs)))
    (clojure.java.browse/browse-url (io/as-url f))))

(defn- normalize-time [report]
  (let [{:keys [metrics]} report
        min-time (->> metrics
                      (mapcat :samples)
                      (reduce #(min %1 (:time-ms %2)) Long/MAX_VALUE))
        new-metrics (for [metric metrics
                          :let [{:keys [samples]} metric
                                new-samples (mapv #(update % :time-ms - min-time) samples)]]
                      (assoc metric :samples new-samples))]
    (assoc report :metrics (vec new-metrics))))

(defn vs [label report & more]
  (let [pair-seq (cons [label report] (partition 2 more))
        ;; with order
        key-seq (map first pair-seq)
        ;; index without order
        report-map (apply hash-map label report more)]
    {:metrics (vec (for [[i label] (map-indexed vector key-seq)
                         :let [{:keys [metrics]} (normalize-time (report-map label))]
                         metric metrics]
                     (assoc metric :vs-label (str label))))}))
