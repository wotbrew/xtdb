(ns auction-mark
  (:require [xtdb.api :as xt]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [xtdb.io :as xio]
            [juxt.clojars-mirrors.hiccup.v2v0v0-alpha2.hiccup2.core :as hiccup2]
            [clojure.data.json :as json]
            [xtdb.bus :as bus]
            [clojure.java.shell :as sh])
  (:import (java.time Instant Clock Duration LocalDate LocalDateTime ZoneId)
           (java.util Random Comparator ArrayList UUID)
           (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent ConcurrentHashMap Executors TimeUnit)
           (java.util.function Function Supplier)
           (com.google.common.collect MinMaxPriorityQueue)
           (java.io Closeable File)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmMemoryMetrics JvmGcMetrics JvmThreadMetrics JvmHeapPressureMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           (io.micrometer.core.instrument Meter Measurement MeterRegistry Timer Tag Gauge Timer$Sample)
           (oshi SystemInfo)
           (java.time.format DateTimeFormatter)
           (clojure.lang ExceptionInfo)))

;; definitions
;;
;; benchmark
;; A particular benchmark to run, no parameters, fully concretized as data
;; this represents the 'code' that will run against the SUT
;; what is saved remotely is the arguments to a benchmark function, the function needs to be pure therefore.

;; sut
;; a particular applications, versions, and configuration that we are testing
;; represented as an opaque object, protocol for stopping and sampling time series metrics. Assumed instrumentation.
;; this is an object that is provided on evaluation into a worker/ctx during the bench run
;; e.g an instrumented xt node

;; bench-req
;; a user request for a benchmark to be run, might run locally or across machines in the cloud

;; test
;; a 'baked' bench req, that is all dependencies realised such that the test can be evaluated against the
;; environment
;; it takes evaluation to derive a test from a bench-req (resolving sha's, building jars and projects)
;; the test can be evaluated


;; plan
;; run-for-duration type oltp benchmarks
;; sys under stress
;; vary config and compare
;; few requirements, runs from repl or ci, but can be run on remote infra (e.g ec2/ecs)
;; sut can be a configuration of XT within the current process, or a particular XT git ref, jvm, opts + configuration in some other process (possibly remote!)
;; bench definition is data with hooks into code for procedure/load definitions
;; output/reports are data
;; time series focus with summary, .html visualisation to start with
;; interface to start benchmarks in all cases is the REPL

;; docker is essential if we wanna test kafka/jdbc et al
;; not sure if correct for the JVM under-test.
;; may want option to compare docker rocks say to bare metal or even VM's with fast disks (customers will do this)

;; remote idea: https://docs.docker.com/cloud/ecs-integration/ might be a lot easier than cform and the same thing works on
;; laptop

;; ec2 is probably easiest, need to create a vpc, sg and single instance for many tests could use a cformation template
;; for tear down once test is complete

;; if we do docker via compose first it helps us just have one thing that runs the tests and destroys them



(set! *warn-on-reflection* false)

(defrecord Worker [node random domain-state custom-state clock reports])

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
    {:submit-tx-timer (timer "node.submit-tx")
     :query-timer (timer "node.query")}))

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

        ;; todo hook into dropwizard perhaps
        ;; and delete these?
        submit-counter (AtomicLong.)
        indexed-counter (AtomicLong.)
        indexed-docs-counter (AtomicLong.)
        indexed-bytes-counter (AtomicLong.)
        indexed-av-counter (AtomicLong.)

        _
        (doto meter-reg
          (.gauge "node.tx" ^Iterable [(Tag/of "event" "submitted")] submit-counter)
          (.gauge "node.tx" ^Iterable [(Tag/of "event" "indexed")] indexed-counter)
          (.gauge "node.indexed.docs" indexed-docs-counter)
          (.gauge "node.indexed.bytes" indexed-bytes-counter)
          (.gauge "node.indexed.av" indexed-av-counter))

        {:keys [^Timer submit-tx-timer
                ^Timer query-timer]}
        (install-proxy-node-meters! meter-reg)

        fn-gauge (partial new-fn-gauge meter-reg)

        on-indexed
        (fn [{:keys [submitted-tx, doc-ids, av-count, bytes-indexed] :as event}]
          (reset! last-completed submitted-tx)
          (when (seq doc-ids)
            (.getAndAdd indexed-docs-counter (count doc-ids))
            (.getAndAdd indexed-av-counter (long av-count))
            (.getAndAdd indexed-bytes-counter (long bytes-indexed)))
          (.getAndIncrement indexed-counter)
          nil)

        on-indexed-listener
        (bus/listen (:bus node) {::xt/event-types #{:xtdb.tx/indexed-tx}} on-indexed)

        on-query
        (let [st (atom {})
              start
              (fn [query-id] (swap! st assoc query-id (Timer/start)))
              stop
              (fn [query-id]
                (.stop ^Timer$Sample (@st query-id) query-timer)
                (swap! st dissoc query-id))]
          (fn [{:xtdb.query/keys [query-id]
                ::xt/keys [event-type]}]
            (if (identical? :xtdb.query/submitted-query event-type)
              (start query-id)
              (stop query-id))))

        on-query-events
        #{:xtdb.query/submitted-query
          :xtdb.query/failed-query
          :xtdb.query/completed-query}

        on-query-listener
        (bus/listen (:bus node) {::xt/event-types on-query-events} on-query)

        compute-lag-nanos
        (fn []
          (or
            (when-some [[{::xt/keys [tx-id]} ms] @last-submitted]
              (when-some [{completed-tx-id ::xt/tx-id
                           completed-tx-time ::xt/tx-time} @last-completed]
                (when (< completed-tx-id tx-id)
                  (* (long 1e6) (- ms (inst-ms completed-tx-time))))))
            0))]

    (fn-gauge "node.tx.lag" (comp #(/ % 1e9) compute-lag-nanos) {:unit "seconds"})
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
      (close [_]
        (.close on-query-listener)
        (.close on-indexed-listener)))))

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
           :reports (atom [])
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

(defn get-system-info
  "Returns data about the hardware / OS running this JVM."
  []
  (let [si (SystemInfo.)
        os (.getOperatingSystem si)
        os-version (.getVersionInfo os)
        os-codename (.getCodeName os-version)
        os-version-number (.getVersion os-version)
        arch (System/getProperty "os.arch")
        hardware (.getHardware si)
        cpu (.getProcessor hardware)
        cpu-identifier (.getProcessorIdentifier cpu)
        cpu-name (.getName cpu-identifier)
        cpu-core-count (.getPhysicalProcessorCount cpu)
        cpu-max-freq (.getMaxFreq cpu)
        ram (.getMemory hardware)]
    {:arch arch
     :os (str/join " " (remove str/blank? [(.getFamily os) os-codename os-version-number]))
     :memory (format "%sGB" (/ (long (.getTotal ram)) (* 1024 1024 1024)))
     :cpu (format "%s, %s cores, %.2fGHZ max" cpu-name cpu-core-count (double (/ cpu-max-freq 1e9)))}))

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
                                     :clock clock
                                     :reports (atom [])})]
            (while (not (.isInterrupted thread))
              (try
                (when-some [{:keys [uow-name, ^AtomicLong counter, proc] :as uow} (think worker)]
                  (try
                    (proc worker)
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
      {:system (get-system-info)
       :threads threads
       :seed seed
       :duration @run-time-nanos
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
    (->Worker node (Random.) domain-state custom-state (Clock/systemUTC) (atom []))))

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

;; use vega to plot metrics for now
;; works at repl, no servers needed
;; if this approach takes of the time series data wants to be graphed in something like a shared prometheus / grafana during run
(defn vega-plots [metric-data]
  (vec
    (for [[metric metric-data] (sort-by key (group-by :metric metric-data))]
      {:title metric
       :hconcat (vec (for [[[_statistic unit] metric-data] (sort-by key (group-by (juxt :statistic :unit) metric-data))
                           :let [data {:values (vec (for [{:keys [vs-label, series, samples]} metric-data
                                                          {:keys [time-ms, value]} samples
                                                          :when (Double/isFinite value)]
                                                      {:time (str time-ms)
                                                       :config vs-label
                                                       :series series
                                                       :value value}))
                                       :format {:parse {:time "utc:'%Q'"}}}
                                 any-series (some (comp not-empty :series) metric-data)

                                 series-dimension (and any-series (< 1 (count metric-data)))
                                 vs-dimension (= 2 (bounded-count 2 (keep :vs-label metric-data)))

                                 stack-series series-dimension
                                 stack-vs (and (not stack-series) vs-dimension)
                                 facet-vs (and vs-dimension (not stack-vs))

                                 layer-instead-of-stack
                                 (cond stack-series (str/ends-with? metric "percentile value")
                                       stack-vs true)

                                 mark-type (if stack-vs "line" "area")

                                 spec {:mark {:type mark-type, :line true, :tooltip true}
                                       :encoding {:x {:field "time"
                                                      :type "temporal"
                                                      :title "Time"}
                                                  :y (let [y {:field "value"
                                                              :type "quantitative"
                                                              :title (or unit "Value")}]
                                                       (if layer-instead-of-stack
                                                         (assoc y :stack false)
                                                         y))
                                                  :color
                                                  (cond
                                                    stack-series
                                                    {:field "series",
                                                     :legend {:labelLimit 280}
                                                     :type "nominal"}
                                                    stack-vs
                                                    {:field "config",
                                                     :legend {:labelLimit 280}
                                                     :type "nominal"})}}]]
                       (if facet-vs
                         {:data data
                          :facet {:column {:field "config"}
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

(defn hiccup-report [title report]
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

        [:div
         [:table
          [:thead [:th "config"] [:th "arch"] [:th "os"] [:th "cpu"] [:th "memory"]]
          [:tbody
           (for [{:keys [label, system]} (:systems report)
                 :let [{:keys [arch, os, cpu, memory]} system]]
             [:tr [:th label] [:td arch] [:td os] [:td cpu] [:td memory]])]]]

        [:div
         (for [[group metric-data] (sort-by key (group-metrics report))]
           (list [:h2 group]
                 (for [meter (sort (set (map :meter metric-data)))]
                   [:div {:id (id-from-thing meter)}])))]
        [:script
         (->> (for [[meter metric-data] (group-by :meter (:metrics report))]
                (format "vegaEmbed('#%s', %s);" (id-from-thing meter)
                        (json/write-str
                          {:hconcat (vega-plots metric-data)})))
              (str/join "\n")
              hiccup2/raw)]]])))

(defn show-html-report [rs]
  (let [f (File/createTempFile "xtdb-benchmark-report" ".html")]
    (spit f (hiccup2/html
              {}
              (hiccup-report (:title rs "Benchmark report") rs)))
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
    {:title (str/join " vs " key-seq)
     :systems (for [label key-seq] {:label label, :system (:system report)})
     :metrics (vec (for [[i label] (map-indexed vector key-seq)
                         :let [{:keys [metrics]} (normalize-time (report-map label))]
                         metric metrics]
                     (assoc metric :vs-label (str label))))}))

(def script-example
  {:benchmark `auction-mark/auction-mark
   :duration "PT5M"
   :configurations [{:title "Rocks"
                     :version "master"
                     :jvm :yum
                     :ec2 "t2.medium"
                     :node :rocks}]})

(def benchmark-clj-file-source *file*)
(def benchmark-clj-file-target (.getName (io/file benchmark-clj-file-source)))
(def benchmark-ns (.getName *ns*))

(def default-repository "git@github.com:xtdb/xtdb.git")

(defn- build-io-fns
  "Returns a set of fns for doing IO, running processes and stuff.

  Captures common concerns for bench builds such as error handling, logging, cleanup so the scripts are less of a mess!

  Returns a map of functions:

  :sh - like clojure.java.shell/sh, returns a string out. Takes an optional map first instead of needing to splice options onto the end of the command args.
     e.g (sh {:env {\"SOME_VAR\" \"42\"}} \"my\" \"command\" \"args\")
  :cd - changes the default working directory
  :log - like println
  :home-file returns a file relative to the home dir
  :temp-dir returns a tmp-dir for the given string key, creating it for the first call. "
  []
  (let [working-dir (atom nil)
        cd (fn cd [dir-name] (println "  $ cd" (reset! working-dir (str dir-name))))
        sh (fn sh [opts? & args]
             (let [opts (when (map? opts?) opts?)
                   args (if (map? opts?) args (cons opts? args))
                   {:keys [dir, env]} opts
                   _ (when dir (println "  dir:" dir))
                   _ (doseq [[e] env] (println "  env:" e))
                   command-str (str/join " " args)
                   _ (println "  $" command-str)
                   use-dir (or dir @working-dir)
                   opts (update opts :env merge {"HOME" (System/getenv "HOME")
                                                 "PATH" (System/getenv "PATH")})
                   opts-to-sh (if use-dir (assoc opts :dir use-dir) opts)
                   {:keys [exit, err, out]} (apply sh/sh (apply concat args opts-to-sh))]
               (when-not (= 0 exit)
                 (throw (ex-info (or (not-empty out) "Command failed")
                                 {:command command-str
                                  :exit exit
                                  :out out
                                  :err err})))
               out))
        aws (fn [& args]
              (apply sh "aws" (concat args
                                      ["--output" "json"
                                       ;; todo require explicit setup
                                       "--region" "eu-west-1"
                                       "--profile" "juxt"])))
        log println]
    {:cd cd
     :log log
     :sh sh
     :aws aws
     :home-file (partial io/file (System/getenv "HOME"))
     :temp-dir (memoize (partial xio/create-tmpdir))}))

(defn resolve-sha [repository ref-spec]
  (first (str/split ((:sh (build-io-fns)) "git" "ls-remote" repository ref-spec) #"\t")))

(defn build-xtdb-artifacts-if-needed [repository ref-spec]
  (let [{:keys [cd, log, sh, home-file, temp-dir]} (build-io-fns)
        xt-sha (resolve-sha repository ref-spec)
        xt-version-default (str "bench-" xt-sha)
        xt-core-m2-location-default (home-file ".m2" "repository" "com" "xtdb" "xtdb-core" xt-version-default)
        xt-core-m2-location-if-ref-spec-a-release (home-file ".m2" "repository" "com" "xtdb" "xtdb-core" ref-spec)

        xt-core-m2-location
        (if (.exists xt-core-m2-location-if-ref-spec-a-release)
          xt-core-m2-location-if-ref-spec-a-release
          xt-core-m2-location-default)

        xt-version (.getName xt-core-m2-location)
        xt-core-m2-exists-already (.exists xt-core-m2-location)]

    (when xt-core-m2-exists-already
      (log "Using ~/.m2 xtdb artifacts for" xt-version))

    (when-not xt-core-m2-exists-already
      (let [tmp-dir (temp-dir "benchmark-xtdb")]
        (log "Could not find xtdb artifacts, building from source")
        (cd tmp-dir)
        (log "Fetching xtdb ref" ref-spec "from" repository)
        (sh "git" "init")
        (sh "git" "remote" "add" "origin" repository)
        (sh "git" "fetch" "origin" "--depth" "1" xt-sha)
        (sh "git" "checkout" xt-sha)
        (log "Installing jars for build")
        (sh
          {:env {"XTDB_VERSION" xt-version}}
          "sh" "lein-sub" "install")
        (.delete tmp-dir)))

    {:xt-version xt-version
     :git-repository repository
     :git-ref ref-spec
     :git-sha xt-sha}))

;; delete once happy
(defonce tmp-artifacts (build-xtdb-artifacts-if-needed default-repository "1.21.0"))

(defn module-deps [sut]
  (let [{:keys [index, doc, log]} sut
        deps {:rocks [['com.xtdb/xtdb-rocksdb]]
              :lmdb [['com.xtdb/xtdb-lmdb]]
              :jdbc [['com.xtdb/xtdb-jdbc]]
              :kafka [['com.xtdb/xtdb-kafka]]}]
    (mapcat deps [index doc log])))

(def bench-main-sym (symbol (str "bench.main")))
(def bench-main-file (str "bench/main.clj"))
(def benchmark-main-sym (symbol (str benchmark-ns "/-main")))

(defn -main [& args]

  ;; load benchmark options from 'place' (maybe url/s3) e.g s3://?

  ;; run benchmark for max duration

  ;; when done write output to 'place'

  ;; unhappy paths:

  ;; deps not installed or wrong (e.g java)
  ;; bad java opts
  ;; bad benchmark config
  ;; benchmark definition missing or not as expected
  ;; crash during setup
  ;; crash during bench
  ;; takes too long
  ;; segfault/jvm exit

  ;; during run reporting options
  ;; log (cldwatch)
  ;; wrapping shell script (to watch for jvm dying)
  ;; write to s3
  ;; slack
  ;; live metrics to cloudwatch, graphite or grafana - as we go?

  ;; finish reporting
  ;; report file written to directory (with fat samples)

  ;; reporting structure
  ;; s3://$$$/xtdb-benchmarks/YYYY-MM-DD/HH-MM-SS-MS/definition.edn

  ;; relative s3 paths for each sut/run (ordinals)
  ;; /config/0/sut.jar
  ;; /config/0/result.edn

  ;; merged results
  ;; /results.edn

  ;;

  )

(def bench-main-content
  (->> [(pr-str (list 'ns bench-main-sym))
        ""
        (pr-str
          (list 'defn '-main '[& args]
                (list 'requiring-resolve (list 'quote benchmark-main-sym))))]
       (str/join "\n")))

(defn lein-project
  [{:keys [xt-version, index, log, docs]}]
  (let [project-name (str/join "-" (map name [xt-version "bench" index docs log]))
        module-deps {:rocks [['com.xtdb/xtdb-rocksdb]]
                     :lmdb [['com.xtdb/xtdb-lmdb]]
                     :jdbc [['com.xtdb/xtdb-jdbc]]
                     :kafka [['com.xtdb/xtdb-kafka]]}]
    {:project-name project-name
     :project
     (list 'defproject (symbol project-name) "0"
           :aot ['bench.main]
           :dependencies
           (vec (concat
                  '[[org.clojure/clojure "1.11.1"]]

                  ;; re-use bench project or externalise
                  ;; a dep for bench deps themselves
                  ;; to avoid writing these in two places
                  '[[org.clojure/tools.logging "1.2.4"]
                    [org.clojure/data.json "2.4.0"]
                    [ch.qos.logback/logback-classic "1.2.11"]
                    [ch.qos.logback/logback-core "1.2.11"]
                    [io.micrometer/micrometer-core "1.9.5"]
                    [com.github.oshi/oshi-core "6.3.0"]
                    [pro.juxt.clojars-mirrors.hiccup/hiccup "2.0.0-alpha2"]
                    [com.google.guava/guava "30.1.1-jre"]]

                  ;; core xtdb dependencies
                  [['com.xtdb/xtdb-core xt-version]]

                  ;; module xtdb dependencies
                  (for [[nm ver :as dep] (mapcat module-deps [index log docs])]
                    (if ver dep [nm xt-version])))))}))

(defn run-script [script]
  (let [{:keys [benchmark
                duration
                configurations]} script
        bmark (requiring-resolve benchmark)
        duration (Duration/parse duration)
        run-id (str (System/currentTimeMillis) "-" (LocalDate/now))]

    ))

(def aws-resource-qualifier "auctionmark")
(def aws-s3-bucket "xtdb-bench")
(defn aws-s3-path [& parts] (str/join "/" parts))

(defn ec2-stack-cli-input [{:keys [benchmark-id, sut-id]}]
  {:pre [benchmark-id, sut-id]}
  (let [stack-name (str/join "-" [aws-resource-qualifier benchmark-id sut-id])]
    {"StackName" stack-name
     "TemplateBody" (slurp (io/resource "auction_mark_cf.yml"))
     "Parameters" (vec (for [[k, v] {"BenchmarkId" benchmark-id
                                     "SutId" sut-id
                                     "JarUrl" "not-yet"}]
                         {"ParameterKey" k
                          "ParameterValue" v}))
     "OnFailure" "DELETE"
     "Tags" (vec (for [[k v] {"BenchmarkId" benchmark-id
                              "SutId" sut-id}]
                   {"Key" k
                    "Value" v}))}))

(defn ec2-stack-create [stack]
  (let [{:keys [aws]} (build-io-fns)]
    (aws "cloudformation" "create-stack" "--cli-input-json" (json/write-str (ec2-stack-cli-input stack)))))

(defn ec2-stack-ls []
  (let [{:keys [aws]} (build-io-fns)]
    (aws "cloudformation" "list-stacks"
         "--stack-status-filter" "CREATE_COMPLETE"
         "--query" (format "StackSummaries[?starts_with(StackName, `%s`)]" aws-resource-qualifier))))

(defn ec2-stack-delete [stack-name]
  (let [{:keys [aws]} (build-io-fns)]
    (aws "cloudformation" "delete-stack" "--stack-name" stack-name)))

(def example-ec2-run-def
  {:benchmark :auctionmark
   :duration "PT5M"
   :vs [{:name "RocksDB"
         :repo default-repository
         :version "1.22.0"
         :index :rocks
         :docs :rocks
         :log :rocks
         :jre 17
         :instance "m1.small"}
        {:name "LMDB"
         :repo default-repository
         :version "1.22.0"
         :index :lmdb
         :docs :lmdb
         :log :lmdb
         :jre 17
         :instance "m1.small"}]})

(def ec2-amazon-linux-2-info
  {:ami "ami-0ee415e1b8b71305f"
   ;; package names of different jre's
   :jre-packages {17 "java-17-amazon-corretto-headless"}})

(defn cfn-template [{:keys [ami, jre-package, instance]}]
  {"AWSTemplateFormatVersion" "2010-09-09"
   "Description" "auction mark test"
   "Resources" {"Runner" {"Type" "AWS::EC2::Instance"}
                "Properties" {"ImageId" ami
                              "InstanceType" instance}}})


(defn ec2-stack-create-cli-input [{:keys [benchmark-id, sut-id] :as sut}]
  {:pre [benchmark-id, sut-id]}
  (let [stack-name (str/join "-" [aws-resource-qualifier benchmark-id sut-id])]
    {"StackName" stack-name
     "TemplateBody" (json/write-str (cfn-template sut))
     "Parameters" []
     "OnFailure" "DELETE"
     "Tags" (vec (for [[k v] {"BenchmarkId" benchmark-id
                              "SutId" sut-id}]
                   {"Key" k "Value" v}))}))

(defn ec2-run-manifest [{:keys [benchmark, duration, vs] :as benchmark-def}]
  (let [now (Instant/now)
        {:keys [log]} (build-io-fns)

        ;; london time prefixing for humies
        now-ldt (LocalDateTime/ofInstant now (ZoneId/of "Europe/London"))
        now-unix-ms (.toEpochMilli now)

        s3-ldt-pattern "YYYY/MM/dd"
        s3-ldt-path (.format now-ldt (DateTimeFormatter/ofPattern s3-ldt-pattern))
        s3-path (partial aws-s3-path (name benchmark) s3-ldt-path now-unix-ms)

        benchmark-id (aws-s3-path (str "s3://" aws-s3-bucket) (s3-path))
        ami "ami-0ee415e1b8b71305f"
        jre-packages {17 "java-17-amazon-corretto-headless"}

        _ (log "Manifest for" benchmark-id)
        _ (log "XT Versions")
        code-locs (set (map (juxt :repo :version) vs))
        artifacts
        (->> code-locs
             (map (fn [[repo version]]
                    (build-xtdb-artifacts-if-needed
                      (or repo default-repository)
                      (or version "master"))))
             (zipmap code-locs))
        runs (for [[i {:keys [name, repo, version, index, docs, log, jre, instance] :as sut
                       :or {name ""
                            index :rocks
                            log :rocks
                            docs :rocks
                            jre 17
                            instance "m1.small"}}]
                   (map-indexed vector vs)
                   :let [s3-path-sut (partial s3-path (str "sut" i))
                         sut-id (str benchmark-id "/" "sut" i)]]
               {:benchmark-id benchmark-id
                :sut-id sut-id
                ;; will contain the build map, sha, uberjar loc etc
                :s3-build-key (s3-path-sut "build.edn")
                ;; will contain the status map (updated by the ec2 node!)
                :s3-run-key (s3-path-sut "status.edn")
                ;; will contain the eventual run report
                :s3-report-key (s3-path-sut "report.edn")
                :duration duration
                :instance instance
                :label name
                :ami ami
                :packages [(jre-packages jre)]
                :repo repo
                :version version
                :xtdb-artifacts (artifacts [repo version])
                :index index
                :docs docs
                :logs log})]

    {:benchmark-id benchmark-id
     :benchmark-def benchmark-def
     :s3-bucket aws-s3-bucket
     ;; will contain the input map
     :s3-def-key (s3-path "def.edn")
     ;; will contain this map
     :s3-manifest-key (s3-path "manifest.edn")
     ;; run data for each system-under-test
     :runs (vec runs)}))

(defn ec2-run-setup [manifest]
  (let [{:keys [benchmark-id,
                benchmark-def,
                runs,
                s3-def-key
                s3-manifest-key]} manifest
        {:keys [aws, log]} (build-io-fns)

        s3-key-used
        #(try
           (aws "s3api" "head-object" "--bucket" aws-s3-bucket "--key" %)
           false
           (catch ExceptionInfo ex (str/starts-with? (:err (ex-data ex) "") "An error occurred (404)")))

        s3-key-not-used (complement s3-key-used)
        ]

    (log "Setup" benchmark-id)

    (doseq [{:keys [sut-id, repo, version, xtdb-artifacts] :as sut} runs]
      (log "Building" sut-id)
      (let [lein-proj (lein-project {:xtdb-artifacts xtdb-artifacts, :sut sut})
            ]
        ;; todo build lein proj into uberjar?

        )
      (log "Ok"))



    (log "Testing that existing benchmark will not be clobbered")
    (assert (s3-key-not-used s3-def-key))
    (assert (s3-key-not-used s3-manifest-key))
    (log "Ok")

    ))

#_{:loaders
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
    [:job #'index-item-status-groups {:freq {:ratio 0.2}}]]}

(defn generate [worker f n]
  (let [doc-seq (repeatedly n (partial f worker))
        partition-count 512]
    (doseq [chunk (partition-all partition-count doc-seq)]
      (xt/submit-tx (:node worker) (mapv (partial vector ::xt/put) chunk)))))

(defn compile-benchmark [benchmark middleware]
  (let [run-middleware (fn [task f] (middleware {:task task, :f f}))

        lift-f (fn [f] (if (vector? f) #(apply (first f) % (rest f)) f))

        await-threads
        (fn [threads ^Duration wait]

          (when-some [t (first threads)]
            (.join t (.toMillis wait)))

          ;; warn/alert?
          (doseq [^Thread thread (filter #(.isAlive ^Thread %) threads)]
            (.interrupt thread)))

        compile-task
        (fn compile-task [{:keys [t] :as task}]
          (run-middleware task
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
                                        (f)
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
                                  (let [[f] (sample-fn worker)]
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
                                  (f)
                                  (when (< (.millis (:clock worker)) wait-until)
                                    (sleep)
                                    (recur wait-until))))))))

        seed (:seed benchmark 0)
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

(defn install-tx-fns [worker fns]
  (->> (for [[id fn-def] fns]
         [::xt/put {:xt/id id, :xt/fn fn-def}])
       (xt/submit-tx (:node worker))))

(defn add-report [worker report]
  (swap! (:reports worker) conj report))

(def ^:dynamic ^:private *meter-reg* nil)

(defn wrap-metrics []
  (let [instrument-f
        (fn [k f]
          (let [timer-delay
                (delay
                  (fn []
                    (when *meter-reg*
                      (-> (Timer/builder (str "auctionmark." (name k)))
                          (.publishPercentiles (double-array percentiles))
                          (.maximumExpectedValue (Duration/ofHours 8))
                          (.minimumExpectedValue (Duration/ofNanos 1))
                          (.register *meter-reg*)))))]
            (fn instrumented-transaction [worker]
              (if-some [^Timer timer @timer-delay]
                (.recordCallable timer ^Callable (fn [] (f worker)))
                (f worker)))))

        instrument-stage
        (fn [k f]
          (fn instrumented-stage [worker]
            (let [reg (meter-reg)
                  sampler (meter-sampler reg)
                  executor (Executors/newSingleThreadScheduledExecutor)
                  sample-freq 1000]
              (.scheduleAtFixedRate
                executor
                ^Runnable sampler
                0
                sample-freq
                TimeUnit/MILLISECONDS)
              (try
                (f worker)
                (add-report worker {:stage k, :metrics (sampler :summarize)})
                (finally
                  (.shutdownNow executor))))))]
    (fn [{:keys [task, f]}]
      (let [{:keys [stage, transaction]} task]
        {:task task
         :f (cond
              stage (instrument-stage stage f)
              transaction (instrument-f transaction f)
              :else f)}))))

(defn auctionmark2 [{:keys [seed,
                            threads,
                            duration]
                     :or {seed 0,
                          threads 8,
                          duration "PT30S"}}]
  (let [duration (Duration/parse duration)]
    {:title "Auction Mark OLTP"
     :seed seed
     :tasks
     [{:t :do
       :stage :load
       :tasks [{:t :call :f [install-tx-fns {:apply-seller-fee tx-fn-apply-seller-fee, :new-bid tx-fn-new-bid}]}
               {:t :call :f load-categories-tsv}
               {:t :call :f [generate generate-region 75]}
               {:t :call :f [generate generate-category 16908]}]}
      {:t :concurrently
       :stage :oltp
       :duration duration
       :join-wait (Duration/ofSeconds 5)
       :thread-tasks [{:t :pool
                       :duration duration
                       :join-wait (Duration/ofSeconds 5)
                       :thread-count threads
                       :think Duration/ZERO
                       :pooled-task {:t :pick-weighted
                                     :choices [[{:t :call, :transaction :get-item :f proc-get-item} 12.0]
                                               [{:t :call, :transaction :new-user :f proc-new-user} 0.5]
                                               [{:t :call, :transaction :new-item :f proc-new-item} 1.0]
                                               [{:t :call, :transaction :new-bid :f proc-new-bid} 2.0]]}}
                      {:t :freq-job
                       :duration duration
                       :freq (Duration/ofMillis (* 0.2 (.toMillis duration)))
                       :job-task {:t :call, :ancillary :index-item-status-groups, :f index-item-status-groups}}]}]}))

(defn quick-run [benchmark node-opts]
  (let [f (compile-benchmark benchmark (wrap-metrics))]
    (with-open [sut (xt/start-node node-opts)]
      (f sut))))

(def bench-req-example
  {:title "Rocks"
   :t :auctionmark,
   :arg {:duration "PT30M", :thread-count 8}
   :env {:t :ec2, :instance "m1.small"}
   :sut {:t :xtdb,
         :version "1.22.0"
         :index :rocks
         :log :rocks
         :docs :rocks}})

(defn timestamp-path [epoch-ms]
  (let [ldt (LocalDateTime/ofInstant (Instant/ofEpochMilli epoch-ms) (ZoneId/of "Europe/London"))]
    (.format ldt (DateTimeFormatter/ofPattern "YYYY/MM/dd/HH-mm-ss-SS"))))

(defn bench-path [epoch-ms, filename]
  ;; unencoded filename but who cares right now
  (str "b2/" (timestamp-path epoch-ms) "/" filename))

(defn bench-loc-fn [env epoch-ms]
  (case (:t env)
    :local
    (fn local-path [filename]
      {:t :file,
       ;; todo tmp-dir prop
       :file (.getAbsolutePath (io/file "/tmp" (bench-path epoch-ms filename)))})
    :ec2
    (fn ec2-path [filename]
      {:t :s3,
       :bucket "xtdb-bench"
       :key (bench-path epoch-ms filename)})))

(defn resolve-env [env sut manifest-loc]
  (case (:t env)
    :local env
    :ec2
    (let [{:keys [instance,
                  ami]
           :or {instance "m1.small"
                ami "ami-0ee415e1b8b71305f"}} env
          {:keys [jre, jar]} sut

          jre-package
          (case [(:t jre) (:version jre)]
            [:corretto 17] "java-17-amazon-corretto-headless")

          jar-path (case (:t jar) :s3 (str "s3://" (:bucket jar) "/" (:key jar)))]
      (merge
        env
        {:t :ec2
         :instance instance
         :ami ami
         :packages [jre-package, "awscli"]
         :script [["aws" "cp" jar-path "sut.jar"]
                  ["java" "-jar" "sut.jar" manifest-loc]]}))))

(defn resolve-sut [sut loc-fn]
  (case (:t sut)
    :xtdb
    (let [{:keys [repository
                  version
                  index
                  docs
                  log
                  jre]
           :or {repository "git@github.com:xtdb/xtdb.git"
                version "master"
                index :rocks
                docs :rocks
                log :rocks
                jre {:t :corretto, :version 17}}} sut
          sha (resolve-sha repository version)]
      (merge
        sut
        {:repository repository
         :version version
         :index index
         :docs docs
         :log log
         :sha sha
         :jre jre
         :jar (loc-fn "sut.jar")}))))

(defn resolve-req
  [req]
  (let [{:keys [env, sut]} req
        epoch-ms (System/currentTimeMillis)
        loc-fn (bench-loc-fn env epoch-ms)
        resolved-sut (resolve-sut sut loc-fn)
        manifest-loc (loc-fn "manifest.edn")
        resolved-env (resolve-env env resolved-sut manifest-loc)]
    (merge
      req
      {:epoch-ms epoch-ms
       :manifest manifest-loc
       :report (loc-fn "report.edn")
       :status (loc-fn "status.edn")
       :env resolved-env
       :sut resolved-sut})))

(defn build-sut-artifact-if-needed [{:keys [repository, sha, index, docs, log]}]
  (let [{:keys [xt-version]} (build-xtdb-artifacts-if-needed repository sha)
        {:keys [project-name, project]} (lein-project {:xt-version xt-version, :index index, :docs docs, :log log})
        project-dir (io/file "/tmp" "b2" "projects" project-name)
        {:keys [cd, sh]} (build-io-fns)]
    (cd project-dir)
    (spit (io/file project-dir "project.clj") (pr-str project))
    ;; todo cp self-src and main shim
    (sh "lein uberjar")
    (io/file project-dir "target" (str project-name "-standalone.jar"))))

(defn fufil-sut-requirements [resolved-sut]
  (let [{:keys [t, repository, sha, index, docs, log]} resolved-sut
        _ (assert (= :xtdb t))
        {:keys [xt-version]} (build-xtdb-artifacts-if-needed repository sha)
        {:keys [project-name, project]} (lein-project (assoc resolved-sut :xt-version xt-version))]

    ))

(defn fulfil-manifest [manifest]
  (let [{:keys [log]} (build-io-fns)
        loc-str (fn [loc]
                  (case (:t loc)
                    :s3 (str "s3://" (:bucket loc) "/" (:key loc))
                    :file (str (:file loc))))
        {self-loc :manifest
         :keys [env, sut]} manifest]

    ;; only core1 for now
    (assert (= :xtdb (:t sut)))

    (log "Fulfiling manifest" (loc-str) self-loc)
    (log "Building artifacts")
    (let [{:keys [xt-version]} (build-xtdb-artifacts-if-needed (:repository sut) (:sha sut))
          lein-prj (lein-project
                     {:xt-version xt-version,
                      :index (:index sut)
                      :docs (:docs sut)
                      :log (:log sut)})]

      )

    (log "Uploading artifacts")

    ))

(def bench-manifest-example
  "A bench request is turned into a manifest, in which any ambiguous references (such as tags, versions, packages)
  are resolved into something less ambiguous. All paths and strings that need to be known are hopefully
  captured at some point in time"
  {:title "Rocks"
   :t :auctionmark
   :epoch-ms 166661643559

   :manifest {:t :s3, :bucket "xtdb-bench", :key "b2/2022/10/24/13-54-03-433/manifest.edn"}
   :report {:t :s3, :bucket "xtdb-bench", :key "b2/2022/10/24/13-54-03-433/report.edn"}
   :status {:t :s3, :bucket "xtdb-bench", :key "b2/2022/10/24/13-54-03-433/status.edn"}

   :arg {:duration "PT30M", :thread-count 8}

   :env
   {:t :ec2,
    :instance "m1.small",
    :ami "ami-0ee415e1b8b71305f"
    :packages ["java-17-amazon-corretto-headless", "awscli"]
    :script ["aws cp s3://xtdb-bench/b2/2022/10/24/13-54-03-433/sut.jar sut.jar" "java -jar sut.jar"]}

   :sut
   {:t :xtdb,
    :version "1.22.0",
    :sha "3acfe756058a70dd4186c38430a36f5035a9d21c"
    :jar {:t :s3, :bucket "xtdb-bench", :key "b2/2022/10/24/13-54-03-433/sut.jar"}
    :index :rocks,
    :log :rocks,
    :docs :rocks
    :jre {:t :corretto, :version 17}}})
