(ns xtdb.bench2.auctionmark
  (:require [xtdb.bench2 :as b2]
            [xtdb.api :as xt]
            [xtdb.bench2.core1 :as bcore1]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.time Instant Duration)
           (java.util ArrayList)
           (java.util.concurrent ConcurrentHashMap)))

(defn random-price [worker] (.nextDouble (b2/rng worker)))

(def user-id (partial str "u_"))
(def region-id (partial str "r_"))
(def item-id (partial str "i_"))
(def item-bid-id (partial str "ib_"))
(def category-id (partial str "c_"))
(def global-attribute-group-id (partial str "gag_"))
(def global-attribute-value-id (partial str "gav_"))

(def user-attribute (b2/id))
(def item-name (b2/id))
(def item-description (b2/id))
(def initial-price (b2/id))
(def reserve-price (b2/id))
(def buy-now (b2/id))
(def item-attributes-blob (b2/id))
(def item-image-path (b2/id))
(def auction-start-date (b2/id))

(defn proc-new-user
  "Creates a new USER record. The rating and balance are both set to zero.

  The benchmark randomly selects id from a pool of region ids as an input for u_r_id parameter using flat distribution."
  [worker]
  (let [u_id (b2/increment worker user-id)]
    (->> [[::xt/put {:xt/id u_id
                     :u_id u_id
                     :u_r_id (b2/sample-flat worker region-id)
                     :u_rating 0
                     :u_balance 0.0
                     :u_created (b2/current-timestamp worker)
                     :u_sattr0 (b2/random-str worker)
                     :u_sattr1 (b2/random-str worker)
                     :u_sattr2 (b2/random-str worker)
                     :u_sattr3 (b2/random-str worker)
                     :u_sattr4 (b2/random-str worker)
                     :u_sattr5 (b2/random-str worker)
                     :u_sattr6 (b2/random-str worker)
                     :u_sattr7 (b2/random-str worker)}]]
         (xt/submit-tx (:sut worker)))))

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
    (weighting (b2/rng worker))
    (b2/sample-gaussian worker category-id)))

(defn proc-new-item
  "Insert a new ITEM record for a user.

  The benchmark client provides all the preliminary information required for the new item, as well as optional information to create derivative image and attribute records.
  After inserting the new ITEM record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and ITEM IMAGE.

  After these records are inserted, the transaction then updates the USER record to add the listing fee to the seller’s balance.

  The benchmark randomly selects id from a pool of users as an input for u_id parameter using Gaussian distribution. A c_id parameter is randomly selected using a flat histogram from the real auction site’s item category statistic."
  [worker]
  (let [i_id-raw (.getAndIncrement (b2/counter worker item-id))
        i_id (item-id i_id-raw)
        u_id (b2/sample-gaussian worker user-id)
        c_id (sample-category-id worker)
        name (b2/random-str worker)
        description (b2/random-str worker)
        initial-price (random-price worker)
        attributes (b2/random-str worker)
        gag-ids (remove nil? (b2/random-seq worker {:min 0, :max 16, :unique true} b2/sample-flat global-attribute-group-id))
        gav-ids (remove nil? (b2/random-seq worker {:min 0, :max 16, :unique true} b2/sample-flat global-attribute-value-id))
        images (b2/random-seq worker {:min 0, :max 16, :unique true} b2/random-str)
        start-date (b2/current-timestamp worker)
        ;; up to 42 days
        end-date (.plusSeconds ^Instant start-date (* 60 60 24 (* (inc (.nextInt (b2/rng worker) 42)))))
        ;; append attribute names to desc
        description-with-attributes
        (let [q '[:find ?gag-name ?gav-name
                  :in [?gag-id ...] [?gav-id ...]
                  :where
                  [?gav-gag-id :gav_gag_id ?gag-id]
                  [?gag-id :gag_name ?gag-name]
                  [?gav-id :gav_name ?gav-name]]]
          (->> (xt/q (xt/db (:sut worker)) q gag-ids gav-ids)
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
         (xt/submit-tx (:sut worker)))))

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
  (let [{:keys [sut, ^ConcurrentHashMap custom-state]} worker
        now (b2/current-timestamp worker)]
    (with-open [db (xt/open-db sut)]
      (.putAll custom-state {:item-status-groups (item-status-groups db now)}))))

(defn random-item [worker & {:keys [status] :or {status :all}}]
  (let [isg (-> worker :custom-state :item-status-groups (get status) vec)
        item (b2/random-nth worker isg)]
    item))

(defn- generate-new-bid-params [worker]
  (let [{:keys [i_id, i_u_id]} (random-item worker :status :open)
        i_buyer_id (b2/sample-gaussian worker user-id)]
    (if (and i_buyer_id (= i_buyer_id i_u_id))
      (generate-new-bid-params worker)
      {:i_id i_id,
       :i_u_id i_u_id,
       :ib_buyer_id i_buyer_id
       :bid (random-price worker)
       :max-bid (random-price worker)
       :new-bid-id (b2/increment worker item-bid-id)
       :now (b2/current-timestamp worker)})))

(defn proc-new-bid [worker]
  (let [params (generate-new-bid-params worker)]
    (when (and (:i_id params) (:i_u_id params))
      (xt/submit-tx (:sut worker) [[::xt/fn :new-bid params]]))))

(defn proc-get-item [worker]
  (let [{:keys [sut]} worker
        ;; the benchbase project uses a profile that keeps item pairs around
        ;; selects only closed items for a particular user profile (they are sampled together)
        ;; right now this is a totally random sample with one less join than we need.
        i_id (b2/sample-flat worker item-id)
        q '[:find (pull ?i [:i_id, :i_u_id, :i_initial_price, :i_current_price])
            :in [?iid]
            :where
            [?i :i_id ?iid]
            [?i :i_status 0]]]
    (xt/q (xt/db sut) q i_id)))

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
                           ::category-weighting (b2/weighted-sample-fn (map (juxt :id :item-count) (vals cats)))})))

(defn generate-region [worker]
  (let [r-id (b2/increment worker category-id)]
    {:xt/id r-id
     :r_id r-id
     :r_name (b2/random-str worker 6 32)}))

(defn generate-category [worker]
  (let [{::keys [categories]} (:custom-state worker)
        c-id (b2/increment worker category-id)
        {:keys [category-name, parent]} (categories c-id)]
    {:xt/id c-id
     :c_id c-id
     :c_parent_id (when (seq parent) (:id (categories parent)))
     :c_name (or category-name (b2/random-str worker 6 32))}))

(defn benchmark [{:keys [seed,
                         threads,
                         duration
                         sync]
                  :or {seed 0,
                       threads 8,
                       duration "PT30S"
                       sync false}}]
  (let [duration (Duration/parse duration)]
    {:title "Auction Mark OLTP"
     :seed seed
     :tasks
     [{:t :do
       :stage :load
       :tasks [{:t :call :f [bcore1/install-tx-fns {:apply-seller-fee tx-fn-apply-seller-fee, :new-bid tx-fn-new-bid}]}
               {:t :call :f load-categories-tsv}
               {:t :call :f [bcore1/generate generate-region 75]}
               {:t :call :f [bcore1/generate generate-category 16908]}]}
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
                       :job-task {:t :call, :transaction :index-item-status-groups, :f index-item-status-groups}}]}
      (when sync {:t :call, :f #(xt/sync (:sut %))})]}))
