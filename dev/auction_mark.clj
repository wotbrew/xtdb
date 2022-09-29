(ns auction-mark
  (:require [xtdb.api :as xt]
            [clojure.string :as str])
  (:import (java.time Instant Clock)
           (java.util Random)
           (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function)))

(defrecord Worker [node rng domain-state clock])

(defn current-timestamp ^Instant [worker]
  (.instant ^Clock (:clock worker)))

(defn counter ^AtomicLong [worker domain]
  (.computeIfAbsent ^ConcurrentHashMap (:domain-state worker) domain (reify Function (apply [_ _] (AtomicLong.)))))

(defn rng ^Random [worker] (:rng worker))

(defn worker [node] (->Worker node (Random.) (ConcurrentHashMap.) (Clock/systemUTC)))

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

(defn sample-histogram
  "Selects a value from the domain using the given pre-built weighting histogram."
  [worker domain histogram]
  ;; todo use hist
  (sample-gaussian worker domain))

(defn random-seq [worker opts f & args]
  (let [{:keys [min, max, unique]} opts]
    (->> (repeatedly #(apply f worker args))
         (take (+ min (.nextInt (rng worker) (- max min))))
         ((if unique distinct identity)))))

(defn random-str [worker] (str (.nextInt (rng worker))))

(defn random-price [worker] 3.14)

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

(def category-id-histogram nil)

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
        c_id (sample-histogram worker category-id category-id-histogram)
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

(defn setup [node]
  (xt/submit-tx node [[::xt/put {:xt/id :apply-seller-fee
                                 :xt/fn tx-fn-apply-seller-fee}]]))

(def auction-mark
  "A model of the auction-mark benchmark
  
  https://hstore.cs.brown.edu/projects/auctionmark/"
  {;; do not require the qualification of attributes
   :qualify false

   :types
   {:u-attribute [:alias [:string {:min-length 5, :max-length 32}]]}

   :transactions
   {:new-user

    {:frequency 0.05
     :params
     {:u_id [:next-id :u_id]
      :u_r_id [:pick-gaussian :r_id]
      :attributes [:gen [:vector [:u_attribute] {:min-length 8, :max-length 8}]]
      :now [:now :local-date-time]}
     :f
     (fn new-user [node {:keys [u_id, u_r_id, attributes, now]}]
       (xt/submit-tx node [::xt/put {:xt/id (random-uuid)
                                     :u_id u_id
                                     :u_r_id u_r_id
                                     :u_rating 0
                                     :u_balance 0.0
                                     :u_created now
                                     :u_sattr0 (nth attributes 0)
                                     :u_sattr1 (nth attributes 1)
                                     :u_sattr2 (nth attributes 2)
                                     :u_sattr3 (nth attributes 3)
                                     :u_sattr4 (nth attributes 4)
                                     :u_sattr5 (nth attributes 5)
                                     :u_sattr6 (nth attributes 6)
                                     :u_sattr7 (nth attributes 7)}]))}

    :new-item
    {:frequency 0.05
     :params
     {:i_id [:next-id :i_id]
      :u_id [:pick-gaussian :u_id]
      ;; from category .txt file
      :c_id [:pick-weighted :cat_weights]
      :name [:gen [:string]]
      :description [:gen [:string]]
      :initial_price [:gen [:double]]
      :reserve_price [:gen [:nullable [:double]]]
      :buy_now [:gen [:nullable [:double]]]
      :attributes [:gen [:string]]
      }

     }}

   :attrs
   {:c_id [:long {:id-seq :counter}],
    :c_name [:string],
    :c_parent_id [:ref :c_id {:no-cycle true}],

    :gag_c_id [:ref :c_id],
    :gag_id [:long {:id-seq :counter}],
    :gag_name [:string],

    :gav_gag_id [:gag_id],
    :gav_id [:long {:id-seq :counter}],

    :i_c_id [:c_id],
    :i_current_price [:double],
    :i_description [:string {:min-length 50, :max-length 255}],
    :i_end_date [:local-date-time],
    :i_id [:long {:id-seq :counter, :max-bits 60}],
    :i_initial_price [:double],
    :i_name [:string {:min-length 6, :max-length 32}],
    :i_num_bids [:long],
    :i_num_global_attrs [:long],
    :i_num_images [:long],
    :i_start_date [:local-date-time],
    :i_status [:int {:elements [0 1 2]}],
    :i_u_id [:u_id],
    :i_user_attributes [:string {:min-length 20, :max-length 255}],

    :r_id [:long {:id-seq :counter}],
    :r_name [:string],

    :u_balance [:double],
    :u_created [:local-date-time],
    :u_id [:long {:id-seq :counter}],
    :u_r_id [:r_id],
    :u_rating [:long {:min 0, :max 6}],

    :u_sattr0 [:string {:min-length 16, :max-length 64}],
    :u_sattr1 [:string {:min-length 16, :max-length 64}],
    :u_sattr2 [:string {:min-length 16, :max-length 64}],
    :u_sattr3 [:string {:min-length 16, :max-length 64}],
    :u_sattr4 [:string {:min-length 16, :max-length 64}],
    :u_sattr5 [:string {:min-length 16, :max-length 64}],
    :u_sattr6 [:string {:min-length 16, :max-length 64}],
    :u_sattr7 [:string {:min-length 16, :max-length 64}],

    :ua_created [:local-date-time],
    :ua_id [:long {:id-seq :counter}],
    :ua_name [:string {:min-length 5, :max-length 32}],
    :ua_u_id [:u_id],
    :ua_value [:string {:min-length 5, :max-length 32}]}

   :entities
   {:region
    {:size 62, :req [:r_id :r_name]},

    :global_attribute_group
    {:size 100, :req [:gag_id :gag_c_id :gag_name]},

    :global_attribute_value
    {:size [:* :global_attribute_group 10], :req [:gav_id :gav_gag_id]},

    :category
    {:size 19459,
     :req [:c_id :c_name :c_parent_id]},

    :user
    {:size 1000000
     :req
     [:u_sattr6
      :u_r_id
      :u_id
      :u_sattr5
      :u_sattr3
      :u_sattr4
      :u_sattr7
      :u_created
      :u_sattr1
      :u_balance
      :u_rating
      :u_sattr0
      :u_sattr2]},

    :user_attributes
    {:size [:* :user 1.3],
     :req [:ua_id :ua_u_id :ua_name :ua_value :ua_created]},

    :item
    {:size [:* :user 10],
     :attrs
     [:i_start_date
      :i_current_price
      :i_num_images
      :i_initial_price
      :i_user_attributes
      :i_description
      :i_num_bids
      :i_c_id
      :i_id
      :i_name
      :i_end_date
      :i_u_id
      :i_status
      :i_num_global_attrs]}}})
