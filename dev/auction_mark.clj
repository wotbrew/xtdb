(ns dev.auction-mark
  (:require [xtdb.api :as xt]
            [clojure.string :as str]))

(defn current-timestamp [worker])

(defn submit-tx [worker tx])

(defn domain
  "Defines some value domain (e.g for an attribute / type) that is used in transactions or loading."
  [& opts])

(defn increment [worker domain & args])
(defn pick-gaussian [worker domain & args])
(defn pick-rand [worker domain & args])

(def xt-auto-id (domain))
(def user-id (domain))
(def region-id (domain))
(def user-attribute (domain))
(def item-id (domain))
(def user-id (domain))
(def category-id (domain))
(def global-attribute-group-id (domain))
(def global-attribute-value-id (domain))
(def item-name (domain))
(def item-description (domain))
(def initial-price (domain))
(def reserve-price (domain))
(def buy-now (domain))
(def item-attributes-blob (domain))
(def item-image-path (domain))
(def auction-start-date (domain))
(def auction-end-date (domain))



(defn tx-new-user [worker]
  (->> [[::xt/put {:xt/id (increment worker xt-auto-id)
                   :u_id (increment worker user-id)
                   :u_r_id (pick-gaussian worker region-id)
                   :u_rating 0
                   :u_balance 0.0
                   :u_created (current-timestamp worker)
                   :u_sattr0 (pick-rand worker user-attribute)
                   :u_sattr1 (pick-rand worker user-attribute)
                   :u_sattr2 (pick-rand worker user-attribute)
                   :u_sattr3 (pick-rand worker user-attribute)
                   :u_sattr4 (pick-rand worker user-attribute)
                   :u_sattr5 (pick-rand worker user-attribute)
                   :u_sattr6 (pick-rand worker user-attribute)
                   :u_sattr7 (pick-rand worker user-attribute)}]]
       (submit-tx worker)))

(defn pick-histogram
  "Selects an id using the given pre-built weighting histogram."
  [worker domain])

(defn pick-many [opts f & args])

(defn q [worker q & args])

(defn tx-new-item [worker]
  (let [i_id (increment worker item-id)
        u_id (pick-gaussian worker user-id)
        c_id (pick-histogram worker category-id)
        name (pick-rand worker item-name)
        description (pick-rand worker item-description)
        initial-price (pick-rand worker initial-price)
        reserve-price (pick-rand worker reserve-price)
        buy-now (pick-rand worker buy-now)
        attributes (pick-rand worker item-attributes-blob)
        gag-ids (pick-many {:min 0, :max 16, :unique true} pick-rand global-attribute-group-id)
        gav-ids (pick-many {:min 0, :max 16, :unique true} pick-rand global-attribute-value-id)
        images (pick-many {:min 0, :max 16, :unique true} pick-rand item-image-path)
        start-date (pick-rand worker auction-start-date)
        end-date (pick-rand worker auction-end-date {:min auction-start-date})

        ;; append attribute names to desc
        description-with-attributes
        (->> '[:find ?gag-name ?gav-name
               ;; todo check this param syntax
               :in [[?gag-id] [?gav-id]]
               :where
               [?gav-gag-id :gav_gag_id ?gag-id]
               [?gag-id :gag_name ?gag-name]
               [?gav-id :gav_name ?gav-name]]
             (q worker {:params [gag-ids gav-ids]})
             (str/join " ")
             (str description " "))

        ]

    (->> (concat
           ;; todo fill
           [[::xt/put {}]]
           ;; todo fill
           (for [[i image] (map-indexed vector images)
                 :let [ii_id (bit-or (bit-shift-left i 60) (bit-and i_id 0x0FFFFFFFFFFFFFFF))]]
             [::xt/put {}])
           ;; reg tx fn?
           [[:xt/fn :apply-seller-fee u_id]])
         (submit-tx worker))

    ))

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
