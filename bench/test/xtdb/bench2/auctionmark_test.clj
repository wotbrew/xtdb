(ns xtdb.bench2.auctionmark-test
  (:require [clojure.test :as t]
            [xtdb.bench2 :as b2]
            [xtdb.bench2.auctionmark :as am]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.api :as xt]
            [xtdb.bench2.core1 :as bcore1])
  (:import (java.time Clock)
           (java.util Random)
           (java.util.concurrent ConcurrentHashMap)))

(t/use-fixtures :each fix/with-node)

(defn- ->worker [node]
  (let [clock (Clock/systemUTC)
        domain-state (ConcurrentHashMap.)
        custom-state (ConcurrentHashMap.)
        root-random (Random. 112)
        reports (atom [])
        worker (b2/->Worker node root-random domain-state custom-state clock reports)]
    worker))

(t/deftest generate-user-test
  (let [worker (->worker *api*)]
    (bcore1/generate worker am/generate-user 1)
    (xt/sync *api*)
    (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :u_id]]}))))
    (t/is (= "u_0" (b2/sample-flat worker am/user-id)))))

(t/deftest generate-categories-test
  (let [worker (->worker *api*)]
    (am/load-categories-tsv worker)
    (bcore1/generate worker am/generate-category 1)
    (xt/sync *api*)
    (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :c_id]]}))))
    (t/is (= "c_0" (b2/sample-flat worker am/category-id)))))

(t/deftest generate-region-test
  (let [worker (->worker *api*)]
    (bcore1/generate worker am/generate-region 1)
    (xt/sync *api*)
    (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :r_id]]}))))
    (t/is (= "r_0" (b2/sample-flat worker am/region-id)))))

(t/deftest generate-global-attribute-group-test
  (let [worker (->worker *api*)]
    (am/load-categories-tsv worker)
    (bcore1/generate worker am/generate-category 1)
    (bcore1/generate worker am/generate-global-attribute-group 1)
    (xt/sync *api*)
    (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :gag_name]]}))))
    (t/is (= "gag_0" (b2/sample-flat worker am/gag-id)))))

(t/deftest generate-global-attribute-value-test
  (let [worker (->worker *api*)]
    (am/load-categories-tsv worker)
    (bcore1/generate worker am/generate-category 1)
    (bcore1/generate worker am/generate-global-attribute-group 1)
    (bcore1/generate worker am/generate-global-attribute-value 1)
    (xt/sync *api*)
    (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :gav_name]]}))))
    (t/is (= "gav_0" (b2/sample-flat worker am/gav-id)))))

(t/deftest generate-user-attributes-test
  (let [worker (->worker *api*)]
    (bcore1/generate worker am/generate-user 1)
    (bcore1/generate worker am/generate-user-attributes 1)
    (xt/sync *api*)
    (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :ua_u_id]]}))))
    (t/is (= "ua_0" (b2/sample-flat worker am/user-attribute-id)))))

(t/deftest generate-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *api*)]
      (bcore1/generate worker am/generate-user 1)
      (am/load-categories-tsv worker)
      (bcore1/generate worker am/generate-category 1)
      (bcore1/generate worker am/generate-item 1)
      (xt/sync *api*)
      (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :i_id]]}))))
      (t/is (= "i_0" (:i_id (am/random-item worker :status :open)) )))))

(t/deftest proc-get-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *api*)]
      (bcore1/generate worker am/generate-user 1)
      (am/load-categories-tsv worker)
      (bcore1/generate worker am/generate-category 1)
      (bcore1/generate worker am/generate-item 1)
      (xt/sync *api*)
      (t/is (= "i_0" (-> (am/proc-get-item worker) ffirst :i_id))))))

(t/deftest proc-new-user-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *api*)]
      (am/load-categories-tsv worker)
      (bcore1/generate worker am/generate-category 1)
      (bcore1/generate worker am/generate-item 1)
      (am/proc-new-user worker)
      (xt/sync *api*)
      (t/is (= [1] (first (xt/q (xt/db *api*) '{:find [(count id)] :where [[id :u_id]]}))))
      (t/is (= "u_0" (b2/sample-flat worker am/user-id))))))

(t/deftest proc-new-bid-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *api*)]
      (t/testing "new bid"
        (bcore1/install-tx-fns worker {:apply-seller-fee am/tx-fn-apply-seller-fee, :new-bid am/tx-fn-new-bid})
        (bcore1/generate worker am/generate-user 1)
        (am/load-categories-tsv worker)
        (bcore1/generate worker am/generate-category 1)
        (bcore1/generate worker am/generate-item 1)
        (am/proc-new-bid worker)
        (xt/sync *api*)
        ;; item has a new bid
        (t/is (= 1 (-> (xt/q (xt/db *api*) '{:find [(pull id [*])] :where [[id :i_id]]})
                       ffirst :i_num_bids)))
        ;; there exists a bid
        (t/is (= {:ib_i_id "i_0", :ib_id "ib_0"}
                 (ffirst (xt/q (xt/db *api*) '[:find (pull ib [:ib_id :ib_i_id]) :where [ib :ib_id]]))))
        ;; new max bid
        (t/is (= ["ib_0-i_0" "i_0"] (first (xt/q (xt/db *api*) '[:find imb imb_i_id :where [imb :imb_i_id imb_i_id]])))))
      (t/testing "new bid but does not exceed max"
        (with-redefs [am/random-price (constantly Double/MIN_VALUE)]
          (bcore1/generate worker am/generate-user 1)
          (am/proc-new-bid worker)
          (xt/sync *api*)
          ;; new bid
          (t/is (= 2 (-> (xt/q (xt/db *api*) '{:find [(pull id [*])] :where [[id :i_id]]})
                         ffirst :i_num_bids)))
          ;; winning bid remains the same
          (t/is (= ["ib_0-i_0" "i_0"] (first (xt/q (xt/db *api*) '[:find imb imb_i_id :where [imb :imb_i_id imb_i_id]])))))))))

(t/deftest proc-new-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *api*)]
      (t/testing "new bid"
        (bcore1/install-tx-fns worker {:apply-seller-fee am/tx-fn-apply-seller-fee, :new-bid am/tx-fn-new-bid})
        (bcore1/generate worker am/generate-user 1)
        (am/load-categories-tsv worker)
        (bcore1/generate worker am/generate-category 10)
        (bcore1/generate worker am/generate-global-attribute-group 10)
        (bcore1/generate worker am/generate-global-attribute-value 100)
        (am/proc-new-item worker)
        (xt/sync *api*)
        ;; new item
        (let [{:keys [i_id i_u_id]} (ffirst (xt/q (xt/db *api*) '{:find [(pull id [*])] :where [[id :i_id]]}))]
          (t/is (= "i_0" i_id))
          (t/is (= "u_0" i_u_id)))
        (t/is (< (- (ffirst (xt/q (xt/db *api*) '{:find [u_balance]
                                                  :where [[u :u_id uid]
                                                          [u :u_balance u_balance]]}))
                    (double -1.0))
                 0.0001))))))
