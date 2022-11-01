(ns xtdb.bench.report
  (:require [clojure.string :as str]
            [juxt.clojars-mirrors.hiccup.v2v0v0-alpha2.hiccup2.core :as hiccup2]
            [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:import (java.io File)))

(defn stage-points [metric-data]
  (into (sorted-map)
        (for [[stage metric-data] (group-by :stage metric-data)
              :let [first-samples (keep (comp first :samples) metric-data)
                    earliest-sample (reduce min Long/MAX_VALUE (map :time-ms first-samples))]]
          [stage earliest-sample])))

;; use vega to plot metrics for now
;; works at repl, no servers needed
;; if this approach takes of the time series data wants to be graphed in something like a shared prometheus / grafana during run
(defn vega-plots [metric-data]
  (let [stage-time-points (stage-points metric-data)]
    (vec
      (for [[[stage metric] metric-data]
            (sort-by (fn [[[stage metric]]] [(stage-time-points stage) metric])
                     (group-by (juxt :stage :metric) metric-data))]
        {:title (str stage " " metric)
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
                           (assoc spec :data data))))}))))

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
          [:thead [:th "config"] [:th "jre"] [:th "arch"] [:th "os"] [:th "cpu"] [:th "memory"]]
          [:tbody
           (for [{:keys [label, system]} (:systems report)
                 :let [{:keys [jre, arch, os, cpu, memory]} system]]
             [:tr [:th label] [:td jre] [:td arch] [:td os] [:td cpu] [:td memory]])]]]

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
     :systems (for [label key-seq] {:label label, :system (:system (report-map label))})
     :metrics (vec (for [[i label] (map-indexed vector key-seq)
                         report (:reports (report-map label))
                         :let [{:keys [stage, metrics]} (normalize-time report)]
                         metric metrics]
                     (assoc metric :vs-label (str label), :stage stage)))}))

(defn stage-only [report stage]
  (update report :reports (partial filterv #(= stage (:stage %)))))
