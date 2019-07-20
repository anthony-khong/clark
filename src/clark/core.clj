(ns clark.core
  (:require [flambo.sql :as sql]
            [flambo.sql-functions :refer [col]])
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.sql functions))
  (:gen-class))

(defonce spark (.. (SparkSession/builder)
                   (appName "Simple app")
                   (master "local[*]")
                   getOrCreate))

(def raw-dataframe (.. spark
                       read
                       (options {"header"      "true"
                                 "date"        "yyyy-MM-dd"
                                 "inferSchema" "true"})
                       (csv "data.csv")))

;; Utility Functions
(defn col-as [expr alias]
  (.as (col expr) alias))

(defn escape-keys [some-map]
  (into {} (map (fn [[k v]] [(str "`" k "`") v]) some-map)))

(defn rename [dataframe rename-map]
  (let [columns (sql/columns dataframe)
        renamed-exprs (->> rename-map
                           (merge (zipmap columns columns))
                           escape-keys
                           (map #(apply col-as %)))]
    (apply sql/select dataframe renamed-exprs)))

(defn describe [dataframe expr & exprs]
  (.describe dataframe (into-array java.lang.String (conj exprs expr))))

(defn sample [dataframe with-replacement fraction]
  (.sample dataframe with-replacement fraction))

(defn limit [dataframe n-rows]
  (.limit dataframe n-rows))

;; Actual Script
(def dataframe
  (-> raw-dataframe
      (rename {"Keyword status"   "keyword_status"
               "Keyword"          "keyword"
               "Campaign"         "campaign"
               "Ad group"         "ad_group"
               "Currency code"    "currency_code"
               "Status"           "status"
               "Max. CPC"         "max_cpc"
               "Policy details"   "policy_details"
               "Final URL"        "final_url"
               "Mobile final URL" "mobile_final_url"
               "Clicks"           "clicks"
               "Impr."            "impressions"
               "CTR"              "ctr"
               "Avg. CPC"         "avg_cpc"
               "Cost"             "cost"
               "Conversions"      "conversions"
               "Cost / conv."     "cost_per_conv"
               "Conv. rate"       "cr"})
      (.withColumn "cr" (-> (col "cr")
                            (functions/regexp_replace "%" "")
                            (.divide (functions/lit 100))
                            (.cast "float")))
      (.withColumn "clicks" (-> (col "clicks")
                                (.cast "float")))))


;; (-> dataframe
;;     (limit 10000)
;;     (.filter (.isNotNull (col "keyword")))
;;     (.withColumn "kwcost"
;;                  (.over (functions/sum "cost")
;;                         (sql/partition-by (sql/window) "keyword")))
;;     (.filter (.notEqual (col "kwcost") (col "cost")))
;;     (sql/select "kwcost" "cost")
;;     ;; (sql/group-by "keyword")
;;     ;; (sql/agg (.as (functions/sum "cost") "freq"))
;;     .show)

;; (println (sql/print-schema dataframe) (sql/count dataframe))


;; (-> dataframe
;;     (sql/agg (functions/sum "clicks")
;;              (functions/max "ctr"))
;;     sql/show)


;; (-> dataframe
;;     (describe "clicks")
;;     sql/show)


;; (-> dataframe
;;     (sql/select "cr")
;;     sql/show)

;; (sql/print-schema dataframe)
