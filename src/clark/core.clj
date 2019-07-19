(ns clark.core
  (:require [flambo.sql :as sql]
            [flambo.sql-functions :refer [col]]
            [flambo.api :as api]
            [clojure.reflect :refer [reflect]]
            [clojure.pprint :refer [print-table]])
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark SparkContext)
           (org.apache.spark.api.java JavaSparkContext)
           (org.apache.spark.sql Column)
           (org.apache.spark.sql functions RowFactory)
           (org.apache.spark.sql.types DataTypes)
           (org.apache.spark.sql.expressions Window)
           (org.apache.spark SparkContext))
  (:gen-class))

(defonce spark (.. (SparkSession/builder)
                   (appName "Simple app")
                   (master "local[*]")
                   getOrCreate))

(def dataframe (.. spark
                   read
                   (options {"header" "true"
                             "date" "yyyy-MM-dd"
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

(-> dataframe
    (rename {"Keyword status" "keyword_status"
             "Keyword" "keyword"
             "Campaign" "campaign"
             "Ad group" "ad_group"
             "Currency code" "currency_code"
             "Status" "status"
             "Max. CPC" "max_cpc"
             "Policy details" "policy_details"
             "Final URL" "final_url"
             "Mobile final URL" "mobile_final_url"
             "Clicks" "clicks"
             "Impr." "impressions"
             "CTR" "ctr"
             "Avg. CPC" "avg_cpc"
             "Cost" "cost"
             "Conversions" "conversions"
             "Cost / conv." "cost_per_conv"
             "Conv. rate" "cr"})
    sql/print-schema)

;; TODO: Abstract out asScalaBuffer
;; TODO: Abstract out columns
;; TODO: Abstract out aggregates
;; TODO: Have a describe function
;; TODO: Try cleaning the percentages
;; (.show (.describe dataframe
;;            (scala.collection.JavaConversions/asScalaBuffer ["Clicks"])))
