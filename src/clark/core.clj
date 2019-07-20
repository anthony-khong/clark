(ns clark.core
  (:require [flambo.sql :as sql]
            [flambo.sql-functions]
            [clark.functions :as cl :refer [col]])
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.sql functions))
  (:gen-class))

(defonce spark (.. (SparkSession/builder)
                   (appName "Simple app")
                   (master "local[*]")
                   getOrCreate))

(def raw-dataframe (cl/read-csv spark "data.csv"))

;; Actual Script

(def dataframe
  (-> raw-dataframe
      (cl/rename {"Keyword status"   "keyword_status"
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
      (cl/with-column "cr" (-> (col "cr")
                               (cl/regexp-replace "%" "")
                               (cl// (cl/lit 100))
                               (cl/cast "float")))
      (cl/with-column "clicks" (-> (col "clicks")
                                   (cl/cast "float")))))

;; (-> dataframe
;;     (cl/agg (cl/count-distinct (col "status")))
;;     .show)

;; (-> dataframe
;;     (cl/limit 10000)
;;     (cl/filter (cl/is-not-null (col "keyword")))
;;     (cl/filter (cl/<= (cl/lit 0) (col "cost")))
;;     (cl/with-column "kwcost"
;;                     (cl/over (cl/sum "cost")
;;                              (-> (cl/window)
;;                                  (cl/partition-by "keyword"))))
;;     (cl/filter (cl/not-equal (col "kwcost") (col "cost")))
;;     (cl/select "kwcost" "cost")
;;     cl/show)

;; (-> dataframe
;;     (cl/group-by "keyword" "status")
;;     (cl/agg (cl/as (cl/sum "cost") "freq"))
;;     cl/show)

;; (-> dataframe
;;     (cl/agg (cl/sum "clicks")
;;             (cl/max "ctr"))
;;     cl/show)

;; (-> dataframe
;;     (cl/describe "clicks")
;;     cl/show)

;; (println (cl/print-schema dataframe) (cl/count dataframe))

;; TODO: create recommendations based on CPC of JG
