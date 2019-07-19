(ns clark.core
  (:require [flambo.sql :as sql :refer [create-custom-schema group-by agg order-by window over]]
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

(.printSchema dataframe)

(->> dataframe
     .columns
     (map println))


(->> spark
     #(.textFile % "data.csv"))

(.textFile spark "data.csv")

;; Utility Functions
(defn select [dataframe column-names]
  (let [columns (map #(Column. %) column-names)]
    (select-cols dataframe columns)))

(defn select-cols [dataframe columns]
  (->> columns
       scala.collection.JavaConversions/asScalaBuffer
       (.select dataframe)))

(defn rename [dataframe rename-map]
  (let [columns (map
                 (fn [[old-name new-name]]
                   (.as (Column. old-name) new-name))
                 rename-map)]
    (select-cols dataframe columns)))


(-> dataframe
    (rename {"Clicks" "clicks"
             "CTR" "ctr"
             "`Conv. rate`" "cr"})
    .show)

(-> dataframe
    (.agg))

(.agg dataframe
      (scala.collection.JavaConversions/asScalaBuffer
       [(functions/sum (Column. "Clicks"))]))

(.agg dataframe
      (functions/sum (Column. "Clicks"))
      (scala.collection.JavaConversions/asScalaBuffer []))

;; TODO: Abstract out asScalaBuffer
;; TODO: Abstract out columns
;; TODO: Abstract out aggregates
;; TODO: Have a describe function
;; TODO: Try cleaning the percentages
(.show (.describe dataframe
           (scala.collection.JavaConversions/asScalaBuffer ["Clicks"])))
