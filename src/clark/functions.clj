(ns clark.functions
  (:refer-clojure :exclude [+ - * / <= < >= >])
  (:require [flambo.sql :as sql]
            [flambo.sql-functions :as [sql]])
  (:import (org.apache.spark.sql functions)
           [org.apache.spark.sql Column]))


;; IO
(defn read-csv [spark file]
  (.. spark
      read
      (options {"header"      "true"
                "date"        "yyyy-MM-dd"
                "inferSchema" "true"})
      (csv file)))

;; Utils
(defn col [x] (sqlf/col x))

(defn col-as [expr alias]
  (.as (col expr) alias))

(defn escape-keys [some-map]
  (into {} (map (fn [[k v]] [(str "`" k "`") v]) some-map)))

(defn as-col-array [exprs]
  (into-array Column (clojure.core/map col exprs)))

;; Expressions
(defn lit [value] (functions/lit value))

(defn cast [column type] (.cast column type))

(defn + [x y] (.plus x y))

(defn - [x y] (.minus x y))

(defn * [x y] (.multiply x y))

(defn / [x y] (.divide x y))

(defn < [x y] (.lt x y))

(defn <= [x y] (.leq x y))

(defn > [x y] (.gt x y))

(defn >= [x y] (.geq x y))

(defn equal [x y] (.equalTo x y))

(defn not-equal [x y] (.notEqual x y))

(defn is-null [x] (.isNull x))

(defn is-not-null [x] (.isNotNull x))

(defn as [column name] (.as column name))

(defn regexp-replace [column pattern replacement]
  (functions/regexp_replace column pattern replacement))

(defn approx-count-distinct [x] (functions/approx_count_distinct x))

;; Aggregate Expressions
(defn count-distinct [x & xs]
  (functions/countDistinct (col x) (as-col-array xs)))

(defn first [x] (functions/first x))

(defn kurtosis [x] (functions/kurtosis x))

(defn max [x] (functions/max x))

(defn mean [x] (functions/mean x))

(defn min [x] (functions/min x))

(defn skewness [x] (functions/skewness x))

(defn std [x] (functions/stddev x))

(defn sum [x] (functions/sum x))

(defn var [x] (functions/variance x))

(defn over [agg-expr window] (.over agg-expr window))

;; Windows
(defn window [] (sql/window))

(defn order-by [window & exprs]
  (apply sql/order-by window exprs))

(defn partition-by [window & exprs]
  (apply sql/partition-by window exprs))

(defn rows-between [window lower upper]
  (sql/rows-between window lower upper))

(defn range-between [window lower upper]
  (sql/range-between window lower upper))

;; Dataframe
(defn filter [dataframe expr]
  (.filter dataframe expr))

(defn group-by [dataframe & exprs]
  (apply sql/group-by dataframe exprs))

(defn agg [dataframe expr & exprs]
  (apply sql/agg dataframe expr exprs))

(defn select [dataframe expr & exprs]
  (apply sql/select dataframe expr exprs))

(defn describe [dataframe expr & exprs]
  (.describe dataframe (into-array java.lang.String (conj exprs expr))))

(defn sample [dataframe with-replacement fraction]
  (.sample dataframe with-replacement fraction))

(defn limit [dataframe n-rows]
  (.limit dataframe n-rows))

(defn with-column [dataframe name expr]
  (.withColumn dataframe name expr))

(defn rename [dataframe rename-map]
  (let [columns (sql/columns dataframe)
        renamed-exprs (->> rename-map
                           (merge (zipmap columns columns))
                           escape-keys
                           (map #(apply col-as %)))]
    (apply sql/select dataframe renamed-exprs)))

(defn cache [dataframe] (cache dataframe))

(def show (memfn show))

(def count (memfn count))

(def print-schema (memfn printSchema))
