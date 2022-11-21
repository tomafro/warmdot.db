(ns warmdot.db.dataset
  (:refer-clojure :exclude [select update])
  (:require [honey.sql.helpers]))

(defn merge-queries
  ([& queries]
   (reduce (fn [q1 q2]
             (-> (merge q1 (dissoc q2 :where))
                 (honey.sql.helpers/where (:where q2))))
           queries)))

(defprotocol Dataset
  (select [this {:as options}])
  (insert [this values])
  (update [this {:as options}])
  (delete [this {:as options}]))

(defn select-one
  [dataset {:as options}]
  (select dataset (assoc options :limit 1)))

(defn dataset?
  [x]
  (satisfies? Dataset x))

(extend-protocol Dataset
  clojure.lang.Keyword
  (select
    [table {:as options}]
    (merge-queries {:select [:*] :from [table]} options))

  (insert
    [table {:as options}]
    (merge-queries {:insert-into table
                    :values :default
                    :returning [:*]}
                   options))

  (update
    [table {:as options}]
    (merge-queries {:update table} options))

  (delete
    [table {:as options}]
    (merge-queries {:delete-from table} options))

  clojure.lang.APersistentMap
  (select
    [dataset {:as options}]
    (merge-queries {:with [[:dataset dataset]]
                    :select [:*]
                    :from [:dataset]}
                   options)))

(defn row-query
  [table id builder options]
  (merge-queries (builder table options) {:where [:= id :id]}))

(defrecord Row [table id]
  Dataset
  (select
    [_row {:as options}]
    (row-query table id select options))

  (update
    [_row {:as options}]
    (row-query table id update options))

  (delete
    [_row {:as options}]
    (row-query table id delete options)))

(defn by
  [& {:as conditions}]
  (reduce (fn [q [a b]] (honey.sql.helpers/where q [:= a b])) {} conditions))
