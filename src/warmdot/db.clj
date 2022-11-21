(ns warmdot.db
  (:refer-clojure :exclude [count])
  (:require [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [warmdot.db.dataset :as dataset]))

(def ^:dynamic *current-db* nil)

(defmacro with-db
  [db & body]
  `(with-bindings {#'*current-db* ~db}
     ~@body))

(defn set-db!
  [db]
  (alter-var-root #'*current-db* (constantly db)))

(defn- throw-no-db
  []
  (throw (ex-info "No connection found; either set a global connection with `set-db!`, a temporary connection with `with-db`, or pass in :warmdot.db/db as an option"
                  {:type ::connection-missing})))

(defn- throw-record-not-found
  [dataset & args]
  (throw (ex-info "Record not found"
                  {:type ::database-exception :dataset dataset :args args})))

(defn- wrap-not-found
  [f]
  (fn [dataset & args] (or (apply f dataset args)
                           (throw-record-not-found dataset args))))

(defn find-db!
  ([] (find-db! nil))
  ([query]
   (or (and (map? query) (get query ::db))
       *current-db*
       (throw-no-db))))

(defn- execute-query!
  [f query]
  (let [connection (find-db! query)
        query (if (map? query) (dissoc query ::db) query)
        formatted-query (if (string? query)
                          [query]
                          (sql/format query))]
    (try
      (f connection formatted-query)
      (catch org.postgresql.util.PSQLException e
        (throw (ex-info (str "Query failed: " (.getMessage e)) {:query (if (string? query) [query] (sql/format query {:inline true}))} e))))))

(defmacro with-transaction
  [& body]
  `(jdbc/transact (find-db!)
                  (^{:once true} fn* [database#]
                                     (with-db database#
                                       ~@body))))

(defn rollback!
  []
  (.rollback (find-db!)))

(defn execute!
  [query]
  (execute-query! jdbc/execute! query))

(defn execute-one!
  [query]
  (execute-query! jdbc/execute-one! query))

(defn- insert-update-or-delete!
  [query]
  (last (first (execute-one! query))))

(defn find-all
  [dataset & {:as options}]
  (execute! (dataset/select dataset options)))

(defn find-first
  [dataset & {:as options}]
  (execute-one! (dataset/select-one dataset options)))

(def find-first! (wrap-not-found find-first))

(defn by-id
  [dataset id & {:as options}]
  (execute-one! (dataset/merge-queries (dataset/select-one dataset options) {:where [:= :id id]})))

(def by-id!
  (wrap-not-found by-id))

(defn pluck-all
  [dataset columns & {:as options}]
  (if (coll? columns)
    (map vals (execute! (dataset/select dataset (assoc options :select columns))))
    (map first (pluck-all dataset [columns] options))))

(defn pluck-first
  [dataset columns & {:as options}]
  (first (pluck-all dataset columns (assoc options :limit 1))))

(defn pluck-first!
  [dataset columns & {:as options}]
  (if-let [result (seq (pluck-all dataset columns (assoc options :limit 1)))]
    (first result)
    (throw-record-not-found dataset columns options)))

(defn insert!
  [dataset & {:as options}]
  (insert-update-or-delete! (dataset/insert dataset options)))

(defn update!
  [dataset & {:as options}]
  (insert-update-or-delete! (dataset/update dataset options)))

(defn delete!
  [dataset & {:as options}]
  (insert-update-or-delete! (dataset/delete dataset options)))

(defn exists?
  [dataset & {:as options}]
  (boolean (seq (execute-one! {:select [true] :where [:exists (dataset/select dataset options)]}))))

(defn row-count
  [dataset & {:as options}]
  (pluck-first dataset :%count.* options))

(defn- dataset-function-def
  [dataset function]
  `(def ~(-> function name symbol) (partial ~function ~dataset)))

(defmacro define-dataset-functions
  [dataset]
  `(do ~@(map #(dataset-function-def dataset %) [`find-all
                                                 `find-first
                                                 `find-first!
                                                 `by-id
                                                 `by-id!
                                                 `pluck-all
                                                 `pluck-first
                                                 `pluck-first!
                                                 `insert!
                                                 `update!
                                                 `delete!
                                                 `exists?
                                                 `row-count])))
