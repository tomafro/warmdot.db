(ns warmdot.db
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
  (if (and *current-db* (thread-bound? #'*current-db*))
    (set! *current-db* db)
    (alter-var-root #'*current-db* (constantly db))))

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

(defprotocol GetDb
  (get-db* [this]))

(extend-protocol GetDb
  clojure.lang.APersistentMap
  (get-db* [this]
    (get this ::db))
  
  clojure.lang.APersistentVector
  (get-db* [[db _dataset]]
    db))

(defn get-db
  [source]
  (and (satisfies? GetDb source)
       (get-db* source)))

(defn find-db
  [& sources]
  (or (some get-db sources)
      *current-db*
      (throw-no-db)))

(defn- execute-query!
  [f dataset query]
  (let [connection (find-db query dataset)
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
  `(jdbc/transact (find-db)
                  (^{:once true} fn* [database#]
                                     (with-db database#
                                       ~@body))))

(defn rollback!
  []
  (.rollback (find-db)))

(defn execute!
  ([query] (execute! nil query))
  ([dataset query]
   (execute-query! jdbc/execute! dataset query)))

(defn execute-one!
  ([query] (execute-one! nil query))
  ([dataset query]
   (execute-query! jdbc/execute-one! dataset query)))

(defn- update-count-or-result
  [result]
  (or (-> result first ::jdbc/update-count)
      result))

(defn find-all
  [dataset & {:as options}]
  (execute! dataset (dataset/select dataset options)))

(defn find-first
  [dataset & {:as options}]
  (execute-one! dataset (dataset/select-one dataset options)))

(def find-first! (wrap-not-found find-first))

(defn by-id
  [dataset id & {:as options}]
  (execute-one! dataset (dataset/merge-queries (dataset/select-one dataset options) {:where [:= :id id]})))

(def by-id!
  (wrap-not-found by-id))

(defn pluck-all
  [dataset columns & {:as options}]
  (if (coll? columns)
    (map vals (execute! dataset (dataset/select dataset (assoc options :select columns))))
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
  (update-count-or-result (execute! dataset (dataset/insert dataset options))))

(defn update!
  [dataset & {:as options}]
  (update-count-or-result (execute! dataset (dataset/update dataset options))))

(defn delete!
  [dataset & {:as options}]
  (update-count-or-result (execute! dataset (dataset/delete dataset options))))

(defn exists?
  [dataset & {:as options}]
  (boolean (seq (execute-one! dataset {:select [true] :where [:exists (dataset/select dataset options)]}))))

(defn row-count
  [dataset & {:as options}]
  (pluck-first dataset :%count.* options))

(defn columns
  [table]
  (find-all :information_schema.columns {:select [:column_name :data_type :udt_name :is_nullable :column_default]
                                         :where [:= :table_name (name table)]}))

(defn backend-pid
  [] 
  (:pid (warmdot.db/execute-one! "select pg_backend_pid() pid")))

(defn- dataset-function-def
  [dataset function]
  `(def ~(-> function name symbol) (partial ~function ~dataset)))

(def query-functions
  [`find-all
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
   `row-count])

(defmacro define-dataset-functions
  [dataset]
  `(do ~@(map #(dataset-function-def dataset %) query-functions)))
