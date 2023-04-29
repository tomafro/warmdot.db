(ns warmdot.db
  (:require [honey.sql :as sql]
            [warmdot.db.connection :as connection]
            [warmdot.db.dataset :as dataset]
            [warmdot.db :as db]))

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

(defn- wrap-throw-not-found-if-no-result
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

(defn- get-db
  [source]
  (and (satisfies? GetDb source)
       (get-db* source)))

(defn find-db!
  "Returns the current database connection given a number of possible sources. The sources are 
 checked in order, with the first found connection returned. If no connection is found in the 
 sources, the current global connection is returned. If this too is not set, an exception is
 thrown."
  [& sources]
  (or (some get-db sources)
      *current-db*
      (throw-no-db)))

(defn- execute-query!
  [f dataset query]
  (let [connection (find-db! query dataset)
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
  `(let [db# (find-db!)]
     (connection/with-transaction
       db#
       (^{:once true} fn* [database#]
                          (with-db database#
                            ~@body)))))

(defn rollback-transaction!
  []
  (connection/rollback-transaction! (find-db!)))

(defn rollback-to-savepoint!
  ([] (rollback-to-savepoint! nil))
  ([savepoint-name]
   (connection/rollback-to-savepoint! (find-db!) savepoint-name)))

(defmacro with-named-savepoint
  [savepoint-name & body]
  `(with-transaction
     (connection/with-named-savepoint
       (find-db!)
       ~savepoint-name
       (^{:once true} fn* [database#]
                          (with-db database#
                            ~@body)))))

(defmacro with-savepoint
  [& body]
  `(let [savepoint-name# (str (gensym "savepoint"))]
     (with-named-savepoint savepoint-name#
       ~@body)))

(defn execute!
  ([query] (execute! nil query))
  ([dataset query]
   (execute-query! connection/execute! dataset query)))

(defn execute-one!
  ([query] (execute-one! nil query))
  ([dataset query]
   (execute-query! connection/execute-one! dataset query)))

(defn- update-count-or-result
  [result]
  (or (-> result first :next.jdbc/update-count)
      result))

(defn find-all
  "Finds all rows in the given dataset"
  [dataset & {:as options}]
  (execute! dataset (dataset/select dataset options)))

(defn find-first
  "Finds the first row in the given dataset, or nil if no rows are found"
  [dataset & {:as options}]
  (execute-one! dataset (dataset/select-one dataset options)))

(def find-first!
  "Find the first row in the given dataset, or throws an exception if no rows are found"
  (wrap-throw-not-found-if-no-result find-first))

(defn by-id
  "Finds the row in the given dataset with the given id, or nil if no row are found"
  [dataset id & {:as options}]
  (execute-one! dataset (dataset/merge-queries (dataset/select-one dataset options) {:where [:= :id id]})))

(def by-id!
  "Find the row in the given dataset with the given id, or throws an exception if no row is found"
  (wrap-throw-not-found-if-no-result by-id))

(defn pluck-all
  "Plucks the given column or columns from the given dataset. If a single column is given, returns a
   sequences of values for that column. If a collection of columns is given returns a sequence of maps.
   
   For example:
   
   (pluck-all :people :name) => [\"John\" \"Jane\" \"Joe\"]
   (pluck-all :people [:name :age]) => [{:name \"John\" :age 30} {:name \"Jane\" :age 25} {:name \"Joe\" :age 40}]"
  [dataset columns & {:as options}]
  (if (coll? columns)
    (map vals (execute! dataset (dataset/select dataset (assoc options :select columns))))
    (map first (pluck-all dataset [columns] options))))

(defn pluck-first
  "Works the same way as pluck-all, returning the first result or nil if no results are found"
  [dataset columns & {:as options}]
  (first (pluck-all dataset columns (assoc options :limit 1))))

(defn pluck-first!
  "Works the same way as pluck-all, returning the first result or raising an exception if no results are found"
  [dataset columns & {:as options}]
  (if-let [result (seq (pluck-all dataset columns (assoc options :limit 1)))]
    (first result)
    (throw-record-not-found dataset columns options)))

(defn insert!
  "Inserts the given values into the given dataset. Returns either the number of rows inserted into the
   database, or any values returned by the insert statement. For example:
   
   (insert! :people {:name \"John\" :age 30}) => 1
   (insert! :people {:name \"John\" :age 30} :returning [:id]) => [{:id 1}]"
  [dataset & {:as options}]
  (update-count-or-result (execute! dataset (dataset/insert dataset options))))

(defn update!
  "Updates the given values in the given dataset. Returns either the number of rows updated in the
   database, or any values returned by the update statement. For example:
   
   (update! :people :set {:name \"Kate\"}) => 1
   (update! :people :set {:name \"Kate\"} :returning [:id]) => [{:id 1}]"
  [dataset & {:as options}]
  (update-count-or-result (execute! dataset (dataset/update dataset options))))

(defn delete!
  "Deletes the given rows in the given dataset. Returns either the number of rows deleted in the
   database, or any values returned by the delete statement. For example:
   
   (delete! :people :where [:= :id 1]) => 1
   (delete! :people :where [:= :id 1] :returning [:id]) => [{:id 1}]"
  [dataset & {:as options}]
  (update-count-or-result (execute! dataset (dataset/delete dataset options))))

(defn exists?
  "Returns true if the given dataset has any rows, false otherwise. For example:
   
   (exists? :people) => true
   (exists? :people :where [:= :age 42]) => false"
  [dataset & {:as options}]
  (boolean (seq (execute-one! dataset {:select [true] :where [:exists (dataset/select dataset options)]}))))

(defn row-count
  "Returns the number of matching rows in the dataset, for example:
   
   (row-count :people) => 15
   (row-count :people :where [:= :age 42]) => 0"
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
  "Defines all the query functions for the given dataset. For example:
   
   (ns db.people
     (:require [warmdot.db :as db]))
   
   (define-dataset-functions :people)
   
   (ns app
     (:require [db.people]))
   
   (db.people/find-all) => [{:id 1 :name \"John\" :age 30} {:id 2 :name \"Jane\" :age 25}]"

  [dataset]
  `(do ~@(map #(dataset-function-def dataset %) query-functions)))
