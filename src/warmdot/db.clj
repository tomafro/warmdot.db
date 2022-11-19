;; Provides a number of functions to make it easier to query the database. All
;; the main functions take a dataset as their first argument. The dataset can 
;; be either a keyword (representing a database table) or a map (representing 
;; a honeysql query).
;;
;;     (find-first :users)
;;     (find-first {:select [:*] :from [:users]})
;;
;; The functions also all take an optional map of honey-sql clauses as their 
;; last argument, which will be merged into the generated query where possible
;;
;;     (find-first :users :where [:= :id 1])
;;     (find-first :users :select [:email])
;;     (find-all {:select [:email] :from [:users]} :limit 100)
;;
;; Functions that expect to return a single result all have two forms, a 
;; standard version, and an 'exceptional' version with an '!' at the end. The
;; only difference between the two is that the latter will raise an exception
;; when nothing is returned. This is useful in those situations where a missing
;; result indicates that something has gone wrong.
;;
;; (find-first :users :where [:= 1 2]) => nil 
;; (find-first! :users :where [:= 1 2]) => raises an exception 
;;
;; None of the examples so far directly take a database connection. Instead, the 
;; connection can be specified in several ways. Using `set-connection`,
;; `with-connection`, or the namespaced :warmdot.db/connection option directly
;; in a query.

;; These are all roughtly equivalent:
;;
;;     (find-first :users :warmdot.db/connection database)
;;
;;     (with-connection database
;;       (find-first :users))
;; 
;;     (set-connection database)
;;     (find-first :users)
;;
;; One reason that the database isn't taken more directly as an argument is that
;; these functions are all designed not just to be used directly, but also to be
;; built on via partials:
;;
;;     (ns db.teams) 
;;
;;     (def teams-dataset
;;       {:select [:teams.* [:users.email :owner-email]]
;;        :from [:teams]
;;        :join [[:users] [:= :users.id :teams.owner-id]]})
;;
;;     (def find-all (partial finders/find-all teams-dataset))
;;     (def find-first (partial finders/find-first teams-dataset))

(ns warmdot.db
  (:refer-clojure :exclude [count])
  (:require [warmdot.db.connection :as connection]
            [warmdot.db.dataset :as dataset]))

(def ^:dynamic *current-connection* nil)

(defmacro with-connection
  [connection & body]
  `(with-bindings {#'*current-connection* ~connection}
     ~@body))

(defn set-connection
  [connection]
  (alter-var-root #'*current-connection* (constantly connection)))

(defn- throw-no-connection
  []
  (throw (ex-info "No connection found; either set a global connection with `set-connection`, a temporary connection with `with-connection`, or pass in a :warmdot.db/connection as an option"
                  {:type ::connection-missing})))

(defn- throw-record-not-found
  [dataset & args]
  (throw (ex-info "Record not found"
                  {:type ::database-exception :dataset dataset :args args})))

(defn- wrap-not-found
  [f]
  (fn [dataset & args] (or (apply f dataset args)
                           (throw-record-not-found dataset args))))

(defn- find-connection!
  [query]
  (or (and (map? query) (get query ::connection))
      *current-connection*
      (throw-no-connection)))

(defn execute!
  [query]
  (connection/execute! (find-connection! query) query))

(defn execute-one!
  [query]
  (connection/execute-one! (find-connection! query) query))

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
  ([dataset] (insert! dataset nil))
  ([dataset values]
   (last (first (execute-one! (dataset/insert dataset (when values [values])))))))

(defn update!
  [dataset & {:as options}]
  (last (first (execute-one! (dataset/update dataset options)))))

(defn delete!
  [dataset & {:as options}]
  (last (first (execute-one! (dataset/delete dataset options)))))

(defn exists?
  [dataset & {:as options}]
  (boolean (first (execute! {:select [true] :where [:exists (dataset/select dataset options)]}))))

(defn count
  [dataset & {:as options}]
  (pluck-first dataset :%count.* options))

(defn define-dataset-function
  [dataset function]
  `(def ~(-> function name symbol) (partial ~function ~dataset)))

(defmacro define-dataset-functions
  [dataset]
  `(do ~@(map #(define-dataset-function dataset %) [`find-all
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
                                                    `count])))
