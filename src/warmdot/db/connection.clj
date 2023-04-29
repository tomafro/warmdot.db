(ns warmdot.db.connection
  (:require [next.jdbc :as jdbc]
            [warmdot.db.postgres :as postgres]))

(defn postgres-connection
  [db]
  {::db (postgres/connection db)})

(defn postgres-pool
  [db]
  {::db (postgres/pool db)})

(defn execute!
  [{:keys [::db]} query]
  (jdbc/execute! db query))

(defn execute-one!
  [{:keys [::db]} query]
  (jdbc/execute-one! db query))

(defn with-transaction
  [{:keys [::db] :as connection} f]
  (jdbc/transact
   db
   (^{:once true} fn* [db*]
                      (f (assoc connection ::db db* ::in-transaction? true)))))

(defn rollback-transaction!
  [{:keys [::db]}]
  (.rollback db))

(defn with-named-savepoint
  [{:keys [::db] :as connection} savepoint-name f]
  (let [savepoint (.setSavepoint db savepoint-name)]
    (f (-> connection
           (assoc ::current-savepoint savepoint)
           (update ::all-savepoints assoc savepoint-name savepoint)))))

(defn rollback-to-savepoint!
  [{:keys [::db ::current-savepoint ::all-savepoints] :as connection} savepoint-name]
  (let [savepoint (get all-savepoints savepoint-name current-savepoint)]
    (.rollback db savepoint)))
