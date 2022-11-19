(ns warmdot.db.connection)

(defprotocol Connection
  (execute! [this query])
  (execute-one! [this query]))

(defn connection?
  [x]
  (satisfies? Connection x))
