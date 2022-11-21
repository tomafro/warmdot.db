# warmdot.db

N.B. This is still a work in progress. The API may radically change before v1.0.

A simple database query library based on `next.jdbc` and `honeysql`.

It provides a number of query functions to make it easier to access the database.
All the main functions take a dataset as their first argument. The dataset can
currently be either a keyword (representing a database table), a map (representing
a honeysql query), or a `warmdot.db.Row` representing a single row.

```clojure
(pluck :users :email)
(find-first {:select [:*] :from [:users]})
(update! (warmdot.db/->Row :users 1) :set {:name "Alessia Russo"})
```

The functions also all take an optional map of honey-sql clauses as their
last argument, which will be merged into the generated query where possible

```clojure
(find-first :users :where [:= :id 1])
(find-first :users :select [:email])
(find-all {:select [:email] :from [:users]} :limit 100)
```

Functions that expect to return a single result all have two forms, a
standard version, and an 'exceptional' version with an '!' at the end. The
only difference between the two is that the latter will raise an exception
when nothing is returned. This is useful in those situations where a missing
result indicates that something has gone wrong.

```clojure
(find-first :users :where [:= 1 2]) => nil
(find-first! :users :where [:= 1 2]) => raises an exception
```

None of the examples so far directly take a database connection. Instead, the
connection can be specified in several ways. Using `set-db!`,
`with-db`, or the namespaced `:warmdot.db/db` option directly
in a query.

These are all roughly equivalent:

```clojure
(find-first :users :warmdot.db/db database)

(with-db database
  (find-first :users))

(set-db! database)
(find-first :users)
```

One reason that the database isn't taken more directly as an argument is that
these functions are all designed not just to be used directly, but also to be
built on via partials:

```clojure
(ns db.teams)

(def teams-dataset
  {:select [:teams.* [:users.email :owner-email]]
   :from [:teams]
   :join [[:users] [:= :users.id :teams.owner-id]]})

(def find-all (partial finders/find-all teams-dataset))
(def find-first (partial finders/find-first teams-dataset))
```

To simplify this, there's a macro that will define all query methods on a
namespace for a provided dataset:

```clojure
(ns db.users)

(warmdot.db/define-dataset-functions :users)
```

When this is called, you can then call functions like the following:

```clojure
(db.users/find-first :where [:= :email "jenny@example.com"])
(db.users/insert! {:name "Claire" :email "claire@example.com"})
(db.users/pluck :email)
```
