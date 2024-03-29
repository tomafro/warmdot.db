(ns warmdot.db-test
  (:require [clojure.test :refer :all]
            [warmdot.db :as db]
            [warmdot.db.connection :as connection]
            [warmdot.db.dataset :as dataset]
            [warmdot.db.test.fixtures :as fixtures]
            [cljc.java-time.offset-time :as offset-time]
            [tick.core :as t]))

(def db1 (connection/postgres-pool {:dbname "db_test"})) #_(postgres/connection {:dbname "db_test"})
(def db2 (connection/postgres-pool {:dbname "db_test2"})) #_(postgres/connection {:dbname "db_test2"})

(defn create-schema
  [db]
  (db/with-db db
    (db/execute! "DROP TABLE IF EXISTS fixtures")
    (db/execute! "DROP TYPE IF EXISTS compass")
    (db/execute! "CREATE TYPE compass AS ENUM ('north', 'south', 'east', 'west')")
    (db/execute!
     "CREATE TABLE fixtures (
        id serial PRIMARY KEY,
        varchar VARCHAR(50),
        text TEXT,
        boolean BOOLEAN default TRUE,
        smallint SMALLINT default 0,
        integer INTEGER default 0,
        bigint BIGINT default 0,
        real REAL,
        double DOUBLE PRECISION,
        date DATE,
        time TIME,
        timetz TIMETZ,
        timestamp TIMESTAMP,
        timestamptz TIMESTAMPTZ,
        enum compass,
        json JSON,
        jsonb JSONB,
        uuid UUID,
        smallint_array smallint[],
        integer_array integer[],
        bigint_array bigint[]
      )")))

(defn connect-to-database
  [f]
  (create-schema db1)
  (create-schema db2)
  (db/set-db! db1)
  (f))

(use-fixtures :once connect-to-database)

(defn submap?
  "Is map1 a submap of map2? A submap is defined as a map where all keys and values are contained
   in map2.

   (submap? {:a 1 :b 2} {:b 2})
   ;= false
   (submap? {:a 1 :b 2} {:a 1 :b 2 :c 3})
   ;= true"
  [map1 map2]
  (reduce (fn [memo [k v]]
            (and memo (= (get map2 k) v)))
          true map1))

(def fixture-1
  {:id 1
   :text "ABC"})

(def fixture-2
  {:id 2
   :text "DEF"})

(def fixtures
  [fixture-1 fixture-2])

(defn reset-fixtures!
  ([] (reset-fixtures! {}))
  ([& fixtures]
   (db/delete! :fixtures)
   (when (seq fixtures)
     (when-let [with-values (seq (filter seq fixtures))]
       (db/insert! :fixtures :values with-values))
     (when-let [without-values (seq (remove seq fixtures))]
       (doseq [_ without-values]
         (db/insert! :fixtures))))))

(deftest insert-test
  (db/set-db! db1)
  (db/delete! :fixtures)

  (testing "returning row count"
    (is (= 1 (db/insert! :fixtures)))
    (is (= 1 (db/insert! :fixtures :values [{:text "A"}])))
    (is (= 2 (db/insert! :fixtures :values [{:text "B"} {:text "C"}]))))

  (testing "returning rows (when :returning clause included)"
    (is (int? (-> (db/insert! :fixtures :returning [:id]) first :id)))
    (is (= [{:text "A"}]
           (db/insert! :fixtures :values [{:text "A"}] :returning [:text])))
    (is (= [{:text "B"} {:text "C"}]
           (db/insert! :fixtures :values [{:text "B"} {:text "C"}] :returning [:text]))))

  (testing "via namespace"
    (db/delete! :fixtures)
    (is (= 1 (fixtures/insert! :values [{:text "A"}])))
    (is (= 2 (fixtures/insert! :values [{:text "B"} {:text "C"}])))
    (is (= [{:text "D"} {:text "E"}]
           (db/insert! :fixtures :values [{:text "D"} {:text "E"}] :returning [:text])))))

(deftest update-test
  (db/set-db! db1)
  (db/delete! :fixtures)
  (db/insert! :fixtures :values [{:text "A"} {:text "B"}])

  (testing "returning row count"
    (testing "without a :set clause"
      (is (= 2 (db/update! :fixtures)))
      (is (= 2 (db/row-count :fixtures))))

    (testing "with a :set clause"
      (is (= 2 (db/update! :fixtures :set {:bigint 1})))
      (is (= 2 (db/row-count :fixtures :where [:= :bigint 1]))))

    (testing "with a :where clause"
      (is (= 1 (db/update! :fixtures :set {:bigint 99} :where [:= "A" :text])))
      (is (= 1 (db/row-count :fixtures :where [:= :bigint 99])))))

  (testing "returning rows (when :returning clause included)"
    (is (int? (-> (db/update! :fixtures :returning [:id]) first :id)))
    (is (= [{:bigint 5}]
           (db/update! :fixtures
                       :set {:bigint 5}
                       :where [:= "A" :text]
                       :returning [:bigint])))))

(deftest delete-test
  (db/set-db! db1)
  (db/delete! :fixtures)
  (db/insert! :fixtures :values [{:text "A"} {:text "B"}])

  (testing "returning row count"
    (testing "without a :set clause"
      (is (= 2 (db/update! :fixtures)))
      (is (= 2 (db/row-count :fixtures))))

    (testing "with a :set clause"
      (is (= 2 (db/update! :fixtures :set {:bigint 1})))
      (is (= 2 (db/row-count :fixtures :where [:= :bigint 1]))))

    (testing "with a :where clause"
      (is (= 1 (db/update! :fixtures :set {:bigint 99} :where [:= "A" :text])))
      (is (= 1 (db/row-count :fixtures :where [:= :bigint 99])))))

  (testing "returning rows (when :returning clause included)"
    (is (int? (-> (db/update! :fixtures :returning [:id]) first :id)))
    (is (= [{:bigint 5}]
           (db/update! :fixtures
                       :set {:bigint 5}
                       :where [:= "A" :text]
                       :returning [:bigint])))))

(deftest queries-test
  (db/set-db! db1)
  (db/delete! :fixtures)
  (db/insert! :fixtures :values [fixture-1 fixture-2])

  (testing "find-all"
    (is (= [] (db/find-all :fixtures :where [:= true false]))))

  (testing "find-first"
    (is (submap? fixture-1 (db/find-first :fixtures)))
    (is (nil? (db/find-first :fixtures :where [:= true false])))

    (is (submap? fixture-2 (db/find-first (dataset/->Row :fixtures 2)))))

  (testing "find-first!"
    (is (submap? fixture-1 (db/find-first! :fixtures)))
    (is (thrown? Exception (db/find-first! :fixtures :where [:= true false]))))

  (testing "by-id"
    (is (submap? fixture-2 (db/by-id :fixtures 2)))
    (is (nil? (db/by-id :fixtures -1))))

  (testing "by-id!"
    (is (submap? fixture-2 (db/by-id! :fixtures 2)))
    (is (thrown? Exception (db/by-id! :fixtures -1))))

  (testing "pluck-all"
    (is (= [1 2] (db/pluck-all :fixtures :id)))
    (is (= [[1 "ABC"] [2 "DEF"]] (db/pluck-all :fixtures [:id :text])))
    (is (= [] (db/pluck-all :fixtures :id :where [:= true false]))))

  (testing "pluck-first"
    (is (= 1 (db/pluck-first :fixtures :id)))
    (is (= [1 "ABC"] (db/pluck-first :fixtures [:id :text])))
    (is (nil? (db/pluck-first :fixtures :id :where [:= true false]))))

  (testing "pluck-first!"
    (is (thrown? Exception (db/pluck-first! :fixtures :id :where [:= true false]))))

  (testing "exists?"
    (is (db/exists? :fixtures))
    (is (not (db/exists? :fixtures :where [:= true false]))))

  (testing "row-count"
    (is (= 2 (db/row-count :fixtures)))
    (is (= 1 (db/row-count :fixtures :where [:= 1 :id]))))

  (testing "delete!"
    (is (= 1 (db/delete! :fixtures :where [:= 1 :id])))
    (is (not (db/exists? :fixtures :where [:= 1 :id])))))

(deftest queries-via-namespace-test
  (db/set-db! db1)
  (fixtures/delete!)
  (fixtures/insert! :values [fixture-1 fixture-2])

  (testing "find-all"
    (is (= fixtures (map #(select-keys % [:id :text]) (fixtures/find-all))))
    (is (= [] (fixtures/find-all :where [:= true false]))))

  (testing "find-first"
    (is (submap? fixture-1 (fixtures/find-first)))
    (is (nil? (fixtures/find-first :where [:= true false]))))

  (testing "find-first!"
    (is (submap? fixture-1 (fixtures/find-first!)))
    (is (thrown? Exception (fixtures/find-first! :where [:= true false]))))

  (testing "by-id"
    (is (submap? fixture-2 (fixtures/by-id 2)))
    (is (nil? (fixtures/by-id -1))))

  (testing "by-id!"
    (is (submap? fixture-2 (fixtures/by-id! 2)))
    (is (thrown? Exception (fixtures/by-id! -1))))

  (testing "pluck-all"
    (is (= [1 2] (fixtures/pluck-all :id)))
    (is (= [[1 "ABC"] [2 "DEF"]] (fixtures/pluck-all [:id :text])))
    (is (= [] (fixtures/pluck-all :id :where [:= true false]))))

  (testing "pluck-first"
    (is (= 1 (fixtures/pluck-first :id)))
    (is (= [1 "ABC"] (fixtures/pluck-first [:id :text])))
    (is (nil? (fixtures/pluck-first :id :where [:= true false]))))

  (testing "pluck-first!"
    (is (thrown? Exception (fixtures/pluck-first! :id :where [:= true false]))))

  (testing "exists?"
    (is (fixtures/exists?))
    (is (not (fixtures/exists? :where [:= true false]))))

  (testing "row-count"
    (is (= 2 (fixtures/row-count)))
    (is (= 1 (fixtures/row-count :where [:= 1 :id]))))

  (testing "update!"
    (is (= 1 (fixtures/update! :set {:text "HIJ"} :where [:= 1 :id])))
    (is (fixtures/exists? :where [:and
                                  [:= 1 :id]
                                  [:= "HIJ" :text]])))

  (testing "delete!"
    (is (= 1 (fixtures/delete! :where [:= 1 :id])))
    (is (not (fixtures/exists? :where [:= 1 :id])))))

(deftest types-test
  (db/set-db! db1)
  (db/delete! :fixtures)
  (let [id (-> (db/insert! :fixtures :returning [:id]) first :id)
        row (dataset/->Row :fixtures id)]

    (testing "text"
      (db/update! row :set {:text "JKL"})
      (is (= "JKL" (db/pluck-first! row :text))))

    (testing "varchar"
      (db/update! row :set {:varchar "QRS"})
      (is (= "QRS" (db/pluck-first! row :varchar))))

    (testing "boolean"
      (db/update! row :set {:boolean false})
      (is (= false (db/pluck-first! row :boolean)))
      (db/update! row :set {:boolean true})
      (is (= true (db/pluck-first! row :boolean))))

    (testing "smallint"
      (db/update! row :set {:smallint 32767})
      (is (= 32767 (db/pluck-first! row :smallint)))
      (db/update! row :set {:smallint -32768})
      (is (= -32768 (db/pluck-first! row :smallint))))

    (testing "integer"
      (db/update! row :set {:integer 2147483647})
      (is (= 2147483647 (db/pluck-first! row :integer)))
      (db/update! row :set {:integer -2147483648})
      (is (= -2147483648 (db/pluck-first! row :integer))))

    (testing "bigint"
      (db/update! row :set {:bigint 9223372036854775807})
      (is (= 9223372036854775807 (db/pluck-first! row :bigint)))
      (db/update! row :set {:bigint -9223372036854775808})
      (is (= -9223372036854775808 (db/pluck-first! row :bigint))))

    (testing "real"
      (db/update! row :set {:real Float/MAX_VALUE})
      (is (= Float/MAX_VALUE (db/pluck-first! row :real)))
      (db/update! row :set {:real Float/MIN_VALUE})
      (is (= Float/MIN_VALUE (db/pluck-first! row :real))))

    (testing "double"
      (db/update! row :set {:double Double/MAX_VALUE})
      (is (= Double/MAX_VALUE (db/pluck-first! row :double)))
      (db/update! row :set {:double Double/MIN_VALUE})
      (is (= Double/MIN_VALUE (db/pluck-first! row :double))))

    (testing "date"
      (let [date (t/date)]
        (db/update! row :set {:date date})
        (is (= date (db/pluck-first! row :date)))))

    (testing "time"
      (let [time (t/time)]
        (db/update! row :set {:time time})
        (is (= time (db/pluck-first! row :time)))))

    (testing "timetz"
      (let [time (offset-time/now)]
        (db/update! row :set {:timetz time})
        (is (= time (db/pluck-first! row :timetz)))))

    (testing "timestamp"
      (let [timestamp (t/date-time)]
        (db/update! row :set {:timestamp timestamp})
        (is (= timestamp (db/pluck-first! row :timestamp)))))

    (testing "timestamptz"
      (let [timestamptz (t/now)]
        (db/update! row :set {:timestamptz timestamptz})
        (is (= timestamptz (t/instant (db/pluck-first! row :timestamptz))))))

    (testing "enum"
      (db/update! row :set {:enum "north"})
      (is (= "north" (db/pluck-first! row :enum))))

    (testing "json"
      (db/update! row :set {:json [:lift {:sample "payload"}]})
      (is (= {:sample "payload"} (db/pluck-first! row :json)))
      (db/update! row :set {:json [:lift ["vector" 1]]})
      (is (= ["vector" 1] (db/pluck-first! row :json))))

    (testing "jsonb"
      (db/update! row :set {:jsonb [:lift {:sample "payload"}]})
      (is (= {:sample "payload"} (db/pluck-first! row :jsonb)))
      (db/update! row :set {:json [:lift ["vector" 1]]})
      (is (= ["vector" 1] (db/pluck-first! row :json))))

    (testing "uuid"
      (db/update! row :set {:uuid (parse-uuid "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")})
      (is (= (parse-uuid "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11") (db/pluck-first! row :uuid)))
      (db/update! row :set {:uuid "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"})
      (is (= "f81d4fae-7dec-11d0-a765-00a0c91e6bf6" (str (db/pluck-first! row :uuid)))))

    (testing "smallint[]"
      (db/update! row :set {:smallint_array (into-array Long [1 2 3 4 5])})
      (is (= [1 2 3 4 5] (db/pluck-first! row :smallint_array)))
      (db/update! row :set {:smallint_array [:lift [6 7 8 9 0]]})
      (is (= [6 7 8 9 0] (db/pluck-first! row :smallint_array))))

    (testing "integer[]"
      (db/update! row :set {:integer_array (into-array Long [1 2 3 4 5])})
      (is (= [1 2 3 4 5] (db/pluck-first! row :integer_array)))
      (db/update! row :set {:integer_array [:lift [6 7 8 9 0]]})
      (is (= [6 7 8 9 0] (db/pluck-first! row :integer_array))))

    (testing "bigint[]"
      (db/update! row :set {:bigint_array (into-array Long [1 2 3 4 5])})
      (is (= [1 2 3 4 5] (db/pluck-first! row :bigint_array)))
      (db/update! row :set {:bigint_array [:lift [6 7 8 9 0]]})
      (is (= [6 7 8 9 0] (db/pluck-first! row :bigint_array))))))

(deftest transactions-test
  (db/set-db! db1)

  (testing "commit on completion"
    (reset-fixtures!)
    (db/with-transaction
      (db/update! :fixtures :set {:text "JKL"})
      (db/update! :fixtures :set {:varchar "QRS"}))
    (is (= ["JKL" "QRS"] (db/pluck-first! :fixtures [:text :varchar]))))

  (testing "rollback on error"
    (reset-fixtures!)
    (try
      (db/with-transaction
        (db/update! :fixtures :set {:text "MNO"})
        (db/update! :fixtures :set {:varchar "XYZ"})
        (throw (ex-info "Error!" {})))
      (catch Exception _))
    (is (= [nil nil] (db/pluck-first! :fixtures [:text :varchar]))))

  (testing "explicit rollback"
    (reset-fixtures!)
    (db/with-transaction
      (db/update! :fixtures :set {:text "NO"})
      (db/rollback-transaction!)
      (db/update! :fixtures :set {:varchar "YES"}))
    (is (= [nil "YES"] (db/pluck-first! :fixtures [:text :varchar])))))

(deftest savepoints-test
  (db/set-db! db1)

  (testing "commit on completion"
    (reset-fixtures!)
    (db/with-transaction
      (db/with-savepoint
        (db/update! :fixtures :set {:text "MNO"})
        (db/update! :fixtures :set {:varchar "PQR"}))
      (db/with-savepoint
        (db/update! :fixtures :set {:enum "north"}))
      (db/update! :fixtures :set {:smallint 606}))
    (is (= ["MNO" "PQR" "north" 606] (db/pluck-first! :fixtures [:text :varchar :enum :smallint]))))

  (testing "rollback to start of savepoint"
    (reset-fixtures!)
    (db/with-transaction
      (db/with-savepoint
        (db/update! :fixtures :set {:text "STU"})
        (db/update! :fixtures :set {:varchar "VWX"}))
      (db/with-savepoint
        (db/update! :fixtures :set {:enum "east"})
        (db/rollback-to-savepoint!))
      (db/update! :fixtures :set {:smallint 707}))
    (is (= ["STU" "VWX" nil 707] (db/pluck-first! :fixtures [:text :varchar :enum :smallint]))))

  (testing "nested rollbacks"
    (reset-fixtures!)
    (db/with-transaction
      (db/with-savepoint
        (db/update! :fixtures :set {:text "ABC"})
        (db/with-savepoint
          (db/update! :fixtures :set {:varchar "DEF"})
          (db/rollback-to-savepoint!)))
      (db/with-savepoint
        (db/update! :fixtures :set {:enum "west"})
        (db/rollback-to-savepoint!))
      (db/update! :fixtures :set {:smallint 101}))
    (is (= ["ABC" nil nil 101] (db/pluck-first! :fixtures [:text :varchar :enum :smallint]))))

  (testing "rollback to named savepoint"
    (reset-fixtures!)
    (db/with-transaction
      (db/with-named-savepoint "a"
        (db/update! :fixtures :set {:text "XYZ"})
        (db/with-named-savepoint "b"
          (db/update! :fixtures :set {:varchar "ABC"})
          (db/with-named-savepoint "c"
            (db/update! :fixtures :set {:enum "west"})
            (db/rollback-to-savepoint! "b"))))
      (db/update! :fixtures :set {:smallint 202}))
    (is (= ["XYZ" nil nil 202] (db/pluck-first! :fixtures [:text :varchar :enum :smallint]))))

  (testing "implicitly opens transaction if none present"
    (reset-fixtures!)
    (db/with-named-savepoint "a"
      (db/update! :fixtures :set {:text "XYZ"})
      (db/rollback-to-savepoint!)
      (db/with-named-savepoint "b"
        (db/update! :fixtures :set {:varchar "ABC"})))
    (db/update! :fixtures :set {:smallint 202})
    (is (= [nil "ABC" nil 202] (db/pluck-first! :fixtures [:text :varchar :enum :smallint])))))

(deftest set-database-test
  (db/set-db! nil)
  (db/delete! :fixtures ::db/db db1)
  (db/delete! :fixtures ::db/db db2)

  (db/with-db db1 (db/insert! :fixtures :values [{:text "db1"}]))
  (db/with-db db2 (db/insert! :fixtures :values [{:text "db2"}]))

  (testing "database can be set"
    (testing "by passing a :warmdot.db/db option"
      (is (= "db1" (db/pluck-first! :fixtures :text ::db/db db1)))
      (is (= "db2" (db/pluck-first! :fixtures :text ::db/db db2))))

    (testing "by wrapping calls using the `with-db` macro"
      (is (= "db1" (db/with-db db1 (db/pluck-first! :fixtures :text))))
      (is (= "db2" (db/with-db db2 (db/pluck-first! :fixtures :text))))

      (testing "nested calls"
        (db/with-db db1
          (is (= "db1" (db/pluck-first! :fixtures :text)))
          (db/with-db db2
            (is (= "db2" (db/pluck-first! :fixtures :text))))
          (is (= "db1" (db/pluck-first! :fixtures :text))))))

    (testing "by calling set-db!"
      (db/set-db! db1)
      (is (= db1 (db/find-db!)))
      (is (= "db1" (db/pluck-first! :fixtures :text)))

      (db/set-db! db2)
      (is (= db2 (db/find-db!)))
      (is (= "db2" (db/pluck-first! :fixtures :text)))
      (testing "within with-db"
        (db/set-db! db1)
        (is (= db1 (db/find-db!)))
        (db/with-db db2
          (is (= db2 (db/find-db!)))
          (db/set-db! db1)
          (is (= db1 (db/find-db!)))
          (db/set-db! db2)
          (is (= db2 (db/find-db!))))
        (is (= db1 (db/find-db!)))))

    (testing "by passing a vector of database and dataset"
      (db/set-db! nil)
      (is (= "db1" (db/pluck-first! [db1 :fixtures] :text)))
      (is (= "db2" (db/pluck-first! [db2 :fixtures] :text))))))
