(ns warmdot.db-test
  (:refer-clojure :exclude [count])
  (:require [clojure.test :refer :all]
            [warmdot.db :as db]
            [warmdot.db.connection.postgres :as postgres]
            [warmdot.db.dataset :as dataset]
            [warmdot.db.test.fixtures :as fixtures]
            [cljc.java-time.offset-time :as offset-time]
            [tick.core :as t]))

(defn connect-to-database
  [f]
  (db/with-connection (postgres/connection {:dbname "db_test"})
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
        smallint_array smallint[],
        integer_array integer[],
        bigint_array bigint[]
      )")
    (f)))

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

(deftest queries-test
  (db/delete! :fixtures)

  (testing "insert!"
    (is (= 1 (db/insert! :fixtures fixture-1)))
    (is (= 2 (db/insert! :fixtures fixture-2))))

  (testing "find-all"
    (is (= fixtures (map #(select-keys % [:id :text]) (db/find-all :fixtures))))
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

  (testing "count"
    (is (= 2 (db/row-count :fixtures)))
    (is (= 1 (db/row-count :fixtures :where [:= 1 :id]))))

  (testing "update!"
    (is (= 1 (db/update! :fixtures :set {:text "HIJ"} :where [:= 1 :id])))
    (is (db/exists? :fixtures :where [:and
                                      [:= 1 :id]
                                      [:= "HIJ" :text]])))

  (testing "delete!"
    (is (= 1 (db/delete! :fixtures :where [:= 1 :id])))
    (is (not (db/exists? :fixtures :where [:= 1 :id])))))

(deftest queries-via-namespace-test
  (fixtures/delete!)

  (testing "insert!"
    (is (= 1 (fixtures/insert! fixture-1)))
    (is (= 2 (fixtures/insert! fixture-2))))

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

  (testing "count"
    (is (= 2 (fixtures/count)))
    (is (= 1 (fixtures/count :where [:= 1 :id]))))

  (testing "update!"
    (is (= 1 (fixtures/update! :set {:text "HIJ"} :where [:= 1 :id])))
    (is (fixtures/exists? :where [:and
                                      [:= 1 :id]
                                      [:= "HIJ" :text]])))

  (testing "delete!"
    (is (= 1 (fixtures/delete! :where [:= 1 :id])))
    (is (not (fixtures/exists? :where [:= 1 :id])))))

(deftest types-test
  (let [id (db/insert! :fixtures)
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
