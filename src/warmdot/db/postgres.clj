(ns warmdot.db.postgres
  (:require [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.connection]
            [next.jdbc.prepare :as prepare]
            [next.jdbc.result-set :as result-set]
            [next.jdbc.date-time])
  (:import (java.sql ResultSet ResultSetMetaData PreparedStatement Array)
           (org.postgresql.util PGobject)))

(def mapper (json/object-mapper {:decode-key-fn keyword}))
(def ->json json/write-value-as-string)
(def <-json #(json/read-value % mapper))

(defn ->json-pgobject
  [x]
  (doto (PGobject.)
    (.setType "json")
    (.setValue (->json x))))

(defn <-json-pgobject
  [^org.postgresql.util.PGobject v]
  (when-let [value (.getValue v)]
    (with-meta (<-json value) {:pgtype (.getType v)})))

(defn set-json
  [statement index value]
  (.setObject statement index (->json-pgobject value)))

(defn- set-long-array
  [statement index value]
  (.setObject statement index (into-array Long value)))

(extend-protocol prepare/SettableParameter
  clojure.lang.IPersistentMap
  (set-parameter [value ^PreparedStatement statement index]
    (condp = (.getParameterTypeName (.getParameterMetaData statement) index)
      "json" (set-json statement index value)
      "jsonb" (set-json statement index value)
      (.setObject statement index value)))

  clojure.lang.IPersistentVector
  (set-parameter [value ^PreparedStatement statement index]
    (condp = (.getParameterTypeName (.getParameterMetaData statement) index)
      "json" (set-json statement index value)
      "jsonb" (set-json statement index value)
      "_int2" (set-long-array statement index value)
      "_int4" (set-long-array statement index value)
      "_int8" (set-long-array statement index value)
      (.setObject statement index value))))

(defn column-reader
  [^ResultSet rs ^ResultSetMetaData md ^Integer i]
  (condp = (.getColumnTypeName md i)
    "date" (.getObject rs i java.time.LocalDate)
    "time" (.getObject rs i java.time.LocalTime)
    "timetz" (.getObject rs i java.time.OffsetTime)
    "timestamp" (.getObject rs i java.time.LocalDateTime)
    "timestamptz" (.getObject rs i java.time.OffsetDateTime)
    "json" (when-let [object (.getObject rs i)] (<-json-pgobject object))
    "jsonb" (when-let [object (.getObject rs i)] (<-json-pgobject object))
    (.getObject rs i)))

(extend-protocol result-set/ReadableColumn
  Array
  (read-column-by-label [^Array v _]    (vec (.getArray v)))
  (read-column-by-index [^Array v _ _]  (vec (.getArray v))))

(def connection-parameters
  {:stringtype "unspecified"})

(defn connection
  [db]
  (-> (jdbc/get-datasource (merge db {:dbtype "postgres"} connection-parameters))
      (jdbc/with-options {:builder-fn (result-set/as-maps-adapter
                                       result-set/as-unqualified-lower-maps
                                       column-reader)})))

(defn pool
  [db]
  (->  (next.jdbc.connection/->pool com.zaxxer.hikari.HikariDataSource
                                    (merge db {:dbtype "postgres"
                                               :dataSourceProperties connection-parameters}))
       (jdbc/with-options {:builder-fn (result-set/as-maps-adapter
                                        result-set/as-unqualified-lower-maps
                                        column-reader)})))
