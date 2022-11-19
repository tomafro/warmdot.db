
(ns warmdot.db.test.fixtures
  (:refer-clojure :exclude [count])
  (:require [warmdot.db :as db]))

(db/define-dataset-functions :fixtures)
