(ns warmdot.db.clj-kondo
  (:require [clj-kondo.hooks-api :as api]
            [warmdot.db]))

(defn- finder-partial
  [finder dataset]
  (api/list-node
   [(api/token-node 'def)
    (api/token-node (symbol (name finder)))
    (api/list-node [(api/token-node 'partial) (api/token-node finder) (api/token-node dataset)])]))

(defn define-dataset-functions [{:keys [:node]}]
  (let [[dataset] (rest (:children node))]
    (when-not dataset
      (throw (ex-info "No dataset provided" {})))
    (api/list-node
     [(api/token-node 'do)
      (map #(finder-partial % dataset) warmdot.db/query-functions)])))

(define-dataset-functions {:node (api/parse-string "(define-dataset-functions :table)")})
