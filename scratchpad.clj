(defn- find-first-by-attributes
  [dataset attributes values default-options options]
  (let [query (dataset/by (zipmap (map keyword attributes) values))]
    (find-first dataset (dataset/merge-queries query default-options options))))

(defmacro define-find-first-by
  ([name dataset attributes & {:as default-options}]
   `(do
      (defn ~name [~@(map symbol attributes) & {:as ~'options}]
        (find-first-by-attributes ~dataset ~attributes [~@(map symbol attributes)] ~default-options ~'options))
      (def ~(symbol (str name "!")) (wrap-not-found ~name)))))
