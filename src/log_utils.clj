(ns log-utils
  (:require [clojure.string  :as str]
            [clojure.java.io :as io]

            [cheshire.core      :as json]
            [org.httpkit.client :as http]))

(defn log-from-file [& _]
  (let [counter (atom 0)]
    (with-open [rdr (io/reader "aidbox_log.ndjson")]
     (doseq [line (partition 2 (line-seq rdr))]
       (http/post "http://localhost:9200/_bulk"
                  {:headers {"Content-Type" "application/x-ndjson"}
                   :body    (reduce (fn [acc v] (str acc v \newline)) "" line)})
       (swap! counter inc))
     (println @counter))))

(defn delete-empty-indexes [& _]
  (let [es-url        "http://localhost:9200"
        indexes       (-> (http/get (str es-url "/_all/_settings?pretty"))
                          (deref)
                          :body
                          (json/parse-string)
                          (keys))
        empty-indexes (->> indexes
                           (map (fn [idx]
                                  (->> idx
                                       (format "%s/%s/_count" es-url)
                                       (http/get)
                                       (deref)
                                       :body
                                       (#(json/parse-string % keyword))
                                       :count
                                       (hash-map :idx idx :count))))
                           (doall)
                           (remove #(-> % :idx (str/includes? ".")))
                           (filter (comp #{0} :count))
                           (map :idx))]
    empty-indexes
    (doall 
      (map (fn [idx] (-> (http/delete (format "%s/%s" es-url idx)) 
                         (deref)
                         :body)) empty-indexes))))

(comment
   (delete-empty-indexes)
  
  )

