(ns log-utils
  (:require [clojure.string  :as str]
            [clojure.java.io :as io]

            [cheshire.core      :as json]
            [org.httpkit.client :as http]))

(defonce counter (atom 0))
(defonce percent (atom 0))

(defn save-to-file [pth cnt]
  (spit pth cnt :append true))

(defn print-progress-bar [percent]
  (let [bar (StringBuilder. "[")]
    (doseq [i (range 50)]
      (cond (< i (int (/ percent 2))) (.append bar "=")
            (= i (int (/ percent 2))) (.append bar ">")
            :else (.append bar " ")))
    (.append bar (str "] " percent "%     "))
    (print "\r" (.toString bar))
    (flush)))

(defn count-lines [filename]
  (with-open [rdr (io/reader filename)]
    (count (line-seq rdr))))

(defn log-from-file [filename batch-size]
  (add-watch percent :percentage
             (fn [_ _ old-state new-state]
               (when (and (> new-state 0)
                          (not (= old-state new-state)))
                 (print-progress-bar new-state))))
  (with-open [rdr (io/reader filename)]
    (let [count-lines (count-lines filename)]
      (prn count-lines)
      (doseq [line (partition batch-size (line-seq rdr))]
        (try
          (let [resp @(http/post "http://localhost:10200/_bulk"
                                 {:headers {"Content-Type" "application/x-ndjson"}
                                  :body    (reduce (fn [acc v] (str acc v \newline)) "" line)})]
            (when (> (:status resp) 299)
              (fn [{:keys [error status]}]
                (if error
                  (do (println error)
                      (throw (ex-info "Http error" {})))
                  (prn status)))))
          (catch Exception _ (save-to-file "bulk_error" (str @counter \newline)))
          (finally (swap! counter inc)
                   (reset! percent (int (* (/ (* @counter batch-size) count-lines) 100)))))))
    (println @counter)
    (reset! counter 0)
    (reset! percent 0)))

(comment
  (future (log-from-file "aidbox_log.ndjson" 100))
  )

(defn delete-empty-indexes [& _]
  (let [es-url        "http://localhost:9201"
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

