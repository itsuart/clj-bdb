(ns bdb
  (:import (com.sleepycat.je Database DatabaseConfig Environment EnvironmentConfig DatabaseEntry OperationStatus LockMode))
  (:import java.io.File)
)

(defn open-or-create [env-path name]
  (let [db-env (Environment. (File. env-path) (doto (EnvironmentConfig.) (.setAllowCreate true)))]
    (.openDatabase db-env nil name (doto (DatabaseConfig.) (.setAllowCreate true)))))

(defn sync [db]
  (doto db .sync))

(defn close [db]
  (.close db))

(defn put [#^Database db key value]
  (.put db nil (DatabaseEntry. key) (DatabaseEntry. value))
  db)

(defn get [db key]
  (let [value (DatabaseEntry.)]
    (when (= OperationStatus/SUCCESS (.get db nil (DatabaseEntry. key) value LockMode/DEFAULT))
      (.getData value))))

(defn delete-record [db key]
  (.delete db nil (DatabaseEntry. key))
  db)

(defn fetch-all 
  ([db callback]
  "iterates over all db contents, calls callback on each row and put its result to array"
  (with-open [cursor (.openCursor db nil nil)]
      (fetch-all cursor callback ())))
  ([cursor callback result]
    (let [found-key (DatabaseEntry.) found-value (DatabaseEntry.)]
     (if (not (= OperationStatus/SUCCESS (.getNext cursor found-key found-value LockMode/DEFAULT))) 
       result
       (recur cursor callback (conj result (callback (.getData found-key) (.getData found-value))))))))
