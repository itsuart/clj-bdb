(ns clj-bdb
  (:import (com.sleepycat.je Database DatabaseConfig Environment EnvironmentConfig DatabaseEntry OperationStatus LockMode))
  (:import java.io.File)
  (:import com.sleepycat.bind.tuple.TupleBinding)
)

;------------------ HELPERS

(defn- to-database-entry [what]
  (let [binder (TupleBinding/getPrimitiveBinding (.getClass what))]
    (if binder
       (let [result (DatabaseEntry.)] (.objectToEntry binder what result) result)
       (DatabaseEntry. what))))

;------------------ PUBLIC API

(defn open-or-create [env-path name]
  (let [db-env (Environment. (File. env-path) (doto (EnvironmentConfig.) (.setAllowCreate true)))]
    (.openDatabase db-env nil name (doto (DatabaseConfig.) (.setAllowCreate true)))))

(defn db-sync [#^Database db]
  (doto db .sync))

(defn db-close [#^Database db]
  (.close db))

(defn db-put [#^Database db key value]
  (.put db nil (to-database-entry key) (DatabaseEntry. value))
  db)

(defn db-get [#^Database db key]
  (let [value (DatabaseEntry.)]
    (when (= OperationStatus/SUCCESS (.get db nil (to-database-entry key) value LockMode/DEFAULT))
      (.getData value))))

(defn db-delete-record [#^Database db key]
  (doto db (.delete  nil (to-database-entry key))))

(defn db-fetch-all [#^Database db]
  "Lazily loads all content of the db. Returns a sequence of vectors [key value]"
  (let [found-key (DatabaseEntry.) found-value (DatabaseEntry.) cursor (.openCursor db nil nil)]
       (letfn [(get-next []
	 (if (not= OperationStatus/SUCCESS (.getNext cursor found-key found-value LockMode/DEFAULT))
	 (.close cursor)
	 (map #(.getData %) [found-key found-value])))
	 (lazy-fetch [] (lazy-seq (when-let [current (get-next)] (cons current (lazy-fetch)))))]
	 (lazy-fetch))
      ))
