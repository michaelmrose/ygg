(ns shard.gesso-integration-test
  "Actual Gesso-to-persistent-JVM integration evidence, not a synthetic job list.

  SHARD_TEST_GESSO_ROOT points to a Gesso checkout. When provided, portable
  assertions examine its *executed* prepared inventory and real deps.edn.

  SHARD_TEST_GESSO_NATIVE=1 additionally starts TWO ordinary test-only Clojure
  CLI JVMs, each running real Gesso namespaces twice without restarting.
  Supply SHARD_TEST_CLOJURE_DEPS_FILE for a local/offline minimal dependency
  base; the default is Gesso's complete deps.edn (which must be resolvable).
  SHARD_TEST_GESSO_FULL_DEPS=1 requires the full checkout and full deps.edn.
  SHARD_TEST_REQUIRE_GESSO=1 makes both real inventory and native tests
  mandatory, rather than treating an unavailable host as successful proof.

  This suite never authorizes production claims. Positive Bubblewrap/ZFS
  admission and complete 138-namespace equivalence are distinct gates."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [shard.runtime.jvm-session :as session])
  (:import [java.io File]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent TimeUnit]
           [java.lang ProcessBuilder ProcessBuilder$Redirect]))

(def ^:private test-namespaces
  ["gesso.live.application-preflight.affordance-test"
   "gesso.model.command-test"])
(def ^:private digest (apply str (repeat 64 "d")))
(def ^:private attrs (make-array FileAttribute 0))

(defn- env-enabled? [name]
  (= "1" (System/getenv name)))

(defn- gesso-root []
  (let [candidate (or (System/getenv "SHARD_TEST_GESSO_ROOT")
                      (str (io/file (System/getProperty "user.dir") ".." "gesso")))
        root (.getCanonicalFile (io/file candidate))]
    (when (and (.isFile (io/file root "deps.edn"))
               (.isFile (io/file root "script/gesso_test.clj"))
               (.isDirectory (io/file root "test/gesso")))
      root)))

(defn- require-root! []
  (or (gesso-root)
      (throw (ex-info "Actual Gesso checkout required; set SHARD_TEST_GESSO_ROOT"
                      {:gate :gesso-root}))))

(defn- with-gesso [f]
  (if-let [root (gesso-root)]
    (f root)
    (if (env-enabled? "SHARD_TEST_REQUIRE_GESSO")
      (require-root!)
      (println "SKIP Gesso checkout integration: set SHARD_TEST_GESSO_ROOT and SHARD_TEST_REQUIRE_GESSO=1"))))

(defn- stop-tree! [^Process p]
  (when p
    (with-open [desc (.descendants (.toHandle p))]
      (doseq [h (iterator-seq (.iterator desc))]
        (when (.isAlive h) (.destroyForcibly h))))
    (when (.isAlive p) (.destroyForcibly p))
    (.waitFor p 10 TimeUnit/SECONDS)))

(defn- run-inventory! [^File root scope]
  (let [log (Files/createTempFile "shard-gesso-inventory-" ".log" attrs)
        binary (or (System/getenv "SHARD_TEST_BB") "bb")
        builder (doto (ProcessBuilder. ^java.util.List
                         [binary "script/gesso_test.clj" "inventory:prepared" scope])
                  (.directory root)
                  (.redirectErrorStream true)
                  (.redirectOutput (ProcessBuilder$Redirect/to (.toFile log))))]
    (try
      (let [p (.start builder)
            done? (.waitFor p 40000 TimeUnit/MILLISECONDS)
            output (slurp (.toFile log))]
        (when-not done? (stop-tree! p))
        (when-not (and done? (zero? (.exitValue p)))
          (throw (ex-info "Actual Gesso prepared inventory could not be enumerated"
                          {:scope scope :result (if done? (.exitValue p) :timeout)
                           :output (subs output 0 (min 2000 (count output)))})))
        (edn/read-string output))
      (finally (Files/deleteIfExists log)))))

(defn- job-names [inventory]
  (mapv #(get-in % [:unit :name]) (:jobs inventory)))

(deftest real-prepared-inventory-preserves-gesso-runtime-boundary
  (with-gesso
    (fn [root]
      (let [all (run-inventory! root "all")
            jvm (run-inventory! root "jvm")
            formal (run-inventory! root "formal")
            jvm-jobs (:jobs jvm)
            all-jvm (filterv #(= :gesso-jvm (:runtime %)) (:jobs all))
            standalone (remove #(= :gesso-jvm (:runtime %)) (:jobs all))
            runtime (get-in jvm [:runtimes :gesso-jvm])]
        (is (>= (count jvm-jobs) 2))
        (is (= :persistent-jvm (:execution runtime)))
        (is (= ["clojure" "-M:test:formal" "-m" "shard.worker.jvm"]
               (:session-command runtime)))
        (is (= (set jvm-jobs) (set all-jvm))
            "The full suite must not lose or replace a namespace job")
        (is (= (count jvm-jobs) (count (set (job-names jvm)))))
        (is (= (count jvm-jobs) (count (set (map :id jvm-jobs)))))
        (is (set/subset? (set (job-names formal)) (set (job-names jvm))))
        (is (every? #(and (= :gesso-jvm (:runtime %))
                          (= :gesso-jvm-namespace (get-in % [:unit :type]))
                          (= "jvm:namespace:prepared" (get-in % [:command 2]))
                          (pos? (:timeout-ms %)))
                    jvm-jobs))
        (is (every? #(and (= :gesso (:runtime %))
                          (= :gesso-gate (get-in % [:unit :type]))
                          (= "gate:prepared" (get-in % [:command 2])))
                    standalone))
        (is (every? (set (job-names jvm)) test-namespaces)
            "Chosen real smoke-test namespaces must belong to Gesso's inventory")
        (is (= :subprocess (get-in all [:runtimes :gesso :execution]))
            "Standalone gates must not be sent to persistent JVM workers")))))

(defn- root-script []
  (let [p (.getCanonicalFile (io/file (System/getProperty "user.dir") "shard"))]
    (when-not (.isFile p)
      (throw (ex-info "Run from Ygg/Shard repo root" {:expected (str p)})))
    p))

(defn- bootstrap-api []
  ;; Exercise the *same* private staging/CLI transformation used by the
  ;; production Shard worker. Loading excludes its terminal -main invocation.
  (let [script (root-script)
        content (str/trimr (slurp script))
        ending "(apply -main *command-line-args*)"
        ns-symbol (gensym "shard_gesso_native_")
        old (System/getProperty "babashka.file")]
    (when-not (str/ends-with? content ending)
      (throw (ex-info "Shard bootstrap entrypoint changed" {})))
    (try
      (System/setProperty "babashka.file" (str script))
      (binding [*ns* (create-ns ns-symbol)]
        (load-string (subs content 0 (- (count content) (count ending)))))
      (let [stage (ns-resolve ns-symbol 'stage-persistent-worker!)
            launch (ns-resolve ns-symbol 'admitted-clojure-command!)]
        (when-not (and stage launch)
          (throw (ex-info "Production staging/launch API is missing" {})))
        {:stage (fn [dir id]
                  (let [previous (System/getProperty "babashka.file")]
                    (try
                      (System/setProperty "babashka.file" (str script))
                      (stage dir id)
                      (finally
                        (if previous (System/setProperty "babashka.file" previous)
                            (System/clearProperty "babashka.file"))))))
         :launch launch})
      (finally
        (if old (System/setProperty "babashka.file" old)
            (System/clearProperty "babashka.file"))))))

(defn- delete-tree! [^File directory]
  (when (.exists directory)
    (doseq [^File f (reverse (file-seq directory))]
      (Files/deleteIfExists (.toPath f)))))

(deftest actual-gesso-deps-alias-retains-test-fixtures-and-jvm-options
  (with-gesso
    (fn [root]
      (let [dir (.toFile (Files/createTempDirectory "shard-gesso-alias-" attrs))]
        (try
          (let [api (bootstrap-api)
                src ((:stage api) dir "alias-probe")
                declared (get-in (run-inventory! root "jvm")
                                 [:runtimes :gesso-jvm :session-command])
                original (edn/read-string (slurp (io/file root "deps.edn")))
                launch ((:launch api) declared (:src src)
                        (.toPath (io/file root "deps.edn")))
                overlay (edn/read-string (nth launch 2))
                runtime (get-in overlay [:aliases :shard.runtime/worker])
                renamed (get-in overlay [:aliases :shard.runtime/entry-0])]
            (is (= ["clojure" "-Sdeps"] (subvec launch 0 2)))
            (is (= ["-m" "shard.worker.jvm"] (subvec launch 4 6)))
            (is (= "-M:shard.runtime/entry-0:formal:shard.runtime/worker"
                   (nth launch 3)))
            (is (= (dissoc (get-in original [:aliases :test]) :main-opts)
                   renamed)
                "Gesso test dependencies, source paths and JVM opts must be preserved")
            (is (not (contains? renamed :main-opts)))
            (is (= [(:src src)] (:extra-paths runtime)))
            (is (contains? (get original :aliases) :formal)))
          (finally (delete-tree! dir)))))))

(defn- source-copy! [^File source ^File dest]
  (.mkdirs (.getParentFile dest))
  (Files/copy (.toPath source) (.toPath dest)
              (into-array java.nio.file.CopyOption [])))

(def ^:private smoke-sources
  ["src/gesso/live/application_preflight/affordance.clj"
   "test/gesso/live/application_preflight/affordance_test.clj"
   "src/gesso/model/command.clj"
   "test/gesso/model/command_test.clj"])

(defn- mini-project! [^File root ^File tmp]
  (let [project (io/file tmp "project")
        dependency-file (System/getenv "SHARD_TEST_CLOJURE_DEPS_FILE")
        dependency-base (if dependency-file
                          (edn/read-string (slurp dependency-file))
                          (edn/read-string (slurp (io/file root "deps.edn"))))
        ;; A supplied offline minimal dependency base is intentionally NOT
        ;; presented as proof of full Gesso dependency-graph execution.
        test-alias (if dependency-file
                     {:extra-paths ["test"] :main-opts ["-m" "not.the.worker"]}
                     (get-in dependency-base [:aliases :test]))
        formal-alias (if dependency-file
                       {:jvm-opts ["-Dshard.gesso.native=1"]}
                       (get-in dependency-base [:aliases :formal]))]
    (when-not (and (map? dependency-base) (map? test-alias) (map? formal-alias))
      (throw (ex-info "Clojure dependency base is malformed" {})))
    (.mkdirs project)
    (doseq [relative smoke-sources]
      (let [source (io/file root relative)]
        (when-not (.isFile source)
          (throw (ex-info "Required real Gesso source missing" {:file relative})))
        (source-copy! source (io/file project relative))))
    (spit (io/file project "deps.edn")
          (pr-str (-> dependency-base
                      (assoc :paths ["src"])
                      (assoc :aliases {:test test-alias :formal formal-alias}))))
    project))

(defn- session-options [^File project ^File run id command]
  {:test-only? true
   :command command
   :cwd (str project)
   :control-dir (str run)
   :stdout-file (str (io/file run (str id ".stdout.log")))
   :stderr-file (str (io/file run (str id ".stderr.log")))
   :worker-id id
   :runtime-fingerprint digest
   :startup-timeout-ms 90000
   :shutdown-timeout-ms 15000})

(defn- real-two-jvm! [^File root]
  (let [dir (.toFile (Files/createTempDirectory "shard-real-gesso-" attrs))
        sessions (atom [])]
    (try
      (let [full? (env-enabled? "SHARD_TEST_GESSO_FULL_DEPS")
            _ (when (and full? (System/getenv "SHARD_TEST_CLOJURE_DEPS_FILE"))
                (throw (ex-info "Full dependency gate cannot use offline fixture overrides" {})))
            project (if full? root (mini-project! root dir))
            run (io/file dir "run")
            _ (.mkdirs run)
            api (bootstrap-api)
            declared (get-in (run-inventory! root "jvm") [:runtimes :gesso-jvm :session-command])
            cli (or (System/getenv "SHARD_TEST_CLOJURE") "clojure")
            start-one! (fn [id]
                         (let [stage ((:stage api) run id)
                               command ((:launch api) declared (:src stage)
                                        (.toPath (io/file project "deps.edn")))
                               command (assoc command 0 cli)
                               started (session/start! (session-options project run id command))]
                           (swap! sessions conj started)
                           started))
            a (start-one! "gesso-worker-a")
            b (start-one! "gesso-worker-b")]
        (is (not= (:pid a) (:pid b)) "Two independent live JVM processes required")
        (let [fa (future [(session/run! a "gesso-a-1" (first test-namespaces) 45000)
                          (session/run! a "gesso-a-2" (first test-namespaces) 45000)])
              fb (future [(session/run! b "gesso-b-1" (second test-namespaces) 45000)
                          (session/run! b "gesso-b-2" (second test-namespaces) 45000)])
              ar (deref fa 120000 ::timeout)
              br (deref fb 120000 ::timeout)]
          (is (and (vector? ar) (vector? br)) "Both workers must return full receipts")
          (when (and (vector? ar) (vector? br))
            (is (= [:passed :passed :passed :passed]
                   (mapv :status (concat ar br))))
            (is (every? pos? (map :tests (concat ar br))))
            (is (every? pos? (map :assertions (concat ar br))))
            (is (= (mapv #(select-keys % [:tests :assertions :passes :failures :errors])
                          ar)
                   (repeat 2 (select-keys (first ar) [:tests :assertions :passes :failures :errors]))))
            (is (= (mapv #(select-keys % [:tests :assertions :passes :failures :errors])
                          br)
                   (repeat 2 (select-keys (first br) [:tests :assertions :passes :failures :errors]))))
            (is (= #{(:pid a) (:pid b)}
                   (set (map :pid (concat ar br)))))
            (is (= #{"gesso-a-1" "gesso-a-2" "gesso-b-1" "gesso-b-2"}
                   (set (map :job-id (concat ar br))))))
        (println "LIVE-GESSO two JVM PIDs" (:pid a) (:pid b)
                 "four namespace receipts")
        (is (= :stopped (:status (session/stop! a))))
        (is (= :stopped (:status (session/stop! b))))))
      (finally
        (doseq [s @sessions]
          (when (not= :stopped @(:phase s))
            (try (session/abort! s)
                 (catch Throwable t
                   (println "JVM abort failure; preserving evidence: " (.getMessage t))))))
        (when (every? #(= :stopped @(:phase %)) @sessions)
          (delete-tree! dir))))))

(deftest actual-gesso-namespaces-run-in-two-reused-clojure-cli-jvms
  (if (or (env-enabled? "SHARD_TEST_GESSO_NATIVE")
          (env-enabled? "SHARD_TEST_REQUIRE_GESSO"))
    (real-two-jvm! (require-root!))
    (println "SKIP real Gesso JVMs: set SHARD_TEST_GESSO_NATIVE=1; require with SHARD_TEST_REQUIRE_GESSO=1")))
