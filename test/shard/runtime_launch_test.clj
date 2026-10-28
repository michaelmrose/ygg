(ns shard.runtime-launch-test
  "Regression contract for the v1125/v1126 Shard-owned Clojure CLI bootstrap.

  Portable tests load the real root Shard executable without invoking its
  production -main. They call the *same* private functions used by the
  admitted worker path: source staging and alias rewriting.

  SHARD_TEST_CLOJURE_CLI=1 enables an actual, test-only Clojure CLI JVM
  session. SHARD_TEST_CLOJURE identifies the binary (default `clojure`).
  SHARD_TEST_CLOJURE_DEPS_FILE optionally provides an offline deps.edn base.
  SHARD_TEST_REQUIRE_CLOJURE_CLI=1 makes native proof mandatory; no silent
  positive result is reported if the CLI is absent. This does not certify
  production Bubblewrap admission or full Gesso suite equivalence."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [shard.runtime.jvm-session :as session])
  (:import [java.io File]
           [java.nio.file Files LinkOption Path]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent TimeUnit]))

(def ^:private attrs (make-array FileAttribute 0))
(def ^:private nofollow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private fingerprint (apply str (repeat 64 "c")))
(def ^:private session-entry ["clojure" "-M:test:formal" "-m" "shard.worker.jvm"])

(defn- root-script []
  (let [f (.getCanonicalFile (io/file (System/getProperty "user.dir") "shard"))]
    (when-not (.isFile f)
      (throw (ex-info "Run from the combined Ygg/Shard repository root"
                      {:expected (str f)})))
    f))

(defn- embedded-launch-functions []
  (let [script (root-script)
        source (str/trimr (slurp script))
        ending "(apply -main *command-line-args*)"
        ns-name (gensym "shard_runtime_launch_")
        old-property (System/getProperty "babashka.file")]
    (when-not (str/ends-with? source ending)
      (throw (ex-info "Shard entrypoint changed; refusing to evaluate CLI -main" {})))
    (try
      (System/setProperty "babashka.file" (str script))
      (binding [*ns* (create-ns ns-name)]
        (load-string (subs source 0 (- (count source) (count ending)))))
      (let [stage (ns-resolve ns-name 'stage-persistent-worker!)
            command (ns-resolve ns-name 'admitted-clojure-command!)]
        (when-not (and stage command)
          (throw (ex-info "The Shard persistent bootstrap integration seam changed" {})))
        {:stage (fn [run worker-id]
                  (let [previous (System/getProperty "babashka.file")]
                    (try
                      (System/setProperty "babashka.file" (str script))
                      (stage run worker-id)
                      (finally
                        (if previous
                          (System/setProperty "babashka.file" previous)
                          (System/clearProperty "babashka.file"))))))
         :command command})
      (finally
        (if old-property
          (System/setProperty "babashka.file" old-property)
          (System/clearProperty "babashka.file"))))))

(def ^:delay launch (delay (embedded-launch-functions)))

(defn- with-directory [f]
  (let [dir (Files/createTempDirectory "shard-runtime-launch-" attrs)]
    (try (f (.toFile dir))
         (finally
           (doseq [^File item (reverse (file-seq (.toFile dir)))]
             (Files/deleteIfExists (.toPath item)))))))

(defn- make-project! [^File directory aliases]
  (let [deps (io/file directory "deps.edn")]
    (spit deps (pr-str {:paths ["src"] :aliases aliases}))
    (.toPath deps)))

(defn- transform! [api command src deps]
  ((:command api) command (str src) deps))

(defn- rejection [f]
  (try (f) ::accepted
       (catch Exception e
         {:message (.getMessage e) :data (ex-data e)})))

(defn- overlay [command]
  (when-not (and (= 6 (count command))
                 (= "-Sdeps" (second command)))
    (throw (ex-info "Unexpected Clojure CLI launch shape" {:command command})))
  (edn/read-string (nth command 2)))

(deftest main-bearing-alias-is-cloned-without-losing-test-configuration
  (with-directory
    (fn [dir]
      (let [api @launch
            original {:extra-paths ["test"]
                      :extra-deps {'org.example/probe {:mvn/version "1.2.3"}}
                      :jvm-opts ["-Dfixture=preserved"]
                      :main-opts ["-m" "legacy.runner"]}
            formal {:jvm-opts ["-Dformal=yes"] :main-opts ["-m" "formal.runner"]}
            deps (make-project! dir {:test original :formal formal})
            output (transform! api session-entry (.getAbsolutePath dir) deps)
            aliases (get (overlay output) :aliases)]
        (is (= "clojure" (first output)))
        (is (= "-m" (nth output 4)))
        (is (= "shard.worker.jvm" (nth output 5)))
        (is (= "-M:shard.runtime/entry-0:shard.runtime/entry-1:shard.runtime/worker"
               (nth output 3)))
        (is (= (dissoc original :main-opts)
               (get aliases :shard.runtime/entry-0)))
        (is (= (dissoc formal :main-opts)
               (get aliases :shard.runtime/entry-1)))
        (is (= {:extra-paths [(.getAbsolutePath dir)]}
               (get aliases :shard.runtime/worker)))
        (is (not (some #(str/includes? % "legacy.runner") output))
            "The old -main must not appear in the JVM command")))))

(deftest aliases-without-main-opts-remain-unaltered-and-in-order
  (with-directory
    (fn [dir]
      (let [api @launch
            deps (make-project! dir {:test {:extra-paths ["test"]}
                                     :formal {:jvm-opts ["-Dformal=yes"]}})
            result (transform! api session-entry (.getAbsolutePath dir) deps)]
        (is (= "-M:test:formal:shard.runtime/worker" (nth result 3)))
        (is (= {:aliases {:shard.runtime/worker
                          {:extra-paths [(.getAbsolutePath dir)]}}}
               (overlay result)))
        (let [reversed (transform! api ["clojure" "-M:formal:test"
                                        "-m" "shard.worker.jvm"]
                                   (.getAbsolutePath dir) deps)]
          (is (= "-M:formal:test:shard.runtime/worker" (nth reversed 3))))))))

(deftest two-aliases-only-clone-main-bearing-alias
  (with-directory
    (fn [dir]
      (let [api @launch
            deps (make-project! dir {:test {:extra-paths ["test"]
                                             :main-opts ["-m" "legacy.runner"]}
                                     :formal {:jvm-opts ["-Dformal=yes"]}})
            result (transform! api session-entry (.getAbsolutePath dir) deps)
            aliases (get (overlay result) :aliases)]
        (is (= "-M:shard.runtime/entry-0:formal:shard.runtime/worker"
               (nth result 3)))
        (is (= {:extra-paths ["test"]} (:shard.runtime/entry-0 aliases)))
        (is (nil? (:shard.runtime/entry-1 aliases)))))))

(deftest reserved-names-undeclared-aliases-and-invalid-launches-refused
  (with-directory
    (fn [dir]
      (let [api @launch
            good (make-project! dir {:test {:main-opts ["-m" "legacy.runner"]}
                                          :formal {}})
            src (.getAbsolutePath dir)]
        (doseq [command [["java" "-M:test:formal" "-m" "shard.worker.jvm"]
                         ["clojure" "-M:test:formal" "-m" "other.main"]
                         ["clojure" "-M:test:formal"]
                         ["clojure" "-M:test:formal" "-e" "(println :wrong)"]
                         ["clojure" "-M" "-m" "shard.worker.jvm"]]]
          (is (map? (rejection #(transform! api command src good)))
              (str "Must reject unsupported entrypoint " command)))
        (is (map? (rejection #(transform! api
                             ["clojure" "-M:not-declared" "-m" "shard.worker.jvm"]
                             src good))))
        (is (map? (rejection #(transform! api
                             ["clojure" "-M:shard.runtime/worker" "-m"
                              "shard.worker.jvm"] src good))))
        (doseq [collision [:shard.runtime/worker :shard.runtime/entry-0]]
          (let [bad (make-project! dir {:test {:main-opts ["-m" "legacy.runner"]}
                                        :formal {}
                                        collision {}})]
            (is (map? (rejection #(transform! api session-entry src bad)))
                (str "Must refuse project-owned reserved alias " collision))))))))

(deftest unsafe-deps-and-malformed-main-options-are-rejected
  (with-directory
    (fn [dir]
      (let [api @launch
            deps (make-project! dir {:test {:main-opts ["-m" "legacy.runner"]}
                                     :formal {}})
            src (.getAbsolutePath dir)]
        (doseq [bad ["not EDN" "{:aliases 4}" "{:aliases {:test {:main-opts 7}}}"
                     "{:aliases {:test {}}}"]]
          (spit (.toFile deps) bad)
          (is (map? (rejection #(transform! api session-entry src deps)))
              (str "Must refuse unsafe project deps.edn: " bad)))
        (Files/delete deps)
        (Files/createSymbolicLink deps (.toPath (io/file dir "missing-deps.edn"))
                                (make-array java.nio.file.attribute.FileAttribute 0))
        (is (map? (rejection #(transform! api session-entry src deps))))))))

(deftest persistent-worker-staging-is-per-worker-and-byte-checked
  (with-directory
    (fn [dir]
      (let [api @launch
            first-stage ((:stage api) dir "worker-a")
            second-stage ((:stage api) dir "worker-b")
            digests (:digests first-stage)]
        (is (= (set ["shard/protocol.clj" "shard/worker/jvm.clj"])
               (set (keys digests))))
        (is (= digests (:digests second-stage)))
        (is (not= (:src first-stage) (:src second-stage)))
        (doseq [relative (keys digests)]
          (let [data (Files/readAllBytes (.toPath (io/file (:src first-stage) relative)))
                original (Files/readAllBytes (.toPath (io/file
                  (.getParentFile (root-script)) "src" relative)))
                permissions (Files/getPosixFilePermissions
                              (.toPath (io/file (:src first-stage) relative))
                              (make-array LinkOption 0))]
            (is (= (seq original) (seq data)))
            (is (not (contains? permissions java.nio.file.attribute.PosixFilePermission/OWNER_WRITE)))))
        (is (map? (rejection #((:stage api) dir "../bad"))))
        (is (map? (rejection #((:stage api) dir "worker-a")))
            "Existing staging identity must not be overwritten")))))

(defn- write-test! [project relative contents]
  (let [target (io/file project relative)]
    (.mkdirs (.getParentFile target))
    (spit target contents)))

(defn- setup-cli-project! [^File project]
  (let [base-file (System/getenv "SHARD_TEST_CLOJURE_DEPS_FILE")
        base (if base-file (edn/read-string (slurp base-file)) {:paths ["src"]})]
    (when-not (map? base)
      (throw (ex-info "SHARD_TEST_CLOJURE_DEPS_FILE must contain a deps map" {})))
    (spit (io/file project "deps.edn")
          (pr-str (-> base
                      (assoc :paths ["src"])
                      (assoc :aliases
                             (merge (:aliases base)
                                    {:test {:extra-paths ["test"]
                                            :main-opts ["-m" "launch.legacy-main"]}
                                     :formal {:jvm-opts ["-Dshard.alias.probe=retained"]}})))))
    (write-test! project "src/launch/legacy_main.clj"
      "(ns launch.legacy-main) (println \"WRONG MAIN EXECUTED\") (System/exit 51)\n")
    (write-test! project "test/launch/ok_test.clj"
      "(ns launch.ok-test (:require [clojure.test :refer :all]))\n(def ^:dynamic *in-fixture* false)\n(use-fixtures :each (fn [f] (binding [*in-fixture* true] (f))))\n(deftest one (is *in-fixture*) (is (= \"retained\" (System/getProperty \"shard.alias.probe\"))))\n")
    (write-test! project "test/launch/bad_test.clj"
      "(ns launch.bad-test (:require [clojure.test :refer :all]))\n(deftest one (is false))\n(deftest two (throw (Exception. \"expected-failure\")))\n")))

(deftest real-clojure-cli-overrides-legacy-main-and-reuses-one-jvm
  (let [enabled? (= "1" (System/getenv "SHARD_TEST_CLOJURE_CLI"))
        required? (= "1" (System/getenv "SHARD_TEST_REQUIRE_CLOJURE_CLI"))]
    (if-not (or enabled? required?)
      (println "SKIP native Clojure CLI bootstrap: set SHARD_TEST_CLOJURE_CLI=1; require with SHARD_TEST_REQUIRE_CLOJURE_CLI=1")
      (with-directory
        (fn [dir]
          (let [api @launch
                project (io/file dir "project")
                stage-dir (io/file dir "run")
                cli (or (System/getenv "SHARD_TEST_CLOJURE") "clojure")]
            (.mkdirs project)
            (.mkdirs stage-dir)
            (setup-cli-project! project)
            (let [staged ((:stage api) stage-dir "native-worker")
                  launch (transform! api
                                     ["clojure" "-M:test:formal" "-m" "shard.worker.jvm"]
                                     (:src staged) (.toPath (io/file project "deps.edn")))
                  command (assoc launch 0 cli)
                  opts {:test-only? true
                        :command command
                        :cwd (str project)
                        :control-dir (str dir)
                        :stdout-file (str (io/file dir "worker.out"))
                        :stderr-file (str (io/file dir "worker.err"))
                        :worker-id "native-worker"
                        :runtime-fingerprint fingerprint
                        :startup-timeout-ms 90000
                        :shutdown-timeout-ms 15000}
                  s (atom nil)]
              (try
                (reset! s (session/start! opts))
                (let [a (session/run! @s "ok-one" "launch.ok-test" 25000)
                      b (session/run! @s "expected-failure" "launch.bad-test" 25000)
                      c (session/run! @s "ok-two" "launch.ok-test" 25000)]
                  (is (= [:passed :failed :passed] (mapv :status [a b c])))
                  (is (= [2 2 2] (mapv :assertions [a b c])))
                  (is (= [1 1] [(:failures b) (:errors b)]))
                  (is (= (select-keys a [:tests :assertions :passes :failures :errors])
                         (select-keys c [:tests :assertions :passes :failures :errors])))
                  (is (= 1 (count (set (map :pid [a b c]))))
                      "The Clojure CLI JVM must survive an assertion failure and exception")
                  (is (= :stopped (:status (session/stop! @s))))
                  (is (not (str/includes? (slurp (io/file dir "worker.out"))
                                          "WRONG MAIN EXECUTED"))))
                (finally
                  (when (and @s (not= :stopped @(:phase @s)))
                    (session/abort! @s)))))))))))
