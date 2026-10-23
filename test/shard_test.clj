(ns shard-test
  "Black-box regression tests for the standalone Shard executable.

  The suite exercises the actual CLI with a disposable job basket and
  independent worker processes. The orchestration test uses a fake Ygg
  executable backed by disposable Git clones; it never calls host ZFS.
  Run with `bb test` from the combined Ygg/Shard repository root."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing run-tests]])
  (:import [java.nio.file Files LinkOption]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent TimeUnit]
           [java.lang ProcessBuilder ProcessBuilder$Redirect]))

(def ^:private attrs (make-array FileAttribute 0))
(def ^:private no-follow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

(defn- file [& xs] (apply io/file xs))
(defn- exists? [f] (Files/exists (.toPath (io/file f)) no-follow))
(defn- remove-tree! [root]
  (when (exists? root)
    (with-open [paths (Files/walk (.toPath (io/file root))
                                  (make-array java.nio.file.FileVisitOption 0))]
      (doseq [p (reverse (sort-by #(.getNameCount %) (iterator-seq (.iterator paths))))]
        (Files/deleteIfExists p)))))

(defn- bb! []
  (or (System/getenv "SHARD_TEST_BB") "bb"))

(defn- shard! []
  (let [f (.getCanonicalFile (file (System/getProperty "user.dir") "shard"))]
    (when-not (.isFile f)
      (throw (ex-info "Run Shard tests from the combined Ygg/Shard repository root"
                      {:expected (str f)})))
    (str f)))

(defn- exec!
  "Bounded external process. All children are CLI commands, not test JVM internals."
  [cwd argv env & [max-ms]]
  (let [log (Files/createTempFile "shard-test-output-" ".log" attrs)
        builder (ProcessBuilder. ^java.util.List (vec argv))]
    (.directory builder (io/file cwd))
    (.redirectErrorStream builder true)
    (.redirectOutput builder (ProcessBuilder$Redirect/to (.toFile log)))
    (doseq [[key value] env]
      (.put (.environment builder) (name key) (str value)))
    (try
      (let [process (.start builder)
            finished? (.waitFor process (long (or max-ms 30000)) TimeUnit/MILLISECONDS)]
        (when-not finished?
          (.destroyForcibly process)
          (.waitFor process 5 TimeUnit/SECONDS))
        {:exit (if finished? (.exitValue process) :test-subprocess-timeout)
         :out (slurp (.toFile log))})
      (finally (Files/deleteIfExists log)))))

(defn- fixture []
  (let [base (.toFile (Files/createTempDirectory "shard-regression-" attrs))
        project (file base "project")
        workspace (file base "workspace")
        bin (file base "bin")
        run (file base "run")]
    (doseq [d [project workspace bin]] (.mkdirs d))
    {:base base :project project :workspace workspace :bin bin :run run
     :shard (shard!) :bb (bb!)}))

(defn- with-fixture [f]
  (let [fx (fixture)]
    (try (f fx)
         (finally (remove-tree! (:base fx))))))

(defn- invoke [fx & args]
  (exec! (:project fx) (into [(:bb fx) (:shard fx)] args) {}))

(defn- job
  ([id command] (job id command []))
  ([id command dependencies]
   {:id id :gate :test :runtime :jvm :unit {:type :namespace}
    :command command :depends-on dependencies
    :timeout-ms 5000 :requires #{}}))

(defn- configure! [fx jobs]
  (let [cfg (file (:base fx) "config.edn")]
    (spit cfg
          (pr-str {:version 1
                   :project-root (.getCanonicalPath (:project fx))
                   :runtimes {:jvm {:identity "jvm-for-test" :capabilities #{}}}
                   :jobs jobs}))
    cfg))

(defn- enumerate! [fx jobs]
  (let [cfg (configure! fx jobs)
        result (invoke fx "enumerate" (str cfg) (str (:run fx)))]
    (is (= 0 (:exit result)) (str "enumerate failed: " (:out result)))
    result))

(defn- read-manifest [fx]
  (edn/read-string (slurp (file (:run fx) "jobs.edn"))))

(defn- worker [fx]
  (invoke fx "worker" (str (:run fx)) (str (:workspace fx)) "jvm" "5000"))

(defn- report [fx]
  (let [result (invoke fx "report" (str (:run fx)) "--edn")]
    (assoc result :edn (when-not (str/blank? (:out result))
                         (try (edn/read-string (:out result))
                              (catch Exception _ nil))))))

(defn- results [fx]
  (into {}
        (for [f (or (.listFiles (file (:run fx) "results")) [])
              :when (str/ends-with? (.getName f) ".edn")]
          [(.getName f) (edn/read-string (slurp f))])))

(deftest enumeration-is-immutable-and-rejects-invalid-graphs
  (with-fixture
    (fn [fx]
      (let [jobs [(job "b" ["bash" "-c" "true"] ["a"])
                  (job "a" ["bash" "-c" "true"])]
            initial (enumerate! fx jobs)
            manifest (read-manifest fx)]
        (is (= 2 (count (:jobs manifest))))
        (is (= ["a" "b"] (mapv :id (:jobs manifest))))
        (is (re-matches #"[0-9a-f]{64}" (:inventory-sha256 manifest)))
        (is (every? #(.isDirectory (file (:run fx) %))
                    ["claims" "results" "logs"]))
        (is (not= 0 (:exit (invoke fx "enumerate"
                                    (str (file (:base fx) "config.edn"))
                                    (str (:run fx))))))
        (is (= (:inventory-sha256 manifest) (:inventory-sha256 (read-manifest fx)))))))
  (doseq [[label jobs]
          [["duplicate IDs" [(job "a" ["true"]) (job "a" ["true"])]]
           ["unknown dependency" [(job "a" ["true"] ["missing"])]]
           ["cyclic dependencies" [(job "a" ["true"] ["b"])
                                   (job "b" ["true"] ["a"])]]
           ["invalid job ID" [(job "../unsafe" ["true"])]]
           ["missing command" [(job "a" [])]]]]
    (with-fixture
      (fn [fx]
        (testing label
          (let [cfg (configure! fx jobs)
                r (invoke fx "enumerate" (str cfg) (str (:run fx)))]
            (is (not= 0 (:exit r)))
            (is (not (exists? (file (:run fx) "jobs.edn"))))))))))

(deftest independent-workers-claim-each-job-exactly-once
  (with-fixture
    (fn [fx]
      (enumerate! fx
                  (vec (for [n (range 1 17)]
                         (job (format "job-%02d" n)
                              ["bash" "-c" "printf ok; sleep 0.04"]))))
      (let [workers (doall (mapv (fn [_] (future (worker fx))) (range 4)))
            exits (mapv #(deref % 40000 ::deadline) workers)
            report (report fx)
            outcomes (vals (results fx))]
        (is (every? #(and (map? %) (= 0 (:exit %))) exits)
            (pr-str exits))
        (is (= 0 (:exit report)) (:out report))
        (is (= :pass (get-in report [:edn :overall])))
        (is (= 16 (count outcomes)))
        (is (= 16 (count (distinct (map :id outcomes)))))
        (is (every? #(= :passed (:status %)) outcomes))
        (is (= 16 (count (or (.listFiles (file (:run fx) "claims")) []))))
        (is (= 16 (get-in report [:edn :counts :passed])))))))

(deftest failures-timeouts-and-blocked-prerequisites-do-not-stop-workers
  (with-fixture
    (fn [fx]
      (enumerate! fx
                  [(job "a-pass" ["bash" "-c" "echo success"])
                   (job "b-fail" ["bash" "-c" "exit 17"])
                   (job "c-block" ["bash" "-c" "exit 99"] ["b-fail"])
                   (assoc (job "d-timeout" ["bash" "-c" "sleep 4"]) :timeout-ms 120)
                   (job "e-after" ["bash" "-c" "echo still-running"])])
      (let [worker-result (worker fx)
            report-result (report fx)
            outcomes (into {} (map (fn [[_ r]] [(:id r) r]) (results fx)))]
        (is (= 0 (:exit worker-result)) (:out worker-result))
        (is (= 1 (:exit report-result)))
        (is (= :fail (get-in report-result [:edn :overall])))
        (is (= :passed (get-in outcomes ["a-pass" :status])))
        (is (= :failed (get-in outcomes ["b-fail" :status])))
        (is (= 17 (get-in outcomes ["b-fail" :exit-code])))
        (is (= :blocked (get-in outcomes ["c-block" :status])))
        (is (= ["b-fail"] (get-in outcomes ["c-block" :blocked-by])))
        (is (= :timed-out (get-in outcomes ["d-timeout" :status])))
        (is (= :passed (get-in outcomes ["e-after" :status])))))))

(deftest timed-out-child-process-cannot-continue-running-in-workspace
  (with-fixture
    (fn [fx]
      (enumerate! fx [(assoc (job "a" ["bash" "-c"
                                          "(sleep 0.6; echo escaped > escaped.txt) & wait"])
                                    :timeout-ms 100)])
      (is (= 0 (:exit (worker fx))))
      (Thread/sleep 800)
      (is (not (exists? (file (:workspace fx) "escaped.txt")))
          "Timeout must kill the entire session, not just the parent shell")
      (is (= :timed-out (:status (edn/read-string
                                  (slurp (file (:run fx) "results" "a.edn")))))))))

(deftest abandoned-claim-is-reported-but-never-stolen
  (with-fixture
    (fn [fx]
      (enumerate! fx [(job "unclaimed" ["bash" "-c" "echo yes"])
                      (job "abandoned" ["bash" "-c" "echo no"])] )
      (.mkdir (file (:run fx) "claims" "abandoned"))
      (is (= 0 (:exit (worker fx))))
      (let [r (report fx)]
        (is (= 1 (:exit r)))
        (is (= :incomplete (get-in r [:edn :overall])))
        (is (= 1 (get-in r [:edn :counts :claimed-without-result])))
        (is (= 1 (get-in r [:edn :counts :passed])))
        (is (not (exists? (file (:run fx) "results" "abandoned.edn"))))))))

(deftest reporter-rejects-tampered-results-and-unsafe-artifacts
  (doseq [[label mutation]
          [["mismatched runtime"
            (fn [fx] (let [f (file (:run fx) "results" "a.edn")]
                       (spit f (pr-str (assoc (edn/read-string (slurp f))
                                              :runtime :forged)))))]
           ["bad passed exit code"
            (fn [fx] (let [f (file (:run fx) "results" "a.edn")]
                       (spit f (pr-str (assoc (edn/read-string (slurp f))
                                              :exit-code 9)))))]
           ["missing log"
            (fn [fx] (Files/delete (.toPath (file (:run fx) "logs" "a.log"))))]
           ["unexpected result"
            (fn [fx] (spit (file (:run fx) "results" "unexpected.edn") "{}"))]
           ["unclaimed result"
            (fn [fx] (remove-tree! (file (:run fx) "claims" "a")))]
           ["symlinked log"
            (fn [fx] (let [log (file (:run fx) "logs" "a.log")]
                       (Files/delete (.toPath log))
                       (Files/createSymbolicLink (.toPath log)
                                                 (.toPath (file (:run fx) "jobs.edn"))
                                                 attrs)))]]]
    (with-fixture
      (fn [fx]
        (testing label
          (enumerate! fx [(job "a" ["bash" "-c" "echo fine"])])
          (is (= 0 (:exit (worker fx))))
          (mutation fx)
          (let [r (report fx)]
            (is (= 2 (:exit r)) (str label " " (:out r)))))))))

(deftest modified-manifest-is-refused-before-claiming
  (with-fixture
    (fn [fx]
      (enumerate! fx [(job "a" ["bash" "-c" "echo original"])])
      (let [f (file (:run fx) "jobs.edn")
            manifest (edn/read-string (slurp f))
            changed (update-in manifest [:jobs 0 :command]
                               (constantly ["bash" "-c" "echo substituted"]))]
        (spit f (pr-str changed))
        (is (= 2 (:exit (worker fx)))
            "A changed job command must not execute under the old fingerprint")
        (is (= 2 (:exit (report fx)))
            "A forged manifest must not produce a successful report")
        (is (empty? (or (.listFiles (file (:run fx) "claims")) [])))))))

(deftest unfinished-result-publication-prevents-a-green-report
  (with-fixture
    (fn [fx]
      (enumerate! fx [(job "a" ["bash" "-c" "true"])])
      (is (= 0 (:exit (worker fx))))
      (let [p (file (:run fx) "results"
                    ".a.12345678-1234-1234-1234-123456789abc.tmp")]
        (spit p "{:incomplete true}")
        (let [r (report fx)]
          (is (= 1 (:exit r)))
          (is (= :incomplete (get-in r [:edn :overall])))
          (is (= 1 (count (get-in r [:edn :unpublished])))))))))

(deftest malformed-cli-invocations-refuse-without-running-tests
  (with-fixture
    (fn [fx]
      (doseq [args [["run" "--workers" "0"]
                    ["run" "--workers" "257"]
                    ["run" "--workers" "not-a-number"]
                    ["run" "--workers" "2" "--workers" "3"]
                    ["worker"] ["report"] ["enumerate"]]]
        (let [r (apply invoke fx args)]
          (is (= 2 (:exit r)) (pr-str args)))))))

(def ^:private fake-ygg
  "#!/usr/bin/env python3
import pathlib, sys, subprocess, shutil
root = pathlib.Path.cwd()
args = sys.argv[1:]
base = root / '.ygg'
state = base / 'fake-workers'
workers = base / 'workers'
if args == ['status', '--edn']:
    if state.exists():
        n = int(state.read_text())
        cs = ' '.join('{:dataset \"fake/clone%d\" :mountpoint \"%s\" :guid \"clone-guid%d\" :mounted? true}' % (i, workers / str(i), i) for i in range(1, n + 1))
        print('{:version 1 :status :ready :source-path \"%s\" :source-dataset \"fake\" :run-id \"fixed-mock-run\" :snapshot \"fake@snapshot\" :snapshot-guid \"snapshot-guid\" :clones [%s]}' % (root, cs))
    else:
        print('{:version 1 :status :idle :source-path \"%s\" :source-dataset \"fake\" :clones []}' % root)
elif 'create' in args:
    n = int(args[args.index('-n') + 1])
    workers.mkdir(parents=True, exist_ok=True)
    for i in range(1, n + 1):
        dest = workers / str(i)
        subprocess.run(['git', 'clone', '-q', '--no-local', str(root), str(dest)], check=True)
    state.write_text(str(n))
elif args == ['clean']:
    for item in workers.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
    state.unlink()
else:
    sys.exit(2)
")

(defn- run-git! [fx & args]
  (let [r (exec! (:project fx) (into ["git"] args) {})]
    (is (= 0 (:exit r)) (str args " " (:out r)))
    r))

(defn- git-project! [fx inventory]
  (let [root (:project fx)
        f (file root "inventory.clj")]
    (spit f (str "(prn " (pr-str {:jobs inventory}) ")\n"))
    (run-git! fx "init" "-q")
    (run-git! fx "add" "inventory.clj")
    (run-git! fx "-c" "user.name=ShardTest" "-c"
              "user.email=shard@example.invalid" "commit" "-qm" "fixture")
    root))

(defn- ygg-shim! [fx]
  (let [f (file (:bin fx) "ygg")
        bb-executable (file (:bb fx))]
    (spit f fake-ygg)
    (is (.setExecutable f true))
    ;; The Shard worker is launched via #!/usr/bin/env bb. When the test
    ;; caller specifies an absolute SHARD_TEST_BB, make it discoverable by
    ;; child workers as well as by the outer test process.
    (when (.isFile bb-executable)
      (Files/createSymbolicLink (.toPath (file (:bin fx) "bb"))
                                (.toPath (.getCanonicalFile bb-executable)) attrs))
    {"PATH" (str (.getAbsolutePath (:bin fx)) java.io.File/pathSeparator
                 (System/getenv "PATH"))}))

(deftest end-to-end-run-uses-verified-clones-and-reports-execution
  (doseq [[label commands expected]
          [["all-pass" [["bash" "-c" "echo good"]
                         ["bash" "-c" "echo good2"]] 0]
           ["failed-test" [["bash" "-c" "exit 3"]
                            ["bash" "-c" "echo continues"]] 1]]]
    (with-fixture
      (fn [fx]
        (testing label
          (git-project! fx (mapv (fn [i cmd] (job (str "job" i) cmd))
                                 (range) commands))
          (let [env (ygg-shim! fx)
                cmd [(:bb fx) (:shard fx) "run" "--workers" "2"
                     "--inventory" (:bb fx) "inventory.clj"]
                r (exec! (:project fx) cmd env 45000)
                run (file (:project fx) ".ygg" "runs" "run-fixed-mock-run")
                result-files (or (.listFiles (file run "results")) [])]
            (is (= expected (:exit r)) (:out r))
            (is (exists? (file run "jobs.edn")))
            (is (exists? (file run "source.edn")))
            (is (= expected (if (str/includes? (:out r) "SHARD REPORT pass") 0 1)))
            (is (not (exists? (file (:project fx) ".ygg" "fake-workers"))))
            (is (empty? (or (.listFiles (file (:project fx) ".ygg" "workers")) [])))
            (is (= 2 (count (filter #(str/ends-with? (.getName %) ".edn")
                                    result-files))))))))))

(deftest run-rejects-dirty-repository-before-creating-workspaces
  (with-fixture
    (fn [fx]
      (git-project! fx [(job "one" ["bash" "-c" "true"])])
      (spit (file (:project fx) "uncommitted.txt") "dirty")
      (let [env (ygg-shim! fx)
            r (exec! (:project fx)
                     [(:bb fx) (:shard fx) "run" "--workers" "1"
                      "--inventory" (:bb fx) "inventory.clj"] env)]
        (is (= 2 (:exit r)) (:out r))
        (is (str/includes? (:out r) "changes outside .ygg"))
        (is (not (exists? (file (:project fx) ".ygg" "fake-workers"))))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'shard-test)]
    (when (pos? (+ fail error)) (System/exit 1))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
