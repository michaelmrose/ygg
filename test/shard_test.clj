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

;; The regression cases below deliberately set a short MAX-IDLE-MS (rather
;; than sleeping for the production default of 60 seconds). The worker must
;; decide whether unfinished work can make progress, not simply time out.
(defn- enumerate-runtimes!
  [fx jobs]
  (let [config (file (:base fx) "config.edn")]
    (spit config
          (pr-str {:version 1
                   :project-root (.getCanonicalPath (:project fx))
                   :runtimes {:jvm {:identity "jvm-test" :capabilities #{}}
                              :node {:identity "node-test" :capabilities #{}}}
                   :jobs jobs}))
    (let [result (invoke fx "enumerate" (str config) (str (:run fx)))]
      (is (= 0 (:exit result)) (str "enumerate failed: " (:out result)))
      result)))

(defn- runtime-worker
  [fx workspace runtime idle-ms]
  (exec! (:project fx)
         [(:bb fx) (:shard fx) "worker" (str (:run fx))
          (str workspace) (name runtime) (str idle-ms)]
         {} 12000))

(defn- await-file!
  "Bounded synchronization with the prerequisite process; no timing guess."
  [path max-ms]
  (let [deadline (+ (System/nanoTime) (* 1000000 max-ms))]
    (loop []
      (if (exists? path)
        true
        (if (< (System/nanoTime) deadline)
          (do (Thread/sleep 15) (recur))
          false)))))

(defn- lifecycle-jobs
  [parent-command child-command]
  [(assoc (job "parent" parent-command) :timeout-ms 4000)
   (assoc (job "dependent" child-command ["parent"])
          :runtime :node :timeout-ms 4000)])

(deftest long-running-cross-runtime-prerequisite-does-not-retire-dependent-worker
  (with-fixture
    (fn [fx]
      (let [node-workspace (file (:base fx) "node-workspace")
            marker (file (:workspace fx) "parent-started")]
        (.mkdirs node-workspace)
        (enumerate-runtimes!
          fx (lifecycle-jobs
               ["bash" "-c" "touch parent-started; sleep 0.85; echo completed"]
               ["bash" "-c" "echo child-completed"] ))
        (let [parent (future (runtime-worker fx (:workspace fx) :jvm 100))]
          (is (await-file! marker 4000)
              "The JVM prerequisite must have started before the Node worker")
          (let [dependent (runtime-worker fx node-workspace :node 100)
                parent-result (deref parent 10000 ::deadline)
                outcomes (into {} (map (fn [[_ r]] [(:id r) r]) (results fx)))
                summary (report fx)]
            (is (map? parent-result) (pr-str parent-result))
            (is (= 0 (:exit parent-result)) (pr-str parent-result))
            (is (= 0 (:exit dependent)) (:out dependent))
            (is (str/includes? (:out dependent) "SHARD JOB dependent passed")
                (:out dependent))
            (is (not (str/includes? (:out dependent) "SHARD WORKER IDLE")))
            (is (= :passed (get-in outcomes ["parent" :status])))
            (is (= :passed (get-in outcomes ["dependent" :status])))
            (is (= 0 (:exit summary)) (:out summary))
            (is (= :pass (get-in summary [:edn :overall])))))))))

(deftest failed-cross-runtime-prerequisite-is-blocked-after-long-wait
  (with-fixture
    (fn [fx]
      (let [node-workspace (file (:base fx) "node-workspace")
            marker (file (:workspace fx) "parent-started")
            forbidden (file node-workspace "dependent-should-not-run")]
        (.mkdirs node-workspace)
        (enumerate-runtimes!
          fx (lifecycle-jobs
               ["bash" "-c" "touch parent-started; sleep 0.75; exit 23"]
               ["bash" "-c" "touch dependent-should-not-run"] ))
        (let [parent (future (runtime-worker fx (:workspace fx) :jvm 100))]
          (is (await-file! marker 4000)
              "The failing prerequisite must be running before the Node worker")
          (let [dependent (runtime-worker fx node-workspace :node 100)
                parent-result (deref parent 10000 ::deadline)
                outcomes (into {} (map (fn [[_ r]] [(:id r) r]) (results fx)))
                summary (report fx)]
            (is (= 0 (:exit parent-result)) (pr-str parent-result))
            (is (= 0 (:exit dependent)) (:out dependent))
            (is (= :failed (get-in outcomes ["parent" :status])))
            (is (= 23 (get-in outcomes ["parent" :exit-code])))
            (is (= :blocked (get-in outcomes ["dependent" :status])))
            (is (= ["parent"] (get-in outcomes ["dependent" :blocked-by])))
            (is (not (exists? forbidden))
                "A blocked dependent must never execute its command")
            (is (str/includes? (:out dependent) "SHARD JOB dependent blocked"))
            (is (= 1 (:exit summary)) (:out summary))
            (is (= :fail (get-in summary [:edn :overall])))))))))

(deftest stale-abandoned-prerequisite-does-not-hold-idle-worker-indefinitely
  (with-fixture
    (fn [fx]
      (let [node-workspace (file (:base fx) "node-workspace")
            marker (file node-workspace "should-not-run")]
        (.mkdirs node-workspace)
        (enumerate-runtimes!
          fx (lifecycle-jobs
               ["bash" "-c" "true"]
               ["bash" "-c" "touch should-not-run"]))
        (let [claim (file (:run fx) "claims" "parent")]
          (is (.mkdir claim))
          (Files/setLastModifiedTime (.toPath claim)
                                     (java.nio.file.attribute.FileTime/fromMillis
                                       (- (System/currentTimeMillis) 20000)))
          (let [dependent (runtime-worker fx node-workspace :node 120)
                summary (report fx)]
            (is (= 0 (:exit dependent)) (:out dependent))
            (is (str/includes? (:out dependent) "SHARD WORKER IDLE")
                (:out dependent))
            (is (not (exists? marker)))
            (is (not (exists? (file (:run fx) "claims" "dependent"))))
            (is (= 1 (:exit summary)) (:out summary))
            (is (= :incomplete (get-in summary [:edn :overall])))
            (is (= 1 (get-in summary [:edn :counts :claimed-without-result])))
            (is (= 0 (get-in summary [:edn :counts :passed])))))))))

(deftest fully-claimed-work-lets-another-worker-retire-without-stealing
  (with-fixture
    (fn [fx]
      (enumerate! fx [(job "claimed" ["bash" "-c" "touch should-not-run"])])
      (is (.mkdir (file (:run fx) "claims" "claimed")))
      (let [r (invoke fx "worker" (str (:run fx))
                      (str (:workspace fx)) "jvm" "5000")
            summary (report fx)]
        (is (= 0 (:exit r)) (:out r))
        (is (str/includes? (:out r) "SHARD WORKER DRAINED") (:out r))
        (is (not (exists? (file (:workspace fx) "should-not-run"))))
        (is (not (exists? (file (:run fx) "results" "claimed.edn"))))
        (is (= 1 (:exit summary)) (:out summary))
        (is (= :incomplete (get-in summary [:edn :overall])))
        (is (= 1 (get-in summary [:edn :counts :claimed-without-result])))))))


;; v1082: interruption and detached-descendant regressions for Shard v1081.
;; Run commands through the public CLI. Never signal the test runner itself.
(defn- start-cli!
  [fx cwd args env]
  (let [out (file (:base fx) (str "shard-live-" (java.util.UUID/randomUUID) ".log"))
        builder (ProcessBuilder. ^java.util.List (vec args))]
    (.directory builder (io/file cwd))
    (.redirectErrorStream builder true)
    (.redirectOutput builder (ProcessBuilder$Redirect/to out))
    (doseq [[key value] env]
      (.put (.environment builder) (name key) (str value)))
    {:process (.start builder) :log out}))

(defn- finish-cli!
  [started timeout-ms]
  (let [^Process process (:process started)
        finished? (.waitFor process (long timeout-ms) TimeUnit/MILLISECONDS)]
    (when-not finished?
      (.destroyForcibly process)
      (.waitFor process 5 TimeUnit/SECONDS))
    {:exit (if finished? (.exitValue process) :test-subprocess-timeout)
     :out (slurp (:log started))}))

(defn- terminate-cli! [started]
  (let [^Process p (:process started)]
    (when (.isAlive p) (.destroy p))
    (finish-cli! started 12000)))

(deftest interrupted-worker-kills-its-test-session-and-descendants
  (with-fixture
    (fn [fx]
      (let [marker (file (:workspace fx) "active-job-started")
            escaped (file (:workspace fx) "escaped-after-termination")]
        (enumerate! fx [(assoc (job "active"
                                     ["bash" "-c"
                                      "(sleep 1.6; touch escaped-after-termination) & touch active-job-started; wait"])
                               :timeout-ms 8000)])
        (let [started (start-cli! fx (:project fx)
                                  [(:bb fx) (:shard fx) "worker"
                                   (str (:run fx)) (str (:workspace fx)) "jvm" "100"]
                                  {})]
          (try
            (is (await-file! marker 5000) "Wait for the active test to start")
            (is (exists? (file (:run fx) "claims" "active" "session.edn"))
                "The process-group identity is recorded while the job runs")
            (let [terminated (terminate-cli! started)]
              (is (not= :test-subprocess-timeout (:exit terminated)) (:out terminated)))
            (Thread/sleep 1750)
            (is (not (exists? escaped))
                "SIGTERM must terminate the child in the detached test session")
            (is (not (exists? (file (:run fx) "results" "active.edn")))
                "Cancellation must never publish a successful test result")
            (let [summary (report fx)]
              (is (= 1 (:exit summary)) (:out summary))
              (is (= :incomplete (get-in summary [:edn :overall]))))
            (finally
              (when (.isAlive ^Process (:process started))
                (terminate-cli! started)))))))))

(deftest successful-job-reaps-background-descendants-before-reporting-pass
  (with-fixture
    (fn [fx]
      (let [escaped (file (:workspace fx) "escaped-after-success")]
        (enumerate! fx [(job "detached"
                             ["bash" "-c"
                              "(sleep 1.3; touch escaped-after-success) >/dev/null 2>&1 & echo completed"])] )
        (let [done (worker fx)
              record (get (results fx) "detached.edn")]
          (is (= 0 (:exit done)) (:out done))
          (is (= :passed (:status record)) (pr-str record))
          (is (exists? (file (:run fx) "claims" "detached" "session.edn")))
          (Thread/sleep 1450)
          (is (not (exists? escaped))
              "A successful shell must not leave background processes writing to clones")
          (is (= 0 (:exit (report fx)))))))))

(deftest interrupted-supervisor-keeps-clones-and-stops-active-job
  (with-fixture
    (fn [fx]
      (git-project! fx [(assoc (job "job0"
                                     ["bash" "-c"
                                      "(sleep 1.7; touch escaped-after-shutdown) & touch supervisor-job-started; wait"])
                               :timeout-ms 9000)])
      (let [env (ygg-shim! fx)
            cmd [(:bb fx) (:shard fx) "run" "--workers" "1"
                 "--inventory" (:bb fx) "inventory.clj"]
            started (start-cli! fx (:project fx) cmd env)
            clone (file (:project fx) ".ygg" "workers" "1")
            marker (file clone "supervisor-job-started")]
        (try
          (is (await-file! marker 7000) "Wait until supervisor's test has started")
          (let [terminated (terminate-cli! started)]
            (is (not= :test-subprocess-timeout (:exit terminated)) (:out terminated)))
          (is (exists? (file (:project fx) ".ygg" "fake-workers"))
              "The supervisor must not clean Ygg clones after interruption")
          (is (exists? clone) "The interrupted clone remains available")
          (Thread/sleep 1850)
          (is (not (exists? (file clone "escaped-after-shutdown")))
              "SIGTERM must reach worker and the test descendant")
          (let [run (file (:project fx) ".ygg" "runs" "run-fixed-mock-run")]
            (is (exists? (file run "claims" "job0" "session.edn")))
            (is (not (exists? (file run "results" "job0.edn")))
                "An interrupted job cannot be represented as passed"))
          (finally
            (when (.isAlive ^Process (:process started))
              (terminate-cli! started))))))))

(deftest invalid-test-session-evidence-forbids-supervisor-cleanup
  (with-fixture
    (fn [fx]
      ;; The Shard basket is read-only *inside* Bubblewrap. Corrupt the
      ;; evidence from the privileged host-side test harness while the job
      ;; waits inside its writable clone. Cleanup must still refuse it.
      (git-project! fx [(assoc (job "job0"
                                    ["bash" "-c"
                                     "touch session-job-ready; for i in $(seq 1 200); do test -e session-job-release && exit 0; sleep .05; done; exit 92"])
                               :timeout-ms 16000)])
      (let [env (ygg-shim! fx)
            cmd [(:bb fx) (:shard fx) "run" "--workers" "1"
                 "--inventory" (:bb fx) "inventory.clj"]
            started (start-cli! fx (:project fx) cmd env)
            run (file (:project fx) ".ygg" "runs" "run-fixed-mock-run")
            clone (file (:project fx) ".ygg" "workers" "1")
            session (file run "claims" "job0" "session.edn")]
        (try
          (is (await-file! (file clone "session-job-ready") 11000)
              "Test must be executing before host corrupts its session record")
          (is (await-file! session 3000)
              "Host can see the active session evidence")
          (when (exists? session)
            (spit session "invalid-session-data"))
          (spit (file clone "session-job-release") "release")
          (let [done (finish-cli! started 23000)]
            (is (= 2 (:exit done)) (:out done))
            (is (str/includes? (:out done) "Invalid job-session record") (:out done)))
          (is (exists? (file (:project fx) ".ygg" "fake-workers"))
              "Unsafe session evidence must prevent Ygg cleanup")
          (is (exists? clone))
          (is (exists? (file run "results" "job0.edn"))
              "Green job evidence cannot override unsafe cleanup evidence")
          (finally
            (when (.isAlive ^Process (:process started))
              (terminate-cli! started))))))))


;; v1091: Shard v1089 scoped, exactly-once preflight admission. Tests use the
;; real Shard executable and fake Gesso/Ygg CLIs. All events live outside the
;; Git checkout, so preflight cannot accidentally validate modified sources.
(def ^:private prepared-bb-shim
  "#!/usr/bin/env python3
import os, pathlib, sys, time
argv = sys.argv[1:]
if argv and os.path.realpath(argv[0]) == os.path.realpath(os.environ['SHARD_TEST_EXECUTABLE']):
    os.execv(os.environ['SHARD_TEST_REAL_BB'], [os.environ['SHARD_TEST_REAL_BB']] + argv)
root = pathlib.Path.cwd()
events = pathlib.Path(os.environ['SHARD_TEST_EVENTS'])
def emit(value):
    with events.open('a') as f:
        f.write(value + '\\n')
if len(argv) >= 2 and argv[0] == 'script/gesso_test.clj':
    mode = argv[1]
    if mode == 'preflight' and len(argv) == 2:
        emit('preflight:start')
        time.sleep(0.35)
        if os.environ.get('SHARD_TEST_PREFLIGHT_FAILURE') == '1':
            emit('preflight:failed')
            sys.exit(7)
        emit('preflight:passed')
        sys.exit(0)
    if mode == 'inventory:prepared' and len(argv) == 3:
        scope = argv[2]
        emit('inventory:start:' + scope)
        time.sleep(0.35)
        path = root / ('inventory-' + scope.replace(':', '_') + '.edn')
        if not path.is_file():
            emit('inventory:missing:' + scope)
            sys.exit(8)
        print(path.read_text(), end='')
        emit('inventory:passed:' + scope)
        sys.exit(0)
    if mode in ('gate:prepared', 'jvm:namespace:prepared') and len(argv) == 3:
        name = argv[2]
        binding = os.environ.get('GESSO_SHARD_PREFLIGHT_COMMIT')
        import subprocess
        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        emit('job:started:' + name)
        if binding != head:
            emit('job:unbound:' + name)
            sys.exit(9)
        if os.environ.get('SHARD_TEST_FAIL_TARGET') == name:
            emit('job:failed:' + name)
            sys.exit(11)
        if os.environ.get('SHARD_TEST_JOB_FAILURE') == '1':
            emit('job:failed:' + name)
            sys.exit(11)
        emit('job:passed:' + name)
        sys.exit(0)
emit('unexpected:' + repr(argv))
sys.exit(2)")

(defn- resolved-test-bb! [fx]
  (let [candidate (:bb fx)
        direct (file candidate)]
    (or (when (and (.isFile direct) (.canExecute direct))
          (.getCanonicalPath direct))
        (some (fn [directory]
                (let [p (file directory candidate)]
                  (when (and (.isFile p) (.canExecute p))
                    (.getCanonicalPath p))))
              (str/split (or (System/getenv "PATH") "")
                         (re-pattern (java.util.regex.Pattern/quote
                                       java.io.File/pathSeparator))))
        (throw (ex-info "Cannot locate Babashka for prepared Shard tests"
                        {:command candidate})))))

(defn- prepared-job [id gate target]
  (assoc (job id ["bb" "script/gesso_test.clj"
                  (if (= :jvm gate) "jvm:namespace:prepared" "gate:prepared")
                  target])
         :gate gate
         :unit (if (= :jvm gate) {:type :namespace} {:type :gate})))

(defn- prepared-gesso-project! [fx inventory-by-scope]
  (let [root (:project fx)]
    (.mkdirs (file root "script"))
    (spit (file root "script" "gesso_test.clj") ";; simulated Gesso v1088 runner\n")
    (spit (file root ".gitignore") "/.ygg/\n/.ygg.lock\n")
    (doseq [[scope jobs] inventory-by-scope]
      (spit (file root (str "inventory-" (str/replace scope ":" "_") ".edn"))
            (pr-str {:jobs jobs})))
    (run-git! fx "init" "-q")
    (run-git! fx "add" ".")
    (run-git! fx "-c" "user.name=ShardTest" "-c"
              "user.email=shard@example.invalid" "commit" "-qm" "fixture")
    root))

(defn- prepared-shims! [fx]
  (let [ygg (file (:bin fx) "ygg")
        bb (file (:bin fx) "bb")
        events (file (:base fx) "prepared-events.log")
        actual-bb (resolved-test-bb! fx)]
    (spit ygg fake-ygg)
    (is (.setExecutable ygg true))
    (spit bb prepared-bb-shim)
    (is (.setExecutable bb true))
    (spit events "")
    {:events events
     :env {"PATH" (str (.getAbsolutePath (:bin fx)) java.io.File/pathSeparator
                        (System/getenv "PATH"))
           "SHARD_TEST_EXECUTABLE" (:shard fx)
           "SHARD_TEST_REAL_BB" actual-bb
           "SHARD_TEST_EVENTS" (.getAbsolutePath events)}}))

(defn- prepared-run! [fx {:keys [env]} scope extra-env]
  (exec! (:project fx)
         (into [(:bb fx) (:shard fx) "run" "--workers" "2"]
               (when scope ["--scope" scope]))
         (merge env extra-env) 45000))

(defn- event-lines [{:keys [events]}]
  (->> (str/split-lines (slurp events))
       (remove str/blank?) vec))

(defn- prepared-run-dir [fx]
  (file (:project fx) ".ygg" "runs" "run-fixed-mock-run"))

(deftest scoped-preflight-is-once-and-overlaps-inventory-preparation
  (doseq [[scope jobs]
          [["jvm" [(prepared-job "jvm-a" :jvm "gesso.sample-a-test")
                   (prepared-job "jvm-b" :jvm "gesso.sample-b-test")]]
           ["node:browser" [(prepared-job "node-browser" :node:browser "node:browser")]]]]
    (with-fixture
      (fn [fx]
        (testing scope
          (prepared-gesso-project! fx {scope jobs})
          (let [shims (prepared-shims! fx)
                result (prepared-run! fx shims scope {})
                events (event-lines shims)
                source-file (file (prepared-run-dir fx) "source.edn")
                expected-head (str/trim (:out (exec! (:project fx)
                                                  ["git" "rev-parse" "HEAD"] {})))]
            (is (= 0 (:exit result)) (:out result))
            (is (= 1 (count (filter #{"preflight:start"} events))) events)
            (is (= 1 (count (filter #{"preflight:passed"} events))) events)
            (is (= 1 (count (filter #{(str "inventory:start:" scope)} events))) events)
            (is (= 1 (count (filter #{(str "inventory:passed:" scope)} events))) events)
            (is (< (.indexOf events "preflight:start")
                   (.indexOf events (str "inventory:passed:" scope)))
                (pr-str events))
            (is (< (.indexOf events (str "inventory:start:" scope))
                   (.indexOf events "preflight:passed"))
                (pr-str events))
            (is (= (set (map #(str "job:passed:" (last (:command %))) jobs))
                   (set (filter #(str/starts-with? % "job:passed:") events))))
            (is (not-any? #(str/starts-with? % "job:unbound:") events))
            (is (= (count jobs) (count (results (assoc fx :run (prepared-run-dir fx))))))
            (is (exists? source-file))
            (when (exists? source-file)
              (let [source (edn/read-string (slurp source-file))]
                (is (= scope (:scope source)))
                (is (= expected-head (:git-head source)))
                (is (= :passed (get-in source [:preflight :status])))
                (is (= expected-head (get-in source [:preflight :git-head])))))
            (is (not (exists? (file (:project fx) ".ygg" "fake-workers")))
                "Successful run must clean fake clones")))))))

(deftest default-gesso-run-selects-all-with-one-preflight
  (with-fixture
    (fn [fx]
      (prepared-gesso-project! fx {"all" [(prepared-job "themes" :themes "themes")]})
      (let [shims (prepared-shims! fx)
            result (prepared-run! fx shims nil {})
            events (event-lines shims)
            source (file (prepared-run-dir fx) "source.edn")]
        (is (= 0 (:exit result)) (:out result))
        (is (= 1 (count (filter #{"preflight:start"} events))) events)
        (is (= 1 (count (filter #{"inventory:start:all"} events))) events)
        (is (some #{"job:passed:themes"} events))
        (is (= "all" (:scope (edn/read-string (slurp source)))))))))

(deftest failed-preflight-even-with-forged-caller-binding-launches-no-jobs
  (with-fixture
    (fn [fx]
      (prepared-gesso-project! fx {"jvm" [(prepared-job "jvm-a" :jvm "gesso.sample-a-test")]})
      (let [shims (prepared-shims! fx)
            result (prepared-run! fx shims "jvm"
                                 {"SHARD_TEST_PREFLIGHT_FAILURE" "1"
                                  "GESSO_SHARD_PREFLIGHT_COMMIT" "forged"})
            events (event-lines shims)]
        (is (not= 0 (:exit result)) (:out result))
        (is (str/includes? (:out result) "Repository integrity failed"))
        (is (= 1 (count (filter #{"preflight:start"} events))) events)
        (is (some #{"preflight:failed"} events))
        (is (= 1 (count (filter #{"inventory:start:jvm"} events))) events)
        (is (not-any? #(str/starts-with? % "job:") events) (pr-str events))
        (is (not (exists? (file (:project fx) ".ygg" "fake-workers"))))
        (is (not (exists? (file (prepared-run-dir fx) "jobs.edn"))))))))

(deftest invalid-prepared-inventory-refuses-clones-after-one-preflight
  (with-fixture
    (fn [fx]
      (prepared-gesso-project!
        fx {"jvm" [(assoc (prepared-job "invalid" :integrity "integrity")
                           :command ["bash" "-c" "true"])]})
      (let [shims (prepared-shims! fx)
            result (prepared-run! fx shims "jvm" {})
            events (event-lines shims)]
        (is (not= 0 (:exit result)) (:out result))
        (is (str/includes? (:out result) "Scoped inventory preparation failed")
            (:out result))
        (is (= 1 (count (filter #{"preflight:start"} events))) events)
        (is (= 1 (count (filter #{"preflight:passed"} events))) events)
        (is (= 1 (count (filter #{"inventory:start:jvm"} events))) events)
        (is (not-any? #(str/starts-with? % "job:") events))
        (is (not (exists? (file (:project fx) ".ygg" "fake-workers"))))
        (is (not (exists? (file (prepared-run-dir fx) "jobs.edn"))))))))

(deftest invalid-gesso-scope-is-rejected-before-preflight
  (with-fixture
    (fn [fx]
      (prepared-gesso-project! fx {"all" [(prepared-job "themes" :themes "themes")]})
      (let [shims (prepared-shims! fx)
            result (prepared-run! fx shims "unrecognized" {})
            events (event-lines shims)]
        (is (not= 0 (:exit result)))
        (is (str/includes? (:out result) "Unsupported Gesso test scope"))
        (is (empty? events) events)
        (is (not (exists? (file (:project fx) ".ygg" "fake-workers"))))))))

(deftest prepared-job-failure-is-reported-without-skipping-preflight
  (with-fixture
    (fn [fx]
      (prepared-gesso-project! fx {"jvm" [(prepared-job "failing" :jvm "gesso.sample-a-test")]})
      (let [shims (prepared-shims! fx)
            result (prepared-run! fx shims "jvm" {"SHARD_TEST_JOB_FAILURE" "1"})
            events (event-lines shims)]
        (is (= 1 (:exit result)) (:out result))
        (is (= 1 (count (filter #{"preflight:start"} events))))
        (is (some #{"job:failed:gesso.sample-a-test"} events))
        (is (not-any? #(str/starts-with? % "job:unbound:") events))
        (is (= :failed (:status (edn/read-string
                               (slurp (file (prepared-run-dir fx) "results" "failing.edn"))))))
        (is (not (exists? (file (:project fx) ".ygg" "fake-workers"))))))))

;; v1093: Exercise the actual run supervisor for selected inventories through
;; disposable Git clones. Gesso and ZFS alone are simulated; immutable basket,
;; claiming, dependency resolution, worker subprocesses, reports and cleanup
;; remain the real Shard implementation.
(defn- scoped-run-data [fx]
  (let [run (prepared-run-dir fx)
        output (exec! (:project fx)
                      [(:bb fx) (:shard fx) "report" (str run) "--edn"] {} 12000)]
    {:run run
     :report-exit (:exit output)
     :report (when (not (str/blank? (:out output)))
               (try (edn/read-string (:out output)) (catch Exception _ nil)))
     :manifest (edn/read-string (slurp (file run "jobs.edn")))
     :source (edn/read-string (slurp (file run "source.edn")))
     :results (results (assoc fx :run run))}))

(defn- assert-scoped-completion! [fx shims jobs expected]
  (let [{:keys [run report manifest source results report-exit] :as d}
        (scoped-run-data fx)
        events (event-lines shims)
        ids (set (map :id jobs))]
    (is (= expected (:overall report)) (pr-str report))
    (is (= (count jobs) (:total report)))
    (is (= (count jobs) (:completed report)))
    (is (= ids (set (map :id (:jobs manifest)))))
    (is (= ids (set (map :id (vals results)))))
    (is (= ids (set (map #(.getName %)
                         (or (.listFiles (file run "claims")) [])))))
    (is (= (:inventory-sha256 manifest) (:inventory-sha256 report)))
    (is (= (:inventory-sha256 manifest) (:inventory-sha256 source)))
    (is (= :passed (get-in source [:preflight :status])))
    (is (= 1 (count (filter #{"preflight:start"} events))))
    (is (= 1 (count (filter #{"preflight:passed"} events))))
    (is (not-any? #(str/starts-with? % "job:unbound:") events))
    (is (not (exists? (file (:project fx) ".ygg" "fake-workers")))
        "Complete run must clean its clones")
    (is (empty? (or (.listFiles (file (:project fx) ".ygg" "workers")) [])))
    (is (= (if (= expected :pass) 0 1) report-exit))
    d))

(deftest scoped-full-suite-respects-cross-runtime-dependencies-and-cleans
  (with-fixture
    (fn [fx]
      (let [jobs [(prepared-job "jvm-a" :jvm "gesso.sample-a-test")
                  (prepared-job "jvm-b" :jvm "gesso.sample-b-test")
                  (assoc (prepared-job "node-browser" :node:browser "node:browser")
                         :runtime :node :depends-on ["jvm-a"])
                  (assoc (prepared-job "themes" :themes "themes")
                         :depends-on ["node-browser"])]]
        (prepared-gesso-project! fx {"all" jobs})
        (let [shims (prepared-shims! fx)
              response (prepared-run! fx shims "all" {})
              {:keys [manifest source report]} (assert-scoped-completion! fx shims jobs :pass)
              events (event-lines shims)]
          (is (= 0 (:exit response)) (:out response))
          (is (= "all" (:scope source)))
          (is (= #{:jvm :node} (set (map :runtime (:jobs manifest)))))
          (is (= 4 (get-in report [:counts :passed])))
          (is (every? #(exists? (file (prepared-run-dir fx) "logs"
                                     (str (:id %) ".log"))) jobs))
          (is (exists? (file (prepared-run-dir fx) "worker-1.log")))
          (is (exists? (file (prepared-run-dir fx) "worker-2.log")))
          (is (< (.indexOf events "preflight:passed")
                 (.indexOf events "job:started:gesso.sample-a-test")))
          (is (< (.indexOf events "job:passed:gesso.sample-a-test")
                 (.indexOf events "job:started:node:browser"))
              "Dependent Node gate must wait for JVM parent")
          (is (< (.indexOf events "job:passed:node:browser")
                 (.indexOf events "job:started:themes"))
              "Themes must wait for the cross-runtime dependency"))))))

(deftest scoped-jvm-run-does-not-admit-unselected-gates
  (with-fixture
    (fn [fx]
      (let [jvm (mapv (fn [n] (prepared-job (str "jvm-" n) :jvm
                                            (str "gesso.namespace-" n "-test"))) (range 5))
            others [(prepared-job "themes" :themes "themes")
                    (prepared-job "browser" :browser "browser")]]
        (prepared-gesso-project! fx {"jvm" jvm "all" (into jvm others)})
        (let [shims (prepared-shims! fx)
              response (prepared-run! fx shims "jvm" {})
              {:keys [source report]} (assert-scoped-completion! fx shims jvm :pass)
              events (event-lines shims)]
          (is (= 0 (:exit response)) (:out response))
          (is (= "jvm" (:scope source)))
          (is (= 5 (get-in report [:counts :passed])))
          (is (= 1 (count (filter #{"inventory:start:jvm"} events))))
          (is (not-any? #(or (str/includes? % "themes")
                             (str/includes? % "browser")) events)))))))

(deftest scoped-single-browser-gate-does-not-schedule-extra-build
  (with-fixture
    (fn [fx]
      (let [only [(prepared-job "integration" :browser:integration "browser:integration")]
            extra [(prepared-job "advanced" :advanced "advanced")]]
        (prepared-gesso-project! fx {"browser:integration" only "all" (into extra only)})
        (let [shims (prepared-shims! fx)
              response (prepared-run! fx shims "browser:integration" {})
              {:keys [source manifest]} (assert-scoped-completion! fx shims only :pass)
              events (event-lines shims)]
          (is (= 0 (:exit response)) (:out response))
          (is (= "browser:integration" (:scope source)))
          (is (= ["integration"] (mapv :id (:jobs manifest))))
          (is (some #{"job:passed:browser:integration"} events))
          (is (not-any? #{"job:started:advanced"} events)))))))

(deftest scoped-full-suite-accounts-for-blocked-gates-and-cleans
  (with-fixture
    (fn [fx]
      (let [jobs [(prepared-job "independent" :jvm "gesso.independent-test")
                  (assoc (prepared-job "failing" :node:browser "node:browser")
                         :runtime :node)
                  (assoc (prepared-job "dependent" :themes "themes")
                         :depends-on ["failing"])]]
        (prepared-gesso-project! fx {"all" jobs})
        (let [shims (prepared-shims! fx)
              response (prepared-run! fx shims "all"
                                      {"SHARD_TEST_FAIL_TARGET" "node:browser"})
              {:keys [results report]} (assert-scoped-completion! fx shims jobs :fail)
              events (event-lines shims)]
          (is (= 1 (:exit response)) (:out response))
          (is (= :passed (get-in results ["independent.edn" :status])))
          (is (= :failed (get-in results ["failing.edn" :status])))
          (is (= :blocked (get-in results ["dependent.edn" :status])))
          (is (= ["failing"] (get-in results ["dependent.edn" :blocked-by])))
          (is (= 1 (get-in report [:counts :failed])))
          (is (= 1 (get-in report [:counts :blocked])))
          (is (some #{"job:failed:node:browser"} events))
          (is (not-any? #{"job:started:themes"} events))
          (is (str/includes? (:out response) "SHARD CLEANED")
              "Terminal failures must not prevent safe cleanup"))))))

;; JVM timing reporting is advisory: it must never alter result validity,
;; the overall exit status, or the historical schedule's input requirements.
;; These fixtures exercise the *real* Shard report command and job logs.
(defn- timing-record [ns-name wall launch testing shutdown]
  ;; Array-map deliberately prints the required prefix first: Shard accepts
  ;; only standalone EDN records, not arbitrary fragments in ordinary output.
  (pr-str (array-map
            :gesso/jvm-timing 1
            :namespace ns-name
            :wall-ms wall
            :launch-to-testing-ms launch
            :testing-to-summary-ms testing
            :summary-to-exit-ms shutdown
            :method :stdout-milestones)))

(defn- output-command [lines exit-code]
  ["bash" "-c"
   (str "cat <<'SHARD_TIMING_FIXTURE_EOF'\n"
        (str/join "\n" lines)
        "\nSHARD_TIMING_FIXTURE_EOF\nexit " exit-code "\n")])

(defn- timed-namespace-job
  ([id ns-name lines] (timed-namespace-job id ns-name lines 0))
  ([id ns-name lines exit-code]
   (assoc (job id (output-command lines exit-code))
          :gate :jvm
          :unit {:type :gesso-jvm-namespace :name ns-name})))

(defn- timing-report! [fx]
  (let [r (report fx)]
    (is (map? (:edn r)) (:out r))
    r))

(deftest timing-summary-aggregates-only-passed-jvm-namespaces
  (with-fixture
    (fn [fx]
      (enumerate! fx
                  [(timed-namespace-job
                     "a" "gesso.a-test"
                     ["unrelated output"
                      (timing-record "gesso.a-test" 101 70 20 10)])
                   (timed-namespace-job
                     "b" "gesso.b-test"
                     [(timing-record "gesso.b-test" 202 100 80 21)])
                   (timed-namespace-job "c" "gesso.c-test" ["ordinary test output"])
                   (timed-namespace-job
                     "d" "gesso.d-test"
                     [(timing-record "gesso.other-test" 400 200 100 100)])
                   (timed-namespace-job
                     "e-failed" "gesso.e-test"
                     [(timing-record "gesso.e-test" 900 700 100 100)] 9)
                   (assoc (job "f-other-gate"
                               (output-command
                                 [(timing-record "gesso.f-test" 800 600 100 100)] 0))
                          :gate :browser
                          :unit {:type :gesso-gate :name "browser"})])
      (is (= 0 (:exit (worker fx))))
      (let [{:keys [exit edn]} (timing-report! fx)
            t (:jvm-timing edn)]
        (is (= 1 exit) "A failed job must still make the report fail")
        (is (= :fail (:overall edn)))
        (is (= 6 (:total edn)))
        (is (= 5 (get-in edn [:counts :passed])))
        (is (= 1 (get-in edn [:counts :failed])))
        (is (= 4 (:eligible t)) "Exclude both failed and unrelated jobs")
        (is (= 2 (:measured t)))
        (is (= ["c"] (:missing t)))
        (is (= ["d"] (:invalid t)))
        (is (= {:wall-ms 303
                :launch-to-testing-ms 170
                :testing-to-summary-ms 100
                :summary-to-exit-ms 31}
               (:totals-ms t)))
        (is (= ["b" "a"] (mapv :id (:slowest t))))
        (is (= ["gesso.b-test" "gesso.a-test"]
               (mapv :namespace (:slowest t))))
        (is (every? #(not (contains? (set (map :id (:slowest t))) %))
                    ["c" "d" "e-failed" "f-other-gate"]))))))

(deftest timing-summary-rejects-corrupt-duplicate-and-untrusted-evidence
  (with-fixture
    (fn [fx]
      (let [good-ns "gesso.sample-test"
            valid (timing-record good-ns 30 10 10 10)
            entries
            [["duplicate" [valid valid]]
             ["bad-edn" ["{:gesso/jvm-timing ?broken"]]
             ["negative" [(timing-record good-ns 30 -1 20 10)]]
             ["inconsistent" [(timing-record good-ns 30 10 10 5)]]
             ["wrong-method" [(str/replace valid ":stdout-milestones"
                                                    ":unsupported")]]
             ["extra-field" [(str/replace valid "}" ", :surprise true}")]]
             ["overlong" [(str valid (apply str (repeat 2050 "x")))]]
             ["non-record" [(str "prefix " valid)]]]]
        (enumerate! fx
                    (mapv (fn [[id lines]]
                            (timed-namespace-job id good-ns lines))
                          entries))
        (is (= 0 (:exit (worker fx))))
        (let [{:keys [exit edn]} (timing-report! fx)
              t (:jvm-timing edn)]
          (is (= 0 exit) "Invalid advisory evidence cannot fail passing jobs")
          (is (= :pass (:overall edn)))
          (is (= 8 (:eligible t)))
          (is (zero? (:measured t)))
          (is (= ["non-record"] (:missing t)))
          (is (= (set (map first (remove #(= "non-record" (first %)) entries)))
                 (set (:invalid t))))
          (is (zero? (get-in t [:totals-ms :wall-ms])))
          (is (empty? (:slowest t))))))))

(deftest timing-summary-orders-top-ten-by-wall-time-then-job-id
  (with-fixture
    (fn [fx]
      (let [values {"j00" 40, "j01" 500, "j02" 15, "j03" 300
                    "j04" 90, "j05" 600, "j06" 1, "j07" 300
                    "j08" 100, "j09" 5, "j10" 700, "j11" 400
                    "j12" 10}
            expected (->> values
                          (sort-by (fn [[id wall]] [(- wall) id]))
                          (take 10) (mapv first))]
        (enumerate! fx
                    (mapv (fn [[id wall]]
                            (timed-namespace-job
                              id (str "gesso." id "-test")
                              [(timing-record (str "gesso." id "-test")
                                              wall wall 0 0)]))
                          values))
        (is (= 0 (:exit (worker fx))))
        (let [{:keys [exit edn]} (timing-report! fx)
              t (:jvm-timing edn)
              human (invoke fx "report" (str (:run fx)))]
          (is (zero? exit))
          (is (= :pass (:overall edn)))
          (is (= 13 (:eligible t) (:measured t)))
          (is (empty? (:missing t)))
          (is (empty? (:invalid t)))
          (is (= (reduce + (vals values))
                 (get-in t [:totals-ms :wall-ms])))
          (is (= expected (mapv :id (:slowest t))))
          (is (= 10 (count (:slowest t))))
          (is (= ["j03" "j07"]
                 (->> (:slowest t) (filter #(= 300 (:wall-ms %)))
                      (mapv :id))))
          (is (zero? (:exit human)))
          (is (str/includes? (:out human) "JVM timing"))
          (is (str/includes? (:out human) "13 / 13 measured"))
          (is (= 5 (count (re-seq #"Slow JVM:" (:out human)))))
          (is (str/includes? (:out human) "j10"))
          (is (not (str/includes? (:out human) "Slow JVM: j02 "))))))))

(deftest timing-summary-does-not-reinterpret-incomplete-runs
  (with-fixture
    (fn [fx]
      (enumerate! fx
                  [(timed-namespace-job
                     "a-passed" "gesso.a-test"
                     [(timing-record "gesso.a-test" 23 10 10 3)])
                   (timed-namespace-job
                     "b-unfinished" "gesso.b-test"
                     [(timing-record "gesso.b-test" 100 50 40 10)])])
      (is (.mkdir (file (:run fx) "claims" "b-unfinished")))
      (is (= 0 (:exit (worker fx))))
      (let [{:keys [exit edn]} (timing-report! fx)
            t (:jvm-timing edn)]
        (is (= 1 exit))
        (is (= :incomplete (:overall edn)))
        (is (= 1 (:completed edn)))
        (is (= 1 (get-in edn [:counts :claimed-without-result])))
        (is (= 1 (:eligible t) (:measured t)))
        (is (= 23 (get-in t [:totals-ms :wall-ms])))
        (is (= ["a-passed"] (mapv :id (:slowest t))))))))

(deftest timing-reporting-is-deterministic-and-read-only
  (with-fixture
    (fn [fx]
      (enumerate! fx [(timed-namespace-job
                        "one" "gesso.one-test"
                        [(timing-record "gesso.one-test" 17 5 10 2)])])
      (is (= 0 (:exit (worker fx))))
      (let [before (slurp (file (:run fx) "results" "one.edn"))
            first (timing-report! fx)
            second (timing-report! fx)]
        (is (= 0 (:exit first) (:exit second)))
        (is (= (:edn first) (:edn second)))
        (is (= before (slurp (file (:run fx) "results" "one.edn"))))
        (is (= :pass (get-in first [:edn :overall])))
        (is (= 1 (get-in first [:edn :jvm-timing :measured])))))))

;; v1102: Bubblewrap is a mandatory execution boundary, not an optional
;; optimization. A test-only unshare shim can exercise *real* Linux net/PID
;; namespace separation when native bwrap is absent; only a native bwrap
;; installation can certify the read-only host mount view.
(defn- sandbox-script! [fx name content]
  (let [path (file (:bin fx) name)]
    (spit path content)
    (is (.setExecutable path true))
    (.getAbsolutePath path)))

(defn- sandbox-bin-env [fx]
  {"PATH" (str (.getAbsolutePath (:bin fx)) java.io.File/pathSeparator
               (System/getenv "PATH"))})

(defn- bwrap-on-test-path []
  (some (fn [directory]
          (let [candidate (file directory "bwrap")]
            (when (and (.isFile candidate) (.canExecute candidate))
              (.getAbsolutePath candidate))))
        (str/split (or (System/getenv "PATH") "")
                   (re-pattern (java.util.regex.Pattern/quote
                                 java.io.File/pathSeparator)))))

(def ^:private fake-bwrap-without-namespaces
  "#!/bin/sh\n# Mimic argument handling but refuse no namespace flags: the real\n# Shard sandbox-probe must reject an unsandboxed subprocess.\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--' ]; then shift; exec \"$@\"; fi\n  shift\ndone\nexit 98\n")

(deftest sandbox-worker-refuses-missing-or-noop-bubblewrap-before-claim
  (doseq [kind [:missing :noop]]
    (with-fixture
      (fn [fx]
        (enumerate! fx [(job "untouched" ["bash" "-c" "touch UNSANDBOXED"])])
        (when (= kind :noop)
          (sandbox-script! fx "bwrap" fake-bwrap-without-namespaces))
        (let [path (.getAbsolutePath (:bin fx))
              outcome (exec! (:project fx)
                             [(resolved-test-bb! fx) (:shard fx) "worker"
                              (str (:run fx)) (str (:workspace fx)) "jvm" "100"]
                             {"PATH" path} 18000)]
          (is (= 2 (:exit outcome)) (str kind " " (:out outcome)))
          (is (re-find #"(?i)(bubblewrap|sandbox)" (:out outcome)) (:out outcome))
          (is (empty? (or (.listFiles (file (:run fx) "claims")) [])))
          (is (empty? (or (.listFiles (file (:run fx) "results")) [])))
          (is (not (exists? (file (:workspace fx) "UNSANDBOXED")))))))))

(deftest sandbox-supervisor-refuses-noop-bubblewrap-before-clone-creation
  (with-fixture
    (fn [fx]
      (git-project! fx [(job "must-not-start"
                             ["bash" "-c" "touch UNSANDBOXED"])])
      (let [env (ygg-shim! fx)
            _ (sandbox-script! fx "bwrap" fake-bwrap-without-namespaces)
            outcome (exec! (:project fx)
                           [(:bb fx) (:shard fx) "run" "--workers" "2"
                            "--inventory" (:bb fx) "inventory.clj"]
                           env 30000)]
        (is (= 2 (:exit outcome)) (:out outcome))
        (is (str/includes? (:out outcome) "refusing unsandboxed jobs") (:out outcome))
        (is (not (exists? (file (:project fx) ".ygg" "fake-workers")))
            "Ygg clone creation must not be reached")
        (is (not (exists? (file (:project fx) ".ygg" "runs" "run-fixed-mock-run"))))
        (is (not (exists? (file (:project fx) "UNSANDBOXED"))))))))

(deftest sandbox-arguments-are-required-for-preflight-and-every-claimed-job
  (with-fixture
    (fn [fx]
      (let [actual (bwrap-on-test-path)
            audit (file (:base fx) "bwrap-audit.log")]
        (is (some? actual) "This test requires bwrap or a namespace-capable test shim")
        (when actual
          (let [escaped (str/replace (.getAbsolutePath audit) "'" "'\\''")
                launcher (str "#!/bin/sh\nprintf '%s\\n' BEGIN \"$@\" END >> '"
                              escaped "'\nexec '"
                              (str/replace actual "'" "'\\''") "' \"$@\"\n")]
            (sandbox-script! fx "bwrap" launcher)
            (enumerate! fx [(job "first" ["bash" "-c" "printf yes > first-out"])
                            (job "second" ["bash" "-c" "printf yes > second-out"])])
            (let [outcome (exec! (:project fx)
                                 [(:bb fx) (:shard fx) "worker" (str (:run fx))
                                  (str (:workspace fx)) "jvm" "100"]
                                 (sandbox-bin-env fx) 60000)
                  data (str/split-lines (slurp audit))
                  starts (count (filter #{"BEGIN"} data))
                  ends (count (filter #{"END"} data))
                  invocations (->> data
                                   (partition-by #{"BEGIN" "END"})
                                   (remove #(#{"BEGIN" "END"} (first %)))
                                   (mapv vec))
                  path (.getCanonicalPath (:workspace fx))]
              (is (= 0 (:exit outcome)) (:out outcome))
              (is (= 3 starts) "One preflight plus one sandbox per job")
              (is (= starts ends))
              (doseq [required ["--unshare-user" "--unshare-pid" "--unshare-net"
                                "--unshare-ipc" "--die-with-parent" "--ro-bind"
                                "--bind" "--proc" "--dev" "--chdir"
                                "/proc" "/dev" path]]
                (is (every? (fn [argv] (some #{required} argv)) invocations)
                    (str "Required argument on every invocation: " required)))
              (is (= 3 (count invocations)))
              (is (= 2 (count (results fx))))
              (is (every? #(= :passed (:status %)) (vals (results fx))))
              (is (exists? (file (:workspace fx) "first-out")))
              (is (exists? (file (:workspace fx) "second-out"))))))))))

(deftest sandbox-really-separates-network-pid-and-mount-namespaces
  (with-fixture
    (fn [fx]
      (enumerate! fx
                  [(job "namespace-view"
                        ["bash" "-c"
                         "readlink /proc/self/ns/net > net.id; readlink /proc/self/ns/pid > pid.id; readlink /proc/self/ns/mnt > mnt.id"])])
      (let [outcome (worker fx)]
        (is (= 0 (:exit outcome)) (:out outcome))
        (is (= :passed (get-in (results fx) ["namespace-view.edn" :status])))
        (doseq [name ["net" "pid" "mnt"]]
          (let [child (str/trim (slurp (file (:workspace fx) (str name ".id"))))
                host (str (Files/readSymbolicLink
                            (.toPath (file "/proc/self/ns" name))))]
            (is (re-matches #"[a-z]+:\[[0-9]+\]" child))
            (is (not= host child)
                (str name " namespace must differ from the parent"))))))))

(deftest sandbox-concurrent-workers-can-hold-identical-localhost-port
  (with-fixture
    (fn [fx]
      (let [other (file (:base fx) "workspace-two")
            _ (.mkdirs other)
            code (str "import pathlib,socket,time,sys\n"
                      "s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
                      "s.bind(('127.0.0.1',44937)); s.listen(1)\n"
                      "pathlib.Path('port-bound').write_text('yes')\n"
                      "limit=time.monotonic()+13\n"
                      "while not pathlib.Path('port-release').exists():\n"
                      "  if time.monotonic()>limit: sys.exit(97)\n"
                      "  time.sleep(.03)\n")]
        (enumerate! fx [(assoc (job "port-a" ["python3" "-c" code]) :timeout-ms 18000)
                        (assoc (job "port-b" ["python3" "-c" code]) :timeout-ms 18000)])
        (let [start-a (start-cli! fx (:project fx)
                                  [(:bb fx) (:shard fx) "worker" (str (:run fx))
                                   (str (:workspace fx)) "jvm" "300"] {})
              start-b (start-cli! fx (:project fx)
                                  [(:bb fx) (:shard fx) "worker" (str (:run fx))
                                   (str other) "jvm" "300"] {})]
          (try
            (let [a (await-file! (file (:workspace fx) "port-bound") 14000)
                  b (await-file! (file other "port-bound") 14000)]
              (is a "First worker must bind while other worker is running")
              (is b "Second worker must bind the identical port before release")
              (is (and a b) "Both servers must hold port 44937 simultaneously"))
            (finally
              ;; Release both servers even if an assertion fails, preventing
              ;; orphaned jobs and long waits in a failed regression.
              (spit (file (:workspace fx) "port-release") "release")
              (spit (file other "port-release") "release")
              (let [done-a (finish-cli! start-a 24000)
                    done-b (finish-cli! start-b 24000)]
                (is (= 0 (:exit done-a)) (:out done-a))
                (is (= 0 (:exit done-b)) (:out done-b)))))
          (is (= #{"port-a.edn" "port-b.edn"} (set (keys (results fx)))))
          (is (every? #(= :passed (:status %)) (vals (results fx))))
          (is (= :pass (get-in (report fx) [:edn :overall]))))))))

(deftest sandbox-private-environment-refuses-symlink-before-claim
  (with-fixture
    (fn [fx]
      (enumerate! fx [(job "never" ["bash" "-c" "touch UNSANDBOXED"])])
      (let [base (file (:workspace fx) ".ygg")
            redirection (file (:base fx) "outside")]
        (.mkdirs base)
        (.mkdirs redirection)
        (Files/createSymbolicLink
          (.toPath (file base "shard-worker-env"))
          (.toPath redirection) attrs)
        (let [outcome (worker fx)]
          (is (= 2 (:exit outcome)) (:out outcome))
          (is (str/includes? (:out outcome) "Unsafe worker environment directory")
              (:out outcome))
          (is (empty? (or (.listFiles (file (:run fx) "claims")) [])))
          (is (not (exists? (file redirection "never")))))))))

(deftest native-bubblewrap-host-mount-is-readonly-while-workspace-is-writable
  ;; A test-only unshare shim cannot simulate ro-bind. Execute this test ONLY
  ;; against a native bwrap executable; it becomes mandatory automatically
  ;; whenever bwrap is installed in a standard system location.
  (let [native (some (fn [p] (when (and (.isFile (file p))
                                         (.canExecute (file p))) p))
                     ["/usr/bin/bwrap" "/bin/bwrap"])]
    (if (nil? native)
      (println "SKIP native Bubblewrap read-only mount test: bwrap not installed")
      (with-fixture
        (fn [fx]
          (let [outside (file (:base fx) "outside")]
            (.mkdirs outside)
            (enumerate! fx
                        [(job "mount"
                              ["bash" "-c"
                               (str "if printf unsafe > '" (.getAbsolutePath
                                    (file outside "untrusted-write"))
                                    "' 2>/dev/null; then exit 51; fi; "
                                    "printf safe > inside-clone")])])
            (let [outcome (exec! (:project fx)
                                 [(:bb fx) (:shard fx) "worker" (str (:run fx))
                                  (str (:workspace fx)) "jvm" "100"]
                                 {"PATH" (str (.getParent (file native))
                                              java.io.File/pathSeparator
                                              (System/getenv "PATH"))} 30000)]
              (is (= 0 (:exit outcome)) (:out outcome))
              (is (= :passed (get-in (results fx) ["mount.edn" :status])))
              (is (exists? (file (:workspace fx) "inside-clone")))
              (is (not (exists? (file outside "untrusted-write")))))))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'shard-test)]
    (when (pos? (+ fail error)) (System/exit 1))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
