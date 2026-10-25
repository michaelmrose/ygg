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
      ;; The job deliberately tampers with its own session record after the
      ;; worker has created it. Supervisors must fail closed even if the
      ;; command and every worker exit normally.
      (git-project! fx [(job "job0"
                             ["bash" "-c"
                              "printf 'invalid-session-data' > ../../runs/run-fixed-mock-run/claims/job0/session.edn"])] )
      (let [env (ygg-shim! fx)
            cmd [(:bb fx) (:shard fx) "run" "--workers" "1"
                 "--inventory" (:bb fx) "inventory.clj"]
            done (exec! (:project fx) cmd env 45000)
            run (file (:project fx) ".ygg" "runs" "run-fixed-mock-run")]
        (is (= 2 (:exit done)) (:out done))
        (is (str/includes? (:out done) "Invalid job-session record") (:out done))
        (is (exists? (file (:project fx) ".ygg" "fake-workers"))
            "Unsafe session evidence must prevent Ygg cleanup")
        (is (exists? (file (:project fx) ".ygg" "workers" "1")))
        (is (exists? (file run "results" "job0.edn"))
            "A green worker result must not override a failed cleanup gate")))))



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

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'shard-test)]
    (when (pos? (+ fail error)) (System/exit 1))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
