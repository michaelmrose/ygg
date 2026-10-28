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

;; v1108: A namespace-capable test double is installed in every disposable
;; fixture, rather than implicitly depending on a host bwrap installation.
;; This launcher creates actual Linux user/net/PID/mount namespaces and
;; brings up private loopback; it does NOT claim Bubblewrap's read-only host
;; mount semantics. Native mount/Playwright acceptance remains separate.
;; The production Shard executable is never modified or bypassed.
(def ^:dynamic *fixture-bin* nil)

(def ^:private scratch-namespace-bwrap-shim
  (str "#!/usr/bin/env python3\n"
       "import os, sys, subprocess\n"
       "argv=sys.argv[1:]\n"
       "if '--' not in argv: sys.exit(91)\n"
       "for flag in ('--unshare-user','--unshare-net','--unshare-pid',\n"
       "             '--unshare-ipc','--ro-bind','--bind','--proc','--dev'):\n"
       "    if flag not in argv: sys.exit(92)\n"
       "command=argv[argv.index('--')+1:]\n"
       "if not command: sys.exit(93)\n"
       "inner = '''import os,sys,subprocess\n"
       "subprocess.run(['ip','link','set','lo','up'],check=True)\n"
       "target=os.environ.get('SHARD_TEST_SCRATCH_READONLY','')\n"
       "if target:\n"
       "    if target not in ('/tmp','/dev/shm'): sys.exit(94)\n"
       "    subprocess.run(['mount','--bind',target,target],check=True)\n"
       "    subprocess.run(['mount','-o','remount,bind,ro',target],check=True)\n"
       "os.execvp(sys.argv[1],sys.argv[1:])\n"
       "'''\n"
       "command=['unshare','--user','--map-root-user','--net','--pid',\n"
       "         '--mount','--fork','python3','-c',inner]+command\n"
       "os.execvp(command[0],command)\n"))

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
    (when *fixture-bin*
      (.put (.environment builder) "PATH"
            (str *fixture-bin* java.io.File/pathSeparator
                 (System/getenv "PATH"))))
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
    (let [launcher (file bin "bwrap")]
      (spit launcher scratch-namespace-bwrap-shim)
      (when-not (.setExecutable launcher true)
        (throw (ex-info "Cannot install namespace-capable test launcher"
                        {:path (str launcher)}))))
    {:base base :project project :workspace workspace :bin bin :run run
     :shard (shard!) :bb (bb!)}))

(defn- with-fixture [f]
  (let [fx (fixture)]
    (try (binding [*fixture-bin* (.getAbsolutePath (:bin fx))]
           (f fx))
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
    (when *fixture-bin*
      (.put (.environment builder) "PATH"
            (str *fixture-bin* java.io.File/pathSeparator
                 (System/getenv "PATH"))))
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

(def ^:private fake-bwrap-without-namespaces
  "#!/bin/sh\n# Mimic argument handling but refuse no namespace flags: the real\n# Shard sandbox-probe must reject an unsandboxed subprocess.\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--' ]; then shift; exec \"$@\"; fi\n  shift\ndone\nexit 98\n")

(deftest sandbox-worker-refuses-missing-or-noop-bubblewrap-before-claim
  (doseq [kind [:missing :noop]]
    (with-fixture
      (fn [fx]
        (enumerate! fx [(job "untouched" ["bash" "-c" "touch UNSANDBOXED"])])
        (if (= kind :noop)
          (sandbox-script! fx "bwrap" fake-bwrap-without-namespaces)
          (Files/deleteIfExists (.toPath (file (:bin fx) "bwrap"))))
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
      (let [actual (.getAbsolutePath (file (:bin fx) "bwrap-base"))
            audit (file (:base fx) "bwrap-audit.log")]
        ;; Delegate to a separate, already verified namespace-capable launcher;
        ;; instrumenting bwrap in place must never recurse into itself.
        (spit actual scratch-namespace-bwrap-shim)
        (is (.setExecutable (file actual) true))
        (when (is (.canExecute (file actual)))
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

(defn- native-bwrap-path []
  ;; SHARD_TEST_NATIVE_BWRAP permits a supplied executable to be checked
  ;; explicitly, without accidentally accepting our test-only shim.
  (let [override (some-> (System/getenv "SHARD_TEST_NATIVE_BWRAP")
                         str/trim not-empty)
        candidates (if override [override] ["/usr/bin/bwrap" "/bin/bwrap"])]
    (some (fn [candidate]
            (let [f (file candidate)]
              (when (and (.isFile f) (.canExecute f))
                (.getCanonicalPath f))))
          candidates)))

(deftest native-bubblewrap-host-mount-is-readonly-while-workspace-is-writable
  ;; A test-only unshare shim cannot simulate ro-bind. Execute this test ONLY
  ;; against a native bwrap executable; it becomes mandatory automatically
  ;; whenever bwrap is installed in a standard system location.
  (let [native (native-bwrap-path)]
    (if (nil? native)
      (if (= "1" (System/getenv "SHARD_TEST_REQUIRE_BWRAP"))
        (is false "Required native Bubblewrap is unavailable")
        (println "SKIP native Bubblewrap read-only mount test: bwrap not installed"))
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


;; v1103: private-browser localhost and SSE integration. These assets are
;; compressed copies of the pinned WebJar fixtures used by Gesso, licensed
;; under the 0BSD (Zero-Clause BSD) license; see the corresponding
;; LICENSE entries in WebJars htmx.org 2.0.7 and htmx-ext-sse 2.2.4.
;; Keep them local: network namespaces intentionally cannot reach a CDN.
(def ^:private browser-htmx-gz-b64
  (str
    "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"
    "MsjdU4THAAA="
  ))

(def ^:private browser-sse-gz-b64
  (str
    "H4sICGMK82gCA3NzZS5taW4uanMAvVZNj9s2EP0rXh0CMdEKTpAChRgWSDd7KJBtijjoZbEHrjSWCdAjlxztB7z87x3qw9Z67TZF"
    "i94kkkPOvHnzZtJliyWZBlOxvdNuVssVrR/yCpYG4fKBAH3cTLyHJNsaNFTsLEBsawXSLNPOpnSgCS7vAGnRtK4EpVrsL6rE9sQR"
    "CiGrgRZgoaTG+WLijwNqHV4n1/z4edkg8pGbJEuuK036/HAx/vt7vXl+ol+5CVmD3bMT7zPqQ3aKctKOvXh6Io6ctLE5WJL+3lC5"
    "imGWmsOPERS3sGwcXFjQ2G4uLaz5zqSI16Cqc77jFyRwqO0n9iB1QsYtrzBnZyZxR9A8w5eTM3UNrttKXdY/wmcvbNMh7rvjhc/o"
    "cQNFgk0FX2FjdQlVEoQ8vDcvo10qQo+d3Duul+zXb64pwftf+ZakMOxeCHzJCMmMYqy95Qzhfja5mOHaMhyrCwcVLxptfUGuhSDC"
    "zlynKLYcWIfDR+LQbluC37VtIcUsGdORiB533wPWRerpSkesMbsTHTZKYWvt3hv+CdEIjqDse5S1gkOU4zKpv/VHjjTwG2soZQIx"
    "KI1L47JRc2k+uNwC1rSS5s0bpkODnmatctfmJmZwnQrZr5VqWh0ciGXvxigC/5/V+W1TPV40yDRDz4iJrc4drJu73vPPxnPRgUvb"
    "rBTymeEzruCeKz93nLzixOqaOQP7B5d8DPJYDEKetp9YBvkSXhQ7WEfnVCl1rqvqpcchnCbA6uF8cOF/pkDTW3zrH19soIzAyyZn"
    "3C41v7lLGnVJoxGq3FvD3J9n78WZUpExRbIDtyfNf5bvw0ffi4zrs5PNYYOh2R3iXMmDvX+TT3csn8c94hTvKt70KspxwrFsnaAC"
    "9LU3iPfIBae+52xkhnv+Voha7tiPoDkJe+eQ9RQyHJl2vAGxhfR5w+G65nky9/USt14IdLfKAt0ZFpCNQh06Fy1r65QGnlOuq8cF"
    "8etKqaliX3z+srj8JLao8OlpLlFxGazytX5I+w+DKb5+l71996PI3opB0vD1D/O5vDdYNfe5Z2qbNTQtpZPuyfLOwYeIS+hCbDaA"
    "JyI8DO4LH500nz4mfPUKf5qP6kfK5X+04B7H1v3R2nTShbPZQRPmhb0A7LYnS4PoWiAu8bmEDzSKLkTR1Sldw40IqObhGK2dOCh9"
    "5f9C/93Aq67NduExj/2RGsgmkP7jdr0eKjFEko2teVpAdpCNQ5mAoSjoiNaBODJM0Nl02DpwFE46SpO54sp4b7COztLo7NCAZl2r"
    "H4aK2ZKbP+xj4MdZF/jNOB/sRkY/hS6OFSzN5DR6zvH6K/gNH4lpiJUcOdoze9D2BRMmyrRZmlIPwiwnQ9a3blyLi3UeycXqF2s9"
    "UpPggYa5rKDpdHI3GW6+A9SzTmN4PGIQ/gTlpH+tJQsAAA=="
  ))

(defn- write-browser-js! [target compressed]
  (let [decoded (.decode (java.util.Base64/getDecoder) compressed)]
    (with-open [input (java.util.zip.GZIPInputStream.
                       (java.io.ByteArrayInputStream. decoded))]
      (io/copy input (io/file target)))))

(def ^:private private-browser-script
"#!/usr/bin/env python3\n\"\"\"Offline Chrome + HTMX 2.0.7 + SSE 2.2.4 private-loopback fixture.\"\"\"\nimport http.server, pathlib, socketserver, subprocess, sys, time, os, urllib.request\n\nMODE = sys.argv[1]\nPORT = 44948\nROOT = pathlib.Path.cwd()\nHTML = r'''<!DOCTYPE html>\n<html><head><title>private-localhost</title>\n<script src=\"/htmx.js\"></script><script src=\"/sse.js\"></script></head>\n<body>\n<button id=\"request\" hx-get=\"/fragment\" hx-target=\"#fragment\" hx-swap=\"innerHTML\">load</button>\n<div id=\"fragment\">not-loaded</div>\n<div hx-ext=\"sse\" sse-connect=\"/events\"><div id=\"events\" sse-swap=\"tick\">waiting</div></div>\n<script>\nwindow.addEventListener('load', function () { document.getElementById('request').click(); });\nvar poll = setInterval(function () {\n  if (window.htmx && document.getElementById('fragment').textContent.includes('htmx-ok') &&\n      document.getElementById('events').textContent.includes('sse-ok')) {\n    document.documentElement.setAttribute('data-browser-result', 'pass'); clearInterval(poll);\n  }\n}, 60);\n</script>\n</body></html>'''\n\nclass Handler(http.server.BaseHTTPRequestHandler):\n    def log_message(self, *args): pass\n    def do_GET(self):\n        if self.path == '/events':\n            self.send_response(200)\n            self.send_header('Content-Type','text/event-stream')\n            self.send_header('Cache-Control','no-cache')\n            self.end_headers()\n            try:\n                self.wfile.write(b'event: tick\\ndata: <span id=\"received-sse\">sse-ok</span>\\n\\n')\n                self.wfile.flush()\n                time.sleep(.2)\n            except (BrokenPipeError, ConnectionResetError): pass\n            return\n        if self.path == '/': data, ctype = HTML.encode(), 'text/html'\n        elif self.path == '/htmx.js': data, ctype = (ROOT/'htmx.js').read_bytes(), 'application/javascript'\n        elif self.path == '/sse.js': data, ctype = (ROOT/'sse.js').read_bytes(), 'application/javascript'\n        elif self.path == '/fragment': data, ctype = b'<strong id=\"received-fragment\">htmx-ok</strong>', 'text/html'\n        else:\n            self.send_error(404); return\n        self.send_response(200)\n        self.send_header('Content-Type', ctype)\n        self.send_header('Content-Length', str(len(data)))\n        self.end_headers()\n        self.wfile.write(data)\n\nclass Server(socketserver.ThreadingMixIn, http.server.HTTPServer):\n    daemon_threads=True\n    allow_reuse_address=False\n\nserver=Server(('127.0.0.1',PORT),Handler)\nimport threading\nthreading.Thread(target=server.serve_forever,daemon=True).start()\n(ROOT/'http-ready').write_text('ready')\ntry:\n    limit=time.monotonic()+22\n    while not (ROOT/'http-go').exists():\n        if time.monotonic()>limit: raise TimeoutError('barrier not released')\n        time.sleep(.04)\n    base='http://127.0.0.1:%s'%PORT\n    if MODE=='transport':\n        for endpoint,expected in [('/',b'hx-get'),('/htmx.js',b'htmx'),('/sse.js',b'htmx'),('/fragment',b'htmx-ok'),('/events',b'sse-ok')]:\n            with urllib.request.urlopen(base+endpoint,timeout=5) as r:\n                result=r.read(100000)\n                assert expected in result, (endpoint,result[:200])\n        (ROOT/'http-verified').write_text('transport-ok')\n    elif MODE=='browser':\n        chrome=os.getenv('SHARD_TEST_CHROMIUM','chromium')\n        args=[chrome,'--headless=new','--no-sandbox','--disable-gpu','--disable-dev-shm-usage',\n              '--no-first-run','--no-default-browser-check','--disable-background-networking',\n              '--disable-features=MediaRouter','--user-data-dir='+str(ROOT/'chrome-profile'),\n              '--virtual-time-budget=7000','--dump-dom',base+'/']\n        completed=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=22)\n        dom=completed.stdout.decode('utf-8','replace')\n        if completed.returncode != 0 or 'data-browser-result=\"pass\"' not in dom:\n            print('CHROMIUM FAILED',completed.returncode,dom[-600:],completed.stderr.decode('utf-8','replace')[-900:],flush=True)\n            raise RuntimeError('HTMX swap and SSE browser update not observed')\n        (ROOT/'http-verified').write_text('browser-htmx-sse-ok')\n    else: raise ValueError('unsupported mode: '+MODE)\n    print('SHARD HTMX SSE OK',MODE,flush=True)\nfinally:\n    server.shutdown(); server.server_close()\n")

(defn- browser-fixture-files! [workspace]
  (spit (file workspace "private-browser-fixture.py") private-browser-script)
  (write-browser-js! (file workspace "htmx.js") browser-htmx-gz-b64)
  (write-browser-js! (file workspace "sse.js") browser-sse-gz-b64))

(defn- browser-integration-two-workers! [fx mode]
  (let [other (file (:base fx) "workspace-browser-two")
        _ (.mkdirs other)
        workspaces [(:workspace fx) other]
        _ (doseq [workspace workspaces] (browser-fixture-files! workspace))
        jobs [(assoc (job "browser-a" ["python3" "private-browser-fixture.py" mode])
                     :timeout-ms 42000)
              (assoc (job "browser-b" ["python3" "private-browser-fixture.py" mode])
                     :timeout-ms 42000)]]
    (enumerate! fx jobs)
    (let [started (mapv (fn [workspace]
                          (start-cli! fx (:project fx)
                                      [(:bb fx) (:shard fx) "worker"
                                       (str (:run fx)) (str workspace) "jvm" "200"] {}))
                        workspaces)]
      (try
        (let [ready (mapv #(await-file! (file % "http-ready") 16000) workspaces)]
          (is (every? true? ready)
              (str "Both namespaced servers must bind TCP port 44948 simultaneously: " ready))
          (when (every? true? ready)
            (doseq [workspace workspaces]
              (spit (file workspace "http-go") "go"))))
        (finally
          ;; Always release both servers so test failure cannot strand workers.
          (doseq [workspace workspaces]
            (spit (file workspace "http-go") "go"))
          (doseq [started-worker started]
            (let [done (finish-cli! started-worker 48000)]
              (is (= 0 (:exit done)) (:out done))))))
      (let [outcomes (results fx)]
        (is (= #{"browser-a.edn" "browser-b.edn"} (set (keys outcomes))))
        (is (every? #(= :passed (:status %)) (vals outcomes)) (pr-str outcomes))
        (is (every? #(exists? (file % "http-verified")) workspaces))
        (doseq [workspace workspaces]
          (when (exists? (file workspace "http-verified"))
            (is (= (if (= mode "browser") "browser-htmx-sse-ok" "transport-ok")
                   (slurp (file workspace "http-verified"))))))
        (is (= :pass (get-in (report fx) [:edn :overall])))))))

(deftest sandbox-private-localhost-simultaneous-http-htmx-and-sse-transport
  (with-fixture
    (fn [fx]
      (browser-integration-two-workers! fx "transport"))))

;; v1105: real Gesso Playwright is the authoritative browser integration gate.
;; The v1103 standalone Chromium fixture only probes the transport layer.
;; This deliberately opt-in, *native* Bubblewrap test checks Gesso's real
;; advanced build + Playwright Chromium + HTMX/SSE integration twice in parallel.
;; It does not modify the supplied Gesso checkout: each job gets an independent
;; disposable Git clone, with matching preflight-admitted HEAD and an independent
;; private net/PID/mount namespace. The Gesso checkout must have all requisite
;; Maven/Git dependencies already cached: a worker has no external network.
;;
;; Run from Ygg/Shard with:
;;   SHARD_TEST_GESSO_ROOT=/absolute/path/to/gesso \
;;   SHARD_TEST_REQUIRE_PLAYWRIGHT=1 bb test
;; Without SHARD_TEST_GESSO_ROOT this test is explicitly skipped. With the
;; variable set any missing native bwrap, failing Playwright/Chromium, failure
;; to create clones, or incomplete job MUST fail; no shim or --dump-dom fallback.

(defn- checked-test-command! [root args env max-ms]
  (let [r (exec! root args env max-ms)]
    (when-not (= 0 (:exit r))
      (throw (ex-info (str "Command failed: " (pr-str args) "\n" (:out r))
                      {:command args :exit (:exit r)})))
    r))

(defn- clone-gesso-for-playwright! [fx src dest]
  ;; Use genuine Git clones, not worktrees. That avoids touching the original
  ;; checkout's .git/worktrees, index, compiled resources or local branch.
  (checked-test-command!
    (:project fx)
    ["git" "clone" "--quiet" "--local" "--no-hardlinks" "--no-checkout"
     (.getCanonicalPath (file src)) (.getCanonicalPath (file dest))]
    {} 180000)
  (let [head (str/trim (:out (checked-test-command!
                               src ["git" "rev-parse" "HEAD"] {} 10000)))]
    (checked-test-command! dest ["git" "checkout" "--quiet" "--detach" head]
                           {} 180000)
    (when-not (= head (str/trim (:out (checked-test-command!
                                      dest ["git" "rev-parse" "HEAD"] {} 10000))))
      (throw (ex-info "Disposable clone HEAD differs from source" {:expected head})))
    head))

(deftest native-bubblewrap-runs-existing-gesso-playwright-in-two-workers
  (let [supplied (some-> (System/getenv "SHARD_TEST_GESSO_ROOT") str/trim not-empty)
        required? (= "1" (System/getenv "SHARD_TEST_REQUIRE_PLAYWRIGHT"))]
    (if-not supplied
      (if required?
        (is false "SHARD_TEST_REQUIRE_PLAYWRIGHT=1 requires SHARD_TEST_GESSO_ROOT")
        (println "SKIP real Gesso Playwright: set SHARD_TEST_GESSO_ROOT to opt in"))
      (with-fixture
        (fn [fx]
          (let [root (.getCanonicalFile (file supplied))
                native (native-bwrap-path)
                ws-a (:workspace fx)
                ws-b (file (:base fx) "gesso-browser-worker-b")
                resolved-bb (resolved-test-bb! fx)]
            (is (.isDirectory root) (str "Gesso checkout not found: " root))
            (is (some? native) "Native /usr/bin/bwrap or /bin/bwrap is required; shims are forbidden")
            (is (.isFile (file root "script" "gesso_test.clj"))
                "Gesso must contain its existing real-browser integration runner")
            (when (and native (.isDirectory root)
                       (.isFile (file root "script" "gesso_test.clj")))
              (let [clean (checked-test-command!
                            root ["git" "status" "--porcelain" "--untracked-files=no"] {} 12000)]
                (when-not (str/blank? (:out clean))
                  (throw (ex-info "Real Gesso Playwright test requires a clean tracked checkout"
                                  {:git-status (:out clean)}))))
              ;; Always use the *real* bwrap, never the namespace-only test shim.
              (Files/createSymbolicLink (.toPath (file (:bin fx) "bwrap"))
                                       (.toPath (file native)) attrs)
              (Files/createSymbolicLink (.toPath (file (:bin fx) "bb"))
                                       (.toPath (file resolved-bb)) attrs)
              (let [head-a (clone-gesso-for-playwright! fx root ws-a)
                    head-b (clone-gesso-for-playwright! fx root ws-b)
                    env {"PATH" (str (.getAbsolutePath (:bin fx))
                                     java.io.File/pathSeparator
                                     (System/getenv "PATH"))
                         "PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD" "1"
                         "GESSO_SHARD_PREFLIGHT_COMMIT" head-a}
                    ;; Gesso's actual preflight is the only authority that may
                    ;; admit these prepared test jobs. Run it exactly once,
                    ;; before either worker is allowed to claim a job.
                    check (checked-test-command!
                            ws-a [resolved-bb "script/gesso_test.clj" "preflight"]
                            env 180000)]
                (when-not (= head-a head-b)
                  (throw (ex-info "Both clones must match preflighted source"
                                  {:first head-a :second head-b})))
                (is (zero? (:exit check)) "Gesso integrity preflight must pass")
                ;; This barrier proves concurrent admission/execution rather
                ;; than merely letting two expensive jobs run sequentially.
                (let [command ["bash" "-c"
                               (str "readlink /proc/self/ns/net > playwright-net-id; "
                                    "readlink /proc/self/ns/pid > playwright-pid-id; "
                                    "readlink /proc/self/ns/mnt > playwright-mnt-id; "
                                    "touch playwright-ready; "
                                    "until test -f playwright-go; do sleep .1; done; "
                                    "exec bb script/gesso_test.clj "
                                    "gate:prepared browser:integration")]
                      jobs [(assoc (job "playwright-a" command)
                                   :gate :browser:integration
                                   :unit {:type :gate}
                                   :timeout-ms 900000)
                            (assoc (job "playwright-b" command)
                                   :gate :browser:integration
                                   :unit {:type :gate}
                                   :timeout-ms 900000)]
                      _ (enumerate! fx jobs)
                      started (mapv (fn [workspace]
                                      (start-cli!
                                        fx (:project fx)
                                        [resolved-bb (:shard fx) "worker"
                                         (str (:run fx)) (str workspace) "jvm" "250"]
                                        env))
                                    [ws-a ws-b])]
                  (let [both-ready? (atom false)]
                    (try
                      (let [ready-a (await-file! (file ws-a "playwright-ready") 45000)
                            ready-b (await-file! (file ws-b "playwright-ready") 45000)]
                        (is ready-a "First real Gesso worker must enter its sandbox")
                        (is ready-b "Second worker must enter its own sandbox concurrently")
                        (when (and ready-a ready-b)
                          (doseq [name ["net" "pid" "mnt"]]
                            (let [a (str/trim (slurp (file ws-a (str "playwright-" name "-id"))))
                                  b (str/trim (slurp (file ws-b (str "playwright-" name "-id"))))]
                              (is (re-matches #"[a-z]+:\[[0-9]+\]" a)
                                  (str "Invalid first " name " namespace"))
                              (is (re-matches #"[a-z]+:\[[0-9]+\]" b)
                                  (str "Invalid second " name " namespace"))
                              (is (not= a b)
                                  (str "Workers must not share the " name " namespace"))))
                          (reset! both-ready? true)))
                      (finally
                        (if @both-ready?
                          (do
                            (spit (file ws-a "playwright-go") "go")
                            (spit (file ws-b "playwright-go") "go")
                            (doseq [started-worker started]
                              (let [done (finish-cli! started-worker 940000)]
                                (is (= 0 (:exit done)) (:out done)))))
                          ;; Do not start two costly real-browser jobs if the
                          ;; sandbox overlap barrier failed.
                          (doseq [started-worker started]
                            (terminate-cli! started-worker))))))
                  (let [outcomes (results fx)
                        summary (report fx)]
                    (is (= #{"playwright-a.edn" "playwright-b.edn"}
                           (set (keys outcomes))))
                    (is (every? #(= :passed (:status %)) (vals outcomes))
                        (pr-str outcomes))
                    (is (= 0 (:exit summary)) (:out summary))
                    (is (= :pass (get-in summary [:edn :overall])))
                    (doseq [id ["playwright-a" "playwright-b"]]
                      (let [log (file (:run fx) "logs" (str id ".log"))]
                        (is (.isFile log) (str "Missing real-browser log " id))
                        (when (.isFile log)
                          (is (str/includes? (slurp log)
                                             "Production runtime / real Chromium integration: PASS")
                              (str "Real Gesso Playwright integration not proven: " id)))))))))))))))


;; v1107 scratch-path admission regression: use the shared v1108
;; namespace launcher, with a real read-only remount for refusal cases.
;; Native Bubblewrap mount policy is checked separately above.

(defn- namespace-scratch-shim! [fx]
  (sandbox-script! fx "bwrap" scratch-namespace-bwrap-shim)
  (let [real-bb (resolved-test-bb! fx)
        link (file (:bin fx) "bb")]
    (when-not (exists? link)
      (Files/createSymbolicLink (.toPath link)
                                (.toPath (file real-bb)) attrs)))
  (sandbox-bin-env fx))

(defn- scratch-worker! [fx env]
  (exec! (:project fx)
         [(:bb fx) (:shard fx) "worker"
          (str (:run fx)) (str (:workspace fx)) "jvm" "100"]
         env 30000))

(defn- assert-no-admitted-scratch-job! [fx result label]
  (is (= 2 (:exit result)) (str label ": " (:out result)))
  (is (str/includes? (:out result) "Browser scratch directory is not writable")
      (str label ": the runtime scratch probe (not namespace setup) must reject it: "
           (:out result)))
  (is (empty? (or (.listFiles (file (:run fx) "claims")) []))
      (str label ": refusal must precede claims"))
  (is (empty? (or (.listFiles (file (:run fx) "results")) []))
      (str label ": refusal cannot publish a result"))
  (is (not (exists? (file (:workspace fx) "scratch-job-ran")))
      (str label ": no job subprocess was executed")))

(deftest sandbox-browser-scratch-probe-accepts-real-writable-paths
  (with-fixture
    (fn [fx]
      (enumerate! fx [(job "scratch-ok" ["python3" "-c"
                                           (str "from pathlib import Path\n"
                                                "import tempfile\n"
                                                "with tempfile.TemporaryDirectory(dir='/tmp') as td, "
                                                "tempfile.TemporaryDirectory(dir='/dev/shm') as sd:\n"
                                                "    a=Path(td)/'probe'; b=Path(sd)/'probe'\n"
                                                "    a.write_text('tmp-ok'); b.write_text('shm-ok')\n"
                                                "    assert a.read_text()=='tmp-ok'\n"
                                                "    assert b.read_text()=='shm-ok'\n"
                                                "Path('scratch-job-ran').write_text('ok')\n")])])
      (let [env (namespace-scratch-shim! fx)
            outcome (scratch-worker! fx env)]
        (is (= 0 (:exit outcome)) (:out outcome))
        (is (= :passed (get-in (results fx) ["scratch-ok.edn" :status])))
        (is (= "ok" (slurp (file (:workspace fx) "scratch-job-ran"))))
        (is (= :pass (get-in (report fx) [:edn :overall])))))))

(deftest sandbox-browser-scratch-readonly-probe-refuses-before-worker-claims
  (doseq [directory ["/tmp" "/dev/shm"]]
    (with-fixture
      (fn [fx]
        (enumerate! fx [(job "scratch-nope" ["bash" "-c" "touch scratch-job-ran"])])
        (let [env (assoc (namespace-scratch-shim! fx)
                         "SHARD_TEST_SCRATCH_READONLY" directory)
              outcome (scratch-worker! fx env)]
          (assert-no-admitted-scratch-job! fx outcome directory)
          ;; An admission failure must not poison this basket. A later
          ;; invocation with genuinely writable scratch can still claim and
          ;; complete the same job exactly once.
          (let [recovered (scratch-worker!
                            fx (dissoc env "SHARD_TEST_SCRATCH_READONLY"))]
            (is (= 0 (:exit recovered)) (:out recovered))
            (is (= :passed (get-in (results fx) ["scratch-nope.edn" :status])))
            (is (exists? (file (:workspace fx) "scratch-job-ran")))
            (is (= 1 (count (results fx))))))))))

(deftest sandbox-browser-scratch-readonly-preflight-refuses-before-clones
  (doseq [directory ["/tmp" "/dev/shm"]]
    (with-fixture
      (fn [fx]
        (git-project! fx [(job "scratch-nope" ["bash" "-c" "touch scratch-job-ran"])])
        (let [env (merge (ygg-shim! fx)
                         (namespace-scratch-shim! fx)
                         {"SHARD_TEST_SCRATCH_READONLY" directory})
              result (exec! (:project fx)
                            [(:bb fx) (:shard fx) "run" "--workers" "2"
                             "--inventory" (:bb fx) "inventory.clj"]
                            env 30000)]
          (is (= 2 (:exit result)) (str directory ": " (:out result)))
          (is (str/includes? (:out result) "Browser scratch directory is not writable")
              (str directory ": the actual scratch probe must reject it: "
                   (:out result)))
          (is (not (exists? (file (:project fx) ".ygg" "fake-workers")))
              (str directory ": preflight must prevent Ygg clone creation"))
          (is (not (exists? (file (:project fx) ".ygg" "runs" "run-fixed-mock-run")))
              (str directory ": no job basket should have been started"))
          (is (not (exists? (file (:project fx) "scratch-job-ran")))))))))

(deftest sandbox-browser-scratch-noop-launcher-remains-fail-closed
  (with-fixture
    (fn [fx]
      (enumerate! fx [(job "scratch-nope" ["bash" "-c" "touch scratch-job-ran"])])
      ;; Writable host scratch must not let a launcher without namespace
      ;; separation pass admission: both properties are mandatory.
      (sandbox-script! fx "bwrap" fake-bwrap-without-namespaces)
      (let [outcome (exec! (:project fx)
                           [(resolved-test-bb! fx) (:shard fx) "worker"
                            (str (:run fx)) (str (:workspace fx)) "jvm" "100"]
                           (sandbox-bin-env fx) 18000)]
        (is (= 2 (:exit outcome)) (:out outcome))
        (is (str/includes? (:out outcome) "refusing unsandboxed jobs")
            (:out outcome))
        (is (empty? (or (.listFiles (file (:run fx) "claims")) [])))
        (is (empty? (or (.listFiles (file (:run fx) "results")) [])))
        (is (not (exists? (file (:workspace fx) "scratch-job-ran"))))))))

;; v1117: Persistent sessions are deliberately *test-injected*, not admitted
;; by the production CLI.  This section exercises the real v1116 Shard claim
;; loop, on-disk inventory/claims/results and two actual JVM processes.  Only
;; the sandbox preflight is replaced by a test-only no-op: the v1114 session
;; launcher cannot prove Bubblewrap/process-group admission.  Those remain
;; separate production acceptance gates; never describe this as sandbox proof.
(defn- persistent-shard-namespace! []
  (let [source (slurp (shard!))
        final-call "(apply -main *command-line-args*)"
        trimmed (str/trimr source)
        n (create-ns (gensym "shard_test_embedded_"))]
    (when-not (str/ends-with? trimmed final-call)
      (throw (ex-info "Shard entry point changed; do not run production -main in fixture" {})))
    (binding [*ns* n]
      (load-string (subs trimmed 0 (- (count trimmed) (count final-call)))))
    (let [worker (ns-resolve n 'worker!)
          preflight (ns-resolve n 'sandbox-preflight!)
          canonical (ns-resolve n 'canonical-edn)
          digest (ns-resolve n 'digest)]
      (when-not (every? some? [worker preflight canonical digest])
        (throw (ex-info "Shard private fixture seam has changed" {})))
      {:worker worker :preflight preflight :canonical canonical :digest digest})))

(defn- persistent-runtime [command]
  {:identity "shard-jvm-test-v1117"
   :capabilities #{}
   :execution :persistent-jvm
   :session-command command})

(defn- persistent-job
  ([id namespace] (persistent-job id namespace [] 15000))
  ([id namespace dependencies timeout-ms]
   {:id id :gate :jvm :runtime :jvm
    :unit {:type :clojure-namespace :name namespace}
    ;; Invalid fallback command deliberately proves no old per-job subprocess
    ;; path can masquerade as persistent execution.
    :command ["/must-not-run-fallback-subprocess"]
    :depends-on dependencies :timeout-ms timeout-ms :requires #{}}))

(defn- enumerate-persistent! [fx runtime jobs]
  (let [cfg (file (:base fx) "persistent.edn")]
    (spit cfg (pr-str {:version 1 :project-root (.getCanonicalPath (:project fx))
                       :runtimes {:jvm runtime} :jobs (vec jobs)}))
    (let [r (invoke fx "enumerate" (str cfg) (str (:run fx)))]
      (is (= 0 (:exit r)) (:out r))
      r)))

(defn- persistent-fixture-ns! [dir namespace body]
  (let [name-path (str (-> namespace
                           (str/replace "." "/")
                           (str/replace "-" "_")) ".clj")
        target (file dir name-path)]
    (.mkdirs (.getParentFile target))
    (spit target (str "(ns " namespace " (:require [clojure.test :refer [deftest is]]))\n" body "\n"))))

(defn- persistent-java-command [classpath]
  [(str (System/getProperty "java.home") java.io.File/separator "bin"
        java.io.File/separator "java")
   "-Xmx256m" "-cp" classpath "clojure.main" "-m" "shard.worker.jvm"])

(defn- persistent-session! [fx command fingerprint worker-id]
  ((requiring-resolve 'shard.runtime.jvm-session/start!)
   {:test-only? true
    :command command :cwd (.getCanonicalPath (:project fx))
    :control-dir (.getCanonicalPath (:base fx))
    :stdout-file (.getCanonicalPath (file (:base fx) (str worker-id ".out")))
    :stderr-file (.getCanonicalPath (file (:base fx) (str worker-id ".err")))
    :worker-id worker-id :runtime-fingerprint fingerprint
    :startup-timeout-ms 30000 :shutdown-timeout-ms 10000}))

(defn- persistent-fingerprint [embedded runtime]
  ((:digest embedded)
   ((:canonical embedded)
    {:runtime :jvm :identity (:identity runtime)
     :session-command (:session-command runtime)})))

(defn- test-worker-thread! [embedded fx workspace session]
  (.mkdirs (io/file workspace))
  (let [answer (promise)
        t (Thread.
           (fn []
             (deliver answer
                      (try
                        ((:worker embedded) (str (:run fx)) (str workspace)
                         "jvm" 15000 nil session)
                        {:status :finished}
                        (catch Throwable t
                          {:status :threw :message (.getMessage t)
                           :data (ex-data t)}))))
           "shard-persistent-claim-fixture")]
    (.setDaemon t true)
    (.start t)
    {:thread t :answer answer}))

(defn- await-fixture-files! [files timeout-ms]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
    (loop []
      (cond
        (every? exists? files) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/sleep 20) (recur))))))

(defn- complete-worker-thread! [{:keys [^Thread thread answer]}]
  (let [result (deref answer 25000 ::timeout)]
    (.join thread 1000)
    (is (not= ::timeout result) "Injected Shard worker must terminate")
    (is (not (.isAlive thread)) "Worker thread must not leak")
    result))

(deftest persistent-cli-refuses-production-session-before-claim
  (with-fixture
    (fn [fx]
      (let [command ["java" "-m" "shard.worker.jvm"]
            runtime (persistent-runtime command)]
        (enumerate-persistent! fx runtime [(persistent-job "a" "shard.test-pass-test")])
        (let [r (worker fx)]
          (is (not= 0 (:exit r)))
          (is (str/includes? (:out r) "production admission is not implemented")
              (:out r))
          (is (empty? (or (.listFiles (file (:run fx) "claims")) [])))
          (is (empty? (or (.listFiles (file (:run fx) "results")) []))))))))

(deftest persistent-inventory-rejects-unsafe-units
  (with-fixture
    (fn [fx]
      (let [runtime (persistent-runtime ["java" "-m" "shard.worker.jvm"])]
        (doseq [unit [{:type :namespace :name "shard.good-test"}
                      {:type :clojure-namespace :name "no-slash/allowed"}
                      {:type :clojure-namespace :name "shard..bad-test"}]]
          (let [jobs [(assoc (persistent-job "a" "shard.good-test") :unit unit)]
                cfg (file (:base fx) "invalid-persistent.edn")]
            (spit cfg (pr-str {:version 1
                               :project-root (.getCanonicalPath (:project fx))
                               :runtimes {:jvm runtime} :jobs jobs}))
            (let [r (invoke fx "enumerate" (str cfg) (str (:run fx)))]
              (is (not= 0 (:exit r)) (:out r))
              (is (not (exists? (file (:run fx) "jobs.edn")))))))))))

(deftest persistent-jvm-claim-loop-concurrent-reuse-and-dependencies
  (let [cp (not-empty (System/getenv "SHARD_TEST_JVM_CLASSPATH"))
        require-native? (= "1" (System/getenv "SHARD_TEST_REQUIRE_JVM"))]
    (cond
      cp
      (with-fixture
        (fn [fx]
          (let [embedded (persistent-shard-namespace!)
                src (file (:base fx) "native-src")
                real-cp (str cp java.io.File/pathSeparator
                             (.getCanonicalPath (file (System/getProperty "user.dir") "src"))
                             java.io.File/pathSeparator (.getCanonicalPath src))
                command (persistent-java-command real-cp)
                runtime (persistent-runtime command)
                fingerprint (persistent-fingerprint embedded runtime)
                first-release (file (:base fx) "first.release")
                second-release (file (:base fx) "second.release")
                begin (fn [marker release]
                        (str "(deftest synchronized-job "
                             "(spit " (pr-str (str marker)) " \"started\") "
                             "(let [limit (+ (System/nanoTime) 12000000000)] "
                             "(loop [] (when-not (.exists (java.io.File. "
                             (pr-str (str release)) ")) "
                             "(if (< (System/nanoTime) limit) "
                             "(do (Thread/sleep 20) (recur)) "
                             "(throw (ex-info \"test barrier timed out\" {})))))) "
                             "(is true))"))
                first-names ["shard.claim-a-test" "shard.claim-b-test"]
                second-names ["shard.claim-c-test" "shard.claim-d-test"]
                first-markers (mapv #(file (:base fx) (str % ".started")) first-names)
                second-markers (mapv #(file (:base fx) (str % ".started")) second-names)
                jobs [(persistent-job "a" (first first-names))
                      (persistent-job "b" (second first-names))
                      (persistent-job "c" (first second-names) ["a" "b"] 15000)
                      (persistent-job "d" (second second-names) ["a" "b"] 15000)
                      (persistent-job "e" "shard.claim-pass-test" ["c" "d"] 15000)
                      (persistent-job "f" "shard.claim-fail-test" ["c" "d"] 15000)
                      (persistent-job "g" "shard.claim-pass-test" ["f"] 15000)
                      (persistent-job "h" "shard.claim-pass-test" ["e"] 15000)]
                sessions (atom [])
                workers (atom [])]
            (.mkdirs src)
            (doseq [[name marker] (map vector first-names first-markers)]
              (persistent-fixture-ns! src name (begin marker first-release)))
            (doseq [[name marker] (map vector second-names second-markers)]
              (persistent-fixture-ns! src name (begin marker second-release)))
            (persistent-fixture-ns! src "shard.claim-pass-test"
                                    "(deftest passes (is (= 2 (+ 1 1))))")
            (persistent-fixture-ns! src "shard.claim-fail-test"
                                    "(deftest fails (is (= 1 2)))")
            (enumerate-persistent! fx runtime jobs)
            (try
              (reset! sessions [(persistent-session! fx command fingerprint "claim.1")
                                (persistent-session! fx command fingerprint "claim.2")])
              (is (not= (:pid (first @sessions)) (:pid (second @sessions))))
              ;; This is the ONLY seam replaced: production Shard still refuses
              ;; the sessionless CLI, while this test validates the actual
              ;; Shard claim loop, result publication and dependency resolution.
              (with-redefs-fn {(:preflight embedded) (fn [_] true)}
                (fn []
                  (reset! workers
                          (mapv #(test-worker-thread! embedded fx
                                                      (file (:base fx) (str "clone-" %2)) %1)
                                @sessions [1 2]))
                  (is (await-fixture-files! first-markers 12000)
                      "Two workers must execute distinct stage-one jobs concurrently")
                  (spit first-release "release")
                  (is (await-fixture-files! second-markers 12000)
                      "Two workers must continue into separate second jobs")
                  (spit second-release "release")
                  (is (every? #(= :finished (:status %))
                              (mapv complete-worker-thread! @workers)))))
              (let [outcomes (into {} (map (juxt :id identity) (vals (results fx))))
                    report-result (report fx)]
                (is (= #{"a" "b" "c" "d" "e" "f" "g" "h"}
                       (set (keys outcomes))))
                (is (= {:a :passed :b :passed :c :passed :d :passed
                        :e :passed :f :failed :g :blocked :h :passed}
                       (into {} (map (fn [[id r]] [(keyword id) (:status r)]) outcomes))))
                (is (= :fail (get-in report-result [:edn :overall]))
                    (str report-result))
                (is (= 8 (count (or (.listFiles (file (:run fx) "claims")) []))))
                (is (= 8 (count outcomes)))
                (is (every? #(>= (count @(:completed-job-ids %)) 2) @sessions)
                    "Each JVM must execute at least two separately CLAIMED jobs")
                (is (= 7 (reduce + (map #(count @(:completed-job-ids %)) @sessions)))
                    "Seven executable jobs, one blocked dependency")
                (is (not (exists? (file (:workspace fx) "must-not-run-fallback-subprocess")))))
              (finally
                (spit first-release "force release")
                (spit second-release "force release")
                (doseq [w @workers] (complete-worker-thread! w))
                (doseq [s @sessions]
                  ((requiring-resolve 'shard.runtime.jvm-session/abort!) s)))))))
      require-native? (is false "SHARD_TEST_JVM_CLASSPATH missing for mandatory live claim test")
      :else (println "SKIP persistent-jvm-claim-loop-concurrent-reuse-and-dependencies: set SHARD_TEST_JVM_CLASSPATH"))))

(deftest persistent-jvm-lost-session-keeps-claim-incomplete
  (let [cp (not-empty (System/getenv "SHARD_TEST_JVM_CLASSPATH"))
        require-native? (= "1" (System/getenv "SHARD_TEST_REQUIRE_JVM"))]
    (cond
      cp
      (with-fixture
        (fn [fx]
          (let [embedded (persistent-shard-namespace!)
                src (file (:base fx) "native-src")
                real-cp (str cp java.io.File/pathSeparator
                             (.getCanonicalPath (file (System/getProperty "user.dir") "src"))
                             java.io.File/pathSeparator (.getCanonicalPath src))
                command (persistent-java-command real-cp)
                runtime (persistent-runtime command)
                fingerprint (persistent-fingerprint embedded runtime)
                s (atom nil)]
            (.mkdirs src)
            (persistent-fixture-ns! src "shard.claim-timeout-test"
                                    "(deftest waits-forever (Thread/sleep 5000) (is true))")
            (enumerate-persistent! fx runtime
                                   [(persistent-job "a-timeout" "shard.claim-timeout-test" [] 200)])
            (try
              (reset! s (persistent-session! fx command fingerprint "timed.1"))
              (with-redefs-fn {(:preflight embedded) (fn [_] true)}
                (fn []
                  (let [w (test-worker-thread! embedded fx (file (:base fx) "clone-1") @s)
                        outcome (complete-worker-thread! w)]
                    (is (= :threw (:status outcome)) (pr-str outcome)))))
              (is (= :poisoned @(:phase @s)))
              (is (not (.isAlive ^Process (:process @s))))
              (is (= 1 (count (or (.listFiles (file (:run fx) "claims")) []))))
              (is (empty? (results fx)) "Timed-out claim must NOT have a forged success")
              (let [r (report fx)]
                (is (not= 0 (:exit r)))
                (is (= :incomplete (get-in r [:edn :overall])) (pr-str r)))
              (finally
                (when @s ((requiring-resolve 'shard.runtime.jvm-session/abort!) @s)))))))
      require-native? (is false "SHARD_TEST_JVM_CLASSPATH missing for mandatory JVM timeout test")
      :else (println "SKIP persistent-jvm-lost-session-keeps-claim-incomplete: set SHARD_TEST_JVM_CLASSPATH"))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'shard-test)]
    (when (pos? (+ fail error)) (System/exit 1))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
