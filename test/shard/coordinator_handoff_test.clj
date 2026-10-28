(ns shard.coordinator-handoff-test
  "Real-process, fake-worker coordinator tests.  They exercise the actual
  v1130 Shard coordinator and immutable shared claims without granting fake
  workers production Bubblewrap admission.  Native sandbox acceptance lives
  in the separate admission tests."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]])
  (:import [java.nio.file Files LinkOption]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent TimeUnit]
           [java.lang ProcessBuilder ProcessBuilder$Redirect]))

(def ^:private attrs (make-array FileAttribute 0))
(def ^:private terminal "(apply -main *command-line-args*)")

(defn- file [& segments] (apply io/file segments))

(defn- root-script []
  (let [script (.getCanonicalFile (file (System/getProperty "user.dir") "shard"))]
    (when-not (.isFile script)
      (throw (ex-info "Run tests from the combined Ygg/Shard repository root"
                      {:expected (str script)})))
    script))

(defn- coordinator-functions []
  (let [source (str/trimr (slurp (root-script)))
        name (gensym "shard_handoff_loaded_")]
    (when-not (str/ends-with? source terminal)
      (throw (ex-info "Shard entrypoint changed; refusing to execute CLI main" {})))
    (binding [*ns* (create-ns name)]
      (load-string (subs source 0 (- (count source) (count terminal)))))
    (let [required '[execute-workers! script-command! worker-specs report-data! digest canonical-edn validate-jobs!]]
      (when-not (every? #(ns-resolve name %) required)
        (throw (ex-info "Shard coordinator API has changed" {:required required}))))
    {:run @(ns-resolve name 'execute-workers!)
     :command (ns-resolve name 'script-command!)
     :specs @(ns-resolve name 'worker-specs)
     :report @(ns-resolve name 'report-data!)
     :digest @(ns-resolve name 'digest)
     :canonical @(ns-resolve name 'canonical-edn)
     :validate-jobs @(ns-resolve name 'validate-jobs!)}))

(def ^:private coordinator (delay (coordinator-functions)))

;; Each worker is an independent OS process.  It claims with mkdir, publishes
;; immutable result files and logs, and writes short append-only event records.
;; It intentionally never uses Bubblewrap or the real JVM session manager:
;; these tests target the coordinator's N-worker scheduling/teardown contract.
(def ^:private simulation-worker
  (str/join "\n"
   ["#!/usr/bin/env python3"
    "import os, sys, pathlib, time, subprocess"
    "_, action, run_text, workspace, runtime, idle, *rest = sys.argv"
    "assert action == 'worker'"
    "run = pathlib.Path(run_text)"
    "mode = (run / 'mode.txt').read_text().strip()"
    "fingerprint = (run / 'fingerprint.txt').read_text().strip()"
    "jobs = [(line.split('|')[0], line.split('|')[1]) for line in (run / 'simulation-jobs.txt').read_text().splitlines()]"
    "ids = [id for id, kind in jobs if kind == runtime]"
    "worker = pathlib.Path(workspace).name"
    "pid = os.getpid()"
    "def event(kind, job=''):"
    "    fd = os.open(run / 'events.log', os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)"
    "    try: os.write(fd, f'{kind}|{worker}|{runtime}|{pid}|{job}\\n'.encode())"
    "    finally: os.close(fd)"
    "event('START', 'scheduled' if rest else 'dynamic')"
    "if mode == 'many-handoffs' and runtime == 'jvm': time.sleep(0.75)"
    "if mode == 'abnormal' and runtime == 'gate':"
    "    event('ERROR')"
    "    sys.exit(17)"
    "for pos, job in enumerate(ids):"
    "    claim = run / 'claims' / job"
    "    try: claim.mkdir()"
    "    except FileExistsError: continue"
    "    event('CLAIM', job)"
    "    if mode == 'unpublished' and runtime == 'gate' and pos == len(ids) - 1:"
    "        event('SKIP', job)"
    "        continue"
    "    time.sleep(0.065 if runtime == 'jvm' else 0.003)"
    "    if mode == 'descendant' and runtime == 'gate' and pos == 0:"
    "        child = subprocess.Popen(['sleep','25'], start_new_session=True)"
    "        (claim / 'session.edn').write_text('{:format :shard/job-session :version 1 :pid ' + str(child.pid) + '}')"
    "        (run / 'leak-pid').write_text(str(child.pid))"
    "    (run / 'logs' / (job + '.log')).write_text('simulated independent job receipt\\n')"
    "    receipt = '{:format :shard/job-result :version 1 :inventory-sha256 \"' + fingerprint + '\" :id \"' + job + '\" :runtime :' + runtime + ' :worker-id \"sim-' + worker + '-' + str(pid) + '\" :started-at \"2026-10-27T00:00:00Z\" :completed-at \"2026-10-27T00:00:01Z\" :log \"logs/' + job + '.log\" :status :passed :exit-code 0 :duration-ms 1}'"
    "    target = run / 'results' / (job + '.edn')"
    "    temporary = run / 'results' / ('.' + job + '.' + str(pid) + '.tmp')"
    "    temporary.write_text(receipt)"
    "    try: os.link(temporary, target)"
    "    finally: temporary.unlink()"
    "    event('RESULT', job)"
    "event('EXIT')"
    "sys.exit(0)"
    ""]))

(defn- remove-tree! [root]
  (when (Files/exists (.toPath (io/file root)) (make-array LinkOption 0))
    (with-open [paths (Files/walk (.toPath (io/file root))
                                  (make-array java.nio.file.FileVisitOption 0))]
      (doseq [path (reverse (sort-by #(.getNameCount %)
                                    (iterator-seq (.iterator paths))))]
        (Files/deleteIfExists path)))))

(defn- make-scenario! [n groups mode]
  (let [base (.toFile (Files/createTempDirectory "shard-n-handoff-" attrs))
        run (file base "run")
        _ (.mkdirs run)
        _ (doseq [sub ["claims" "results" "logs"]] (.mkdir (file run sub)))
        script (file base "fake_worker.py")
        _ (spit script simulation-worker)
        clones (mapv (fn [idx]
                       (let [path (file base (str "clone-" (inc idx)))]
                         (.mkdir path)
                         {:mountpoint (.getCanonicalPath path)}))
                     (range n))
        jobs (vec (mapcat (fn [[runtime count]]
                            (for [idx (range count)]
                              {:id (format "%s-%03d" (name runtime) idx)
                               :gate :test :runtime runtime
                               :unit {:type :namespace}
                               :command ["true"] :depends-on []
                               :timeout-ms 5000 :requires #{}})) groups))
        runtimes (into {} (for [[runtime _] groups]
                            [runtime {:identity (str "fixture-" (name runtime))
                                      :capabilities #{}}]))
        jobs ((:validate-jobs @coordinator) jobs runtimes)
        project-root (.getCanonicalPath base)
        fingerprint ((:digest @coordinator)
                     ((:canonical @coordinator)
                      {:project-root project-root :runtimes runtimes :jobs jobs}))
        manifest {:format :shard/jobs :version 1 :jobs jobs
                  :inventory-sha256 fingerprint :runtimes runtimes
                  :project-root project-root}]
    (spit (file run "jobs.edn") ((:canonical @coordinator) manifest))
    (spit (file run "fingerprint.txt") fingerprint)
    (spit (file run "mode.txt") (name mode))
    (spit (file run "simulation-jobs.txt")
          (str/join "\n" (for [{:keys [id runtime]} jobs]
                            (str id "|" (name runtime)))))
    {:base base :run run :clones clones :jobs jobs :manifest manifest
     :script script :n n :groups groups :mode mode}))

(defn- receipts [scenario]
  (into {}
        (for [f (or (.listFiles (file (:run scenario) "results")) [])
              :when (str/ends-with? (.getName f) ".edn")]
          [(:id (edn/read-string (slurp f))) (edn/read-string (slurp f))])))

(defn- events [scenario]
  (let [log (file (:run scenario) "events.log")]
    (if (.exists log)
      (mapv #(str/split % #"\|" -1) (str/split-lines (slurp log)))
      [])))

(defn- exercise! [scenario]
  (let [command (get @coordinator :command)
        script (.getCanonicalPath (:script scenario))]
    (with-redefs-fn {command (fn [] ["python3" script])}
      #(try {:outcomes ((:run @coordinator) (:run scenario)
                         (:clones scenario) (:manifest scenario) nil)}
            (catch Exception ex {:error (.getMessage ex)
                                 :data (ex-data ex)})))))

(defn- assert-exclusive-workspace-events! [events]
  (let [state (atom {})]
    (doseq [[kind workspace runtime pid job] events]
      (case kind
        "START" (do
                  (is (not (contains? @state workspace))
                      (str "Simultaneous ownership of " workspace))
                  (swap! state assoc workspace pid))
        "EXIT" (do
                 (is (= pid (get @state workspace))
                     (str "Incorrect workspace retirement for " workspace))
                 (swap! state dissoc workspace))
        "ERROR" nil
        "CLAIM" (is (= pid (get @state workspace)))
        "SKIP" (is (= pid (get @state workspace)))
        "RESULT" (is (= pid (get @state workspace)))
        (is false (str "Unknown coordinator event " [kind workspace runtime pid job]))))
    @state))

(defn- assert-normal! [scenario observed]
  (let [all (:jobs scenario)
        ids (set (map :id all))
        events (events scenario)
        launches (filter #(= "START" (first %)) events)
        handoffs (filter #(and (= "START" (first %))
                              (= "jvm" (nth % 2))) events)
        claimed (map #(nth % 4) (filter #(= "CLAIM" (first %)) events))
        published (map #(nth % 4) (filter #(= "RESULT" (first %)) events))
        receipts (receipts scenario)
        by-workspace (group-by second launches)
        outcomes (:outcomes observed)]
    (is (nil? (:error observed)) (str observed))
    (is (= (count ids) (count all)) "All inventory IDs must be unique")
    (is (= ids (set (keys receipts))) "No missing or extra receipts")
    (is (= ids (set claimed)) "Every claim has exactly one job ID")
    (is (= (count ids) (count claimed)) "Every job is claimed only once")
    (is (= ids (set published)) "Every receipt has exactly one job ID")
    (is (= (count ids) (count published)) "Every result publishes once")
    (let [report ((:report @coordinator) (:run scenario))]
      (is (= :pass (:overall report)) "The real Shard reporter must accept all receipts")
      (is (= (count ids) (:completed report)))
      (is (= 0 (get-in report [:counts :claimed-without-result]))))
    (is (every? #(= :passed (:status %)) (vals receipts)))
    (is (every? #(zero? (:exit %)) outcomes))
    (is (<= (count (:clones scenario)) (count outcomes)))
    (is (= (:n scenario) (count (distinct (map second launches)))))
    (is (every? (fn [[_ workers]] (<= 1 (count workers))) by-workspace))
    (is (some #(> (count %) 1) (vals by-workspace))
        "At least one clone must be reassigned to JVM work")
    (is (every? (fn [[_ workers]]
                  (and (= "scheduled" (nth (first workers) 4))
                       (every? #(= "dynamic" (nth % 4)) (rest workers))))
                by-workspace)
        "Every replacement must abandon the previous runtime schedule index")
    (is (<= 2 (count handoffs)))
    (is (empty? (assert-exclusive-workspace-events! events))
        "No active worker may remain at exit")
    ;; The replacement command omits the original schedule index so the
    ;; worker claims from the common basket, rather than a stale queue.
    (is (every? #(= 0 (:exit %)) outcomes))))

(deftest n-worker-gesso-shaped-runtime-reassignment
  (doseq [n [2 4 8 16]]
    (testing (str "Gesso-shaped 138 JVM / six gate jobs, N=" n)
      (let [scenario (make-scenario! n [[:jvm 138] [:gate 6]] :normal)]
        (try
          (let [specs ((:specs @coordinator) (:clones scenario) (:jobs scenario))
                observed (exercise! scenario)]
            (is (= n (count specs)))
            (is (= #{:jvm :gate} (set (map :runtime specs))))
            (assert-normal! scenario observed)
            (is (> (count (:outcomes observed)) n)
                "Gate clone must begin a new phase in the JVM group"))
          (finally (remove-tree! (:base scenario))))))))

(deftest several-runtime-groups-drain-into-one-large-group
  (let [scenario (make-scenario! 16
                                 [[:jvm 240] [:gate-a 2] [:gate-b 2] [:gate-c 2]]
                                 :many-handoffs)]
    (try
      (let [observed (exercise! scenario)
            begins (filter #(= "START" (first %)) (events scenario))
            jvm-begins (filter #(= "jvm" (nth % 2)) begins)]
        (assert-normal! scenario observed)
        (is (<= 3 (- (count jvm-begins)
                     (count (filter #(= :jvm (:runtime %))
                                    ((:specs @coordinator)
                                     (:clones scenario) (:jobs scenario))))))
            "All three short-runtime workers should join the long JVM basket"))
      (finally (remove-tree! (:base scenario))))))

(deftest incomplete-receipt-prevents-clone-reassignment
  (let [scenario (make-scenario! 2 [[:jvm 24] [:gate 2]] :unpublished)]
    (try
      (let [observed (exercise! scenario)
            ev (events scenario)
            started (filter #(= "START" (first %)) ev)
            claims (filter #(= "CLAIM" (first %)) ev)
            receipts (receipts scenario)]
        (is (nil? (:error observed)) (str observed))
        (is (= 2 (count (:outcomes observed))))
        (is (= 2 (count started)) "No workspace is eligible for another phase")
        (is (= (count (:jobs scenario)) (count claims)))
        (is (= (dec (count (:jobs scenario))) (count receipts)))
        (let [report ((:report @coordinator) (:run scenario))]
          (is (= :incomplete (:overall report)))
          (is (= 1 (get-in report [:counts :claimed-without-result])))
          (is (= (dec (:total report)) (:completed report))))
        (is (some (fn [id] (and (not (contains? receipts id))
                                (.exists (file (:run scenario) "claims" id))))
                  (map :id (:jobs scenario)))
            "The unreported job must retain its immutable claim"))
      (finally (remove-tree! (:base scenario))))))

(deftest abnormal-worker-exit-retains-workspaces
  (let [scenario (make-scenario! 2 [[:jvm 24] [:gate 2]] :abnormal)]
    (try
      (let [observed (exercise! scenario)
            ev (events scenario)]
        (is (re-find #"exited abnormally" (str (:error observed))))
        (is (= 2 (count (filter #(= "START" (first %)) ev))))
        (is (= #{"gate" "jvm"}
               (set (map #(nth % 2) (filter #(= "START" (first %)) ev)))))
        (is (every? #(= "scheduled" (nth % 4))
                    (filter #(= "START" (first %)) ev))
            "Abnormal exit cannot authorize a new unscheduled phase"))
      (finally (remove-tree! (:base scenario))))))

(deftest live-descendant-blocks-handoff
  (let [scenario (make-scenario! 2 [[:jvm 24] [:gate 2]] :descendant)]
    (try
      (let [observed (exercise! scenario)
            ev (events scenario)]
        (is (re-find #"descendants are still running" (str (:error observed))))
        (is (= 2 (count (filter #(= "START" (first %)) ev)))
            "No replacement may start while a detached session is live")
        (is (.exists (file (:run scenario) "claims" "gate-000" "session.edn"))))
      (finally
        (let [pid-file (file (:run scenario) "leak-pid")]
          (when (.isFile pid-file)
            (let [pid (str/trim (slurp pid-file))
                  command (ProcessBuilder. ^java.util.List
                                           ["python3" "-c"
                                            "import os,signal,sys; os.killpg(int(sys.argv[1]),signal.SIGKILL)"
                                            pid])]
              (try
                (let [proc (.start command)]
                  (.waitFor proc 3 TimeUnit/SECONDS))
                (catch Exception _ nil)))))
        (remove-tree! (:base scenario))))))
