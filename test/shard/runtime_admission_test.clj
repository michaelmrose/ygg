(ns shard.runtime-admission-test
  "Regression coverage for v1118 persistent-JVM Bubblewrap admission.

  `plan!` constructs an UNVERIFIED sandbox plan; only an actual successful
  `probe!` can establish probe success. Neither function starts or attests a
  persistent JVM. A probe is NOT a production session-admission certificate.

  Portable tests reject unsafe configurations and real failed subprocesses.
  For a native positive gate supply SHARD_TEST_BWRAP (an actual Bubblewrap
  executable) and an accessible `bb` command. Set
  SHARD_TEST_REQUIRE_BWRAP=1 to make missing/denied native isolation a hard
  failure rather than a skip. Native probes use source-tree temp workspaces,
  not /tmp, because a correct sandbox mounts a private /tmp."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [shard.runtime.admission :as admission])
  (:import [java.io File]
           [java.nio.file Files Path LinkOption FileVisitOption]
           [java.nio.file.attribute FileAttribute PosixFilePermission]))

(def ^:private empty-attrs (make-array FileAttribute 0))
(def ^:private no-follow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private owner-only
  #{PosixFilePermission/OWNER_READ PosixFilePermission/OWNER_WRITE
    PosixFilePermission/OWNER_EXECUTE})

(defn- directory! [^Path root part]
  (Files/createDirectories (.resolve root part) empty-attrs))

(defn- remove-tree! [^Path root]
  (when (Files/exists root no-follow)
    ;; Files.walk without FOLLOW_LINKS never descends through a test symlink.
    (with-open [stream (Files/walk root (make-array FileVisitOption 0))]
      (doseq [^Path p (reverse (vec (iterator-seq (.iterator stream))))]
        (Files/deleteIfExists p)))))

(defn- with-fixture
  ([f] (with-fixture nil f))
  ([base f]
   (let [root (if base
                (Files/createTempDirectory ^Path base "shard-admission-test-" empty-attrs)
                (Files/createTempDirectory "shard-admission-test-" empty-attrs))
         workspace (directory! root "worker")
         run (directory! root "runs")
         control (directory! workspace "control")
         home (directory! workspace "home")
         tmp (directory! workspace "private-tmp")
         runtime (directory! workspace "private-runtime")
         host-home (directory! root "host-home")]
     (Files/setPosixFilePermissions control owner-only)
     (try
       (let [opts {:workspace (str workspace)
                   :run-dir (str run)
                   :control-dir (str control)
                   :worker-env {"HOME" (str home)
                                "TMPDIR" (str tmp)
                                "XDG_RUNTIME_DIR" (str runtime)}
                   :command ["java" "-version"]
                   :bwrap "bwrap" :setsid "setsid"}]
         (f {:root root :workspace workspace :run run :control control
             :home home :tmp tmp :runtime runtime :host-home host-home
             :opts opts}))
       (finally (remove-tree! root))))))

(defn- reason-of [f]
  (try (f) :unexpected-success
       (catch clojure.lang.ExceptionInfo e
         (or (:shard.runtime.admission/reason (ex-data e))
             :unclassified-exception))))

(defn- option-reason [opts]
  (reason-of #(admission/plan! opts)))

(defn- has-pair? [argv flag value]
  (boolean (some #(= [flag value] (vec %)) (partition 2 1 argv))))

(deftest verified-plan-shape-remains-only-a-plan
  (with-fixture
    (fn [{:keys [workspace run control opts]}]
      (let [{:keys [argv cwd env isolation status] :as plan} (admission/plan! opts)]
        (is (= :unverified-plan status))
        (is (= (str workspace) cwd))
        (is (= (str workspace) (:workspace plan)))
        (is (= (str control) (:control-dir plan)))
        (is (= (str run) (:run-dir plan)))
        (is (= (:worker-env opts) env))
        (is (= #{:user :ipc :pid :network :mount :private-tmp :private-shm}
               isolation))
        (is (= ["setsid" "--wait" "--" "bwrap"] (vec (take 4 argv))))
        (doseq [flag ["--unshare-user" "--unshare-ipc" "--unshare-pid"
                      "--unshare-net" "--die-with-parent"]]
          (is (some #{flag} argv) (str flag " must be present")))
        (is (has-pair? argv "--ro-bind" "/"))
        (is (has-pair? argv "--bind" (str workspace)))
        (is (has-pair? argv "--proc" "/proc"))
        (is (has-pair? argv "--dev" "/dev"))
        (is (has-pair? argv "--tmpfs" "/tmp"))
        (is (has-pair? argv "--tmpfs" "/dev/shm"))
        (is (has-pair? argv "--chdir" (str workspace)))
        (is (= ["--" "java" "-version"] (vec (take-last 3 argv))))
        (is (not (contains? plan :admitted?)))
        (is (not (contains? plan :session)))
        ;; Planning must not modify the authoritative run basket.
        (is (empty? (seq (.listFiles (.toFile run)))))))))

(deftest worker-plans-do-not-share-writable-clones
  (with-fixture
    (fn [{:keys [root workspace opts]}]
      (let [other (directory! root "worker-two")
            control-two (directory! other "control")
            _ (Files/setPosixFilePermissions control-two owner-only)
            home-two (directory! other "home")
            tmp-two (directory! other "private-tmp")
            runtime-two (directory! other "private-runtime")
            opts-two (-> opts
                         (assoc :workspace (str other)
                                :control-dir (str control-two)
                                :worker-env {"HOME" (str home-two)
                                             "TMPDIR" (str tmp-two)
                                             "XDG_RUNTIME_DIR" (str runtime-two)}))
            first-plan (admission/plan! opts)
            second-plan (admission/plan! opts-two)]
        (is (not= (:workspace first-plan) (:workspace second-plan)))
        (is (not= (:control-dir first-plan) (:control-dir second-plan)))
        (is (has-pair? (:argv first-plan) "--bind" (str workspace)))
        (is (has-pair? (:argv second-plan) "--bind" (str other)))
        (is (not (has-pair? (:argv first-plan) "--bind" (str other))))
        (is (not (has-pair? (:argv second-plan) "--bind" (str workspace))))
        (is (= (:isolation first-plan) (:isolation second-plan)))
        (is (= :unverified-plan (:status first-plan) (:status second-plan)))))))

(deftest command-and-binary-validation-is-fail-closed
  (with-fixture
    (fn [{:keys [opts]}]
      (doseq [command [nil [] "java -version" '("java")
                       ["java" nil] ["java" ""] ["java\u0000bad"]]]
        (is (= :configuration (option-reason (assoc opts :command command)))
            (pr-str command)))
      (doseq [[key value] [[:bwrap "./bwrap"] [:setsid "../../setsid"]
                           [:bwrap "/does-not-exist/bwrap"]
                           [:setsid "/dev/null"]
                           [:bwrap "bwrap --bad"]]]
        (is (= :configuration (option-reason (assoc opts key value)))
            (pr-str [key value]))))))

(deftest run-basket-must-be-outside-worker-tree
  (with-fixture
    (fn [{:keys [root workspace opts]}]
      (is (= :run-overlap (option-reason (assoc opts :run-dir (str root)))))
      (let [nested (directory! workspace "nested-run")]
        (is (= :run-overlap
               (option-reason (assoc opts :run-dir (str nested)))))))))

(deftest control-directory-must-be-private-and-inside-clone
  (with-fixture
    (fn [{:keys [run control opts]}]
      (is (= :unsafe-control
             (option-reason (assoc opts :control-dir (str run)))))
      (Files/setPosixFilePermissions control
                                    (conj owner-only PosixFilePermission/GROUP_READ))
      (is (= :unsafe-control (option-reason opts)))
      (Files/setPosixFilePermissions control
                                    (conj owner-only PosixFilePermission/OTHERS_EXECUTE))
      (is (= :unsafe-control (option-reason opts)))
      (Files/setPosixFilePermissions control owner-only)
      (is (= :unverified-plan (:status (admission/plan! opts)))))))

(deftest worker-environment-must-be-valid-and-contained
  (with-fixture
    (fn [{:keys [root home run opts]}]
      (is (= :configuration (option-reason (assoc opts :worker-env nil))))
      (doseq [key ["HOME" "TMPDIR" "XDG_RUNTIME_DIR"]]
        (is (= :configuration
               (option-reason (update opts :worker-env dissoc key)))
            (str "missing " key))
        (is (= :unsafe-environment
               (option-reason (assoc-in opts [:worker-env key] (str run))))
            (str "escaped " key)))
      (is (= :configuration
             (option-reason (assoc-in opts [:worker-env "HOME"] "relative/home"))))
      (is (= :configuration
             (option-reason (assoc-in opts [:worker-env "TMPDIR"] (str (.resolve root "missing"))))))
      (is (= :configuration
             (option-reason (assoc-in opts [:worker-env "bad-name"] "oops"))))
      (is (= :configuration
             (option-reason (assoc-in opts [:worker-env "UPPER"] 3))))
      (is (= :unverified-plan
             (:status (admission/plan!
                       (assoc-in opts [:worker-env "EXTRA_FLAG"] "ok"))))))))

(deftest symbolic-link-ancestors-are-not-trusted
  (with-fixture
    (fn [{:keys [root workspace home opts]}]
      (let [alias (.resolve root "alias")]
        (Files/createSymbolicLink alias workspace empty-attrs)
        (is (= :unsafe-path
               (option-reason
                (assoc opts :workspace (str (.resolve alias "home"))
                       :control-dir (str (.resolve alias "home/control")))))
            "A symlinked ancestor must be refused even when the leaf exists"))
      (let [outside (.resolve root "elsewhere")]
        (Files/createSymbolicLink outside home empty-attrs)
        (is (= :configuration
               (option-reason (assoc-in opts [:worker-env "HOME"] (str outside)))))))))

(deftest read-only-dependency-cache-bind-contract
  (with-fixture
    (fn [{:keys [opts host-home home]}]
      (let [cache (directory! host-home ".m2")
            dest (directory! home ".m2")
            _ (directory! home ".gitlibs")
            _ (directory! home ".clojure")
            with-cache (assoc opts :host-home (str host-home))
            argv (:argv (admission/plan! with-cache))]
        (is (has-pair? argv "--ro-bind" (str cache)))
        (is (some #{(str dest)} argv))
        (is (not (has-pair? argv "--bind" (str cache))))
        (is (= 1 (count (filter #(= % (str cache)) argv))))
        (let [extra (directory! host-home ".gitlibs")
              gitlibs-dest (.resolve home ".gitlibs")]
          (remove-tree! gitlibs-dest)
          (is (= :unsafe-cache (option-reason with-cache))
              "A present cache may not bind to a missing private destination")
          (directory! home ".gitlibs")
          (is (has-pair? (:argv (admission/plan! with-cache)) "--ro-bind" (str extra))))
        (remove-tree! dest)
        (Files/createSymbolicLink dest cache empty-attrs)
        (is (= :unsafe-cache (option-reason with-cache))
            "Destination symlink must never acquire writable or ro-bind rights")))))

(deftest source-cache-symlink-is-rejected
  (with-fixture
    (fn [{:keys [root host-home home opts]}]
      (let [real (directory! root "cached-files")
            src (.resolve host-home ".clojure")]
        (doseq [name [".m2" ".gitlibs" ".clojure"]]
          (directory! home name))
        (Files/createSymbolicLink src real empty-attrs)
        (is (= :unsafe-cache
               (option-reason (assoc opts :host-home (str host-home)))))))))

(deftest a-successful-child-exit-is-not-sandbox-admission
  (with-fixture
    (fn [{:keys [opts]}]
      ;; /usr/bin/true does nothing, but exits zero. The real admission probe
      ;; must still reject it because no nonce-authenticated evidence exists.
      (let [options (assoc opts :bwrap "/usr/bin/true")]
        (is (= :sandbox-rejected
               (reason-of #(admission/probe! options))))
        (is (= :unverified-plan (:status (admission/plan! opts))))))))

(deftest rejected-processes-cannot-admit-a-worker
  (with-fixture
    (fn [{:keys [run opts]}]
      (let [options (assoc opts :bwrap "/usr/bin/false")]
        (is (= :sandbox-rejected
               (reason-of #(admission/probe! options))))
        (is (empty? (seq (.listFiles (.toFile run))))
            "A refused probe must never write an authoritative job result")))))

(deftest missing-probe-executable-fails-before-subprocess
  (with-fixture
    (fn [{:keys [opts]}]
      (is (= :configuration
             (reason-of #(admission/probe!
                          (assoc opts :probe-bb "/definitely/missing-bb"))))))))

(defn- executable-on-path [name]
  (some (fn [dir]
          (let [file (File. (str dir) name)]
            (when (and (.isFile file) (.canExecute file))
              (.getAbsolutePath file))))
        (str/split (or (System/getenv "PATH") "")
                   (re-pattern (java.util.regex.Pattern/quote File/pathSeparator)))))

(defn- native-bwrap []
  (or (not-empty (System/getenv "SHARD_TEST_BWRAP"))
      (executable-on-path "bwrap")))

(defn- await-file! [^Path ready ^Process process timeout-ms]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (loop []
      (cond
        (Files/exists ready no-follow) true
        (not (.isAlive process))
        (throw (ex-info "Native Bubblewrap worker exited before its port bound"
                        {:exit (.exitValue process)}))
        (>= (System/nanoTime) deadline)
        (throw (ex-info "Native Bubblewrap worker never bound its port"
                        {:timeout-ms timeout-ms}))
        :else (do (Thread/sleep 25) (recur))))))

(defn- native-port-expression [^Path ready ^Path release]
  (str "(let [ready " (pr-str (str ready)) " release " (pr-str (str release)) " "
       "deadline (+ (System/nanoTime) 12000000000)] "
       "(with-open [server (java.net.ServerSocket. 39578 1 (java.net.InetAddress/getByName \"127.0.0.1\"))] "
       "(spit ready \"ready\") "
       "(loop [] (when-not (.exists (java.io.File. release)) "
       "(if (< (System/nanoTime) deadline) "
       "(do (Thread/sleep 25) (recur)) "
       "(throw (ex-info \"Native port worker deadline\" {})))))) "
       "(println \"NATIVE-PORT-OK\"))"))

(defn- start-native! [plan ^Path root suffix]
  (let [pb (ProcessBuilder. ^java.util.List (:argv plan))]
    (.directory pb (.toFile (Path/of ^String (:cwd plan) (make-array String 0))))
    (.putAll (.environment pb) (:env plan))
    (.redirectErrorStream pb true)
    (.redirectOutput pb (.toFile (.resolve root (str suffix ".log"))))
    (.start pb)))

(defn- stop-native! [^Process process]
  (when process
    (when (.isAlive process)
      ;; This is emergency TEST cleanup, not production process-group
      ;; supervision: it must never be used as a production admission proof.
      (with-open [children (.descendants (.toHandle process))]
        (doseq [^java.lang.ProcessHandle child (reverse (vec (iterator-seq (.iterator children))))]
          (.destroyForcibly child)))
      (.destroyForcibly process))
    (.waitFor process 5 java.util.concurrent.TimeUnit/SECONDS)))

(defn- two-native-workers-share-a-port! [{:keys [root workspace opts]} bb bwrap]
  (let [other (directory! root "second-worker")
        control-two (directory! other "control")
        _ (Files/setPosixFilePermissions control-two owner-only)
        home-two (directory! other "home")
        tmp-two (directory! other "private-tmp")
        runtime-two (directory! other "private-runtime")
        first-ready (.resolve workspace "port.ready")
        first-release (.resolve workspace "port.release")
        second-ready (.resolve other "port.ready")
        second-release (.resolve other "port.release")
        first-plan (admission/plan!
                    (assoc opts :bwrap bwrap
                           :command [bb "-e" (native-port-expression first-ready first-release)]))
        second-plan (admission/plan!
                     (assoc opts :bwrap bwrap :workspace (str other)
                            :control-dir (str control-two)
                            :worker-env {"HOME" (str home-two)
                                         "TMPDIR" (str tmp-two)
                                         "XDG_RUNTIME_DIR" (str runtime-two)}
                            :command [bb "-e" (native-port-expression second-ready second-release)]))
        p1 (atom nil)
        p2 (atom nil)]
    (try
      (reset! p1 (start-native! first-plan root "native-first"))
      (await-file! first-ready @p1 10000)
      (reset! p2 (start-native! second-plan root "native-second"))
      (await-file! second-ready @p2 10000)
      (is (and (.isAlive ^Process @p1) (.isAlive ^Process @p2))
          "Both isolated workers bind the SAME loopback port concurrently")
      (spit (str first-release) "release")
      (spit (str second-release) "release")
      (is (.waitFor ^Process @p1 5000 java.util.concurrent.TimeUnit/MILLISECONDS))
      (is (.waitFor ^Process @p2 5000 java.util.concurrent.TimeUnit/MILLISECONDS))
      (when-not (.isAlive ^Process @p1)
        (is (zero? (.exitValue ^Process @p1))))
      (when-not (.isAlive ^Process @p2)
        (is (zero? (.exitValue ^Process @p2))))
      (finally
        (spit (str first-release) "cleanup")
        (spit (str second-release) "cleanup")
        (stop-native! @p1)
        (stop-native! @p2)))))

(defn- native-probe! [bwrap]
  ;; The source-tree location remains reachable when /tmp is mounted private.
  (let [root (Path/of (System/getProperty "user.dir") (make-array String 0))]
    (with-fixture root
      (fn [{:keys [opts workspace] :as fixture}]
        (let [bb (or (not-empty (System/getenv "SHARD_TEST_BB"))
                     (executable-on-path "bb"))
              plan-opts (assoc opts :bwrap bwrap :probe-bb bb)
              result (admission/probe! plan-opts)]
          (is (= :probe-passed (:status result)))
          (is (= (str workspace) (:workspace result)))
          (is (= #{:user :ipc :pid :network :mount :private-tmp :private-shm}
                 (:isolation result)))
          (two-native-workers-share-a-port! fixture bb bwrap))))))

(deftest native-fixed-port-command-is-well-formed
  (with-fixture
    (fn [{:keys [workspace]}]
      (let [expression (native-port-expression
                        (.resolve workspace "ready") (.resolve workspace "release"))]
        (is (list? (read-string expression)))
        (is (str/includes? expression "39578"))
        (is (str/includes? expression "NATIVE-PORT-OK"))))))

(deftest native-bubblewrap-probe-is-an-explicit-acceptance-gate
  (let [bwrap (native-bwrap)
        required? (= "1" (System/getenv "SHARD_TEST_REQUIRE_BWRAP"))]
    (cond
      (nil? bwrap)
      (if required?
        (is false "Native Bubblewrap is REQUIRED but bwrap is unavailable")
        (do (println "SKIP: native Bubblewrap admission; set SHARD_TEST_REQUIRE_BWRAP=1 to require it")
            (is true "Native gate explicitly skipped (not certified)")))
      :else
      (if required?
        (native-probe! bwrap)
        ;; Default tests never claim native admission in a host/container with
        ;; restricted user namespaces. Opt in explicitly for full acceptance.
        (do (println "SKIP: native Bubblewrap admission; set SHARD_TEST_REQUIRE_BWRAP=1 to run it")
            (is true "Native gate explicitly skipped (not certified)"))))))
