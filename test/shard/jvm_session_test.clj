(ns shard.jvm-session-test
  "Regression and live-process tests for the host-side persistent JVM session.

  Ordinary `bb test` exercises fail-closed *pre-launch* admission checks.
  Set SHARD_TEST_JVM_CLASSPATH to the real Clojure runtime JAR classpath to
  additionally exercise TWO independent real JVMs over Unix-domain control
  sockets. SHARD_TEST_REQUIRE_JVM=1 rejects an absent native fixture instead
  of silently claiming that the persistent-process gate passed.

  The launcher remains TEST ONLY in v1114. These tests do not certify a
  Bubblewrap namespace, runtime attestation, or descendant process teardown.
  Shard's scheduler and production worker admission are later milestones."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [shard.runtime.jvm-session :as session])
  (:import [java.io File]
           [java.nio.file Files Path LinkOption]
           [java.util.concurrent TimeUnit]))

(def ^:private fingerprint (apply str (repeat 64 "a")))
(def ^:private nofollow (make-array LinkOption 0))

(defn- reason-of [f]
  (try
    (f)
    :unexpected-success
    (catch clojure.lang.ExceptionInfo e
      (or (:shard.runtime.jvm-session/reason (ex-data e))
          (:shard.protocol/reason (ex-data e))
          :unclassified-exception))))

(defn- directory-empty? [^Path dir]
  (with-open [stream (Files/list dir)]
    (not (.hasNext (.iterator stream)))))

(defn- tree-delete! [^Path root]
  (when (Files/exists root nofollow)
    (doseq [^File f (reverse (file-seq (.toFile root)))]
      (Files/deleteIfExists (.toPath f)))))

(defn- with-temp-dir [f]
  (let [temp (Files/createTempDirectory "shard-jvm-session-test-"
                                        (make-array java.nio.file.attribute.FileAttribute 0))]
    (try (f temp)
         (finally (tree-delete! temp)))))

(defn- options [^Path root worker-id]
  {:test-only? true
   :command ["/never/launch/this/jvm"]
   :cwd (str root)
   :control-dir (str root)
   :stdout-file (str (.resolve root (str worker-id ".out")))
   :stderr-file (str (.resolve root (str worker-id ".err")))
   :worker-id worker-id
   :runtime-fingerprint fingerprint})

(deftest production-launch-is-always-refused-before-spawning
  (with-temp-dir
    (fn [root]
      (let [opts (options root "worker-1")]
        (doseq [bad [(dissoc opts :test-only?)
                     (assoc opts :test-only? false)
                     (assoc opts :test-only? :yes)]]
          (is (= :not-admitted (reason-of #(session/start! bad)))))
        (is (directory-empty? root)
            "Refused production launch must not create a control socket")))))

(deftest invalid-launch-configuration-is-refused-before-spawning
  (with-temp-dir
    (fn [root]
      (let [opts (options root "worker-1")
            missing (.resolve root "missing")]
        (doseq [[label overrides expected]
                [["empty executable" {:command []} :configuration]
                 ["shell string instead of argv" {:command "java -version"} :configuration]
                 ["bad startup bound" {:startup-timeout-ms 0} :configuration]
                 ["bad shutdown bound" {:shutdown-timeout-ms 86400001} :configuration]
                 ["missing cwd" {:cwd (str missing)} :configuration]
                 ["missing control parent" {:control-dir (str missing)} :configuration]
                 ["identical log files" {:stderr-file (:stdout-file opts)} :configuration]
                 ["invalid worker identity" {:worker-id "../other"} :worker]
                 ["invalid runtime fingerprint" {:runtime-fingerprint "unknown"} :runtime]]]
          (testing label
            (is (= expected
                   (reason-of #(session/start! (merge opts overrides)))))))
        (is (directory-empty? root))))))

(deftest child-exit-before-handshake-is-never-ready
  (with-temp-dir
    (fn [root]
      ;; The command is a real child, but does not speak the Shard protocol.
      ;; It must never become a usable session or leave its socket behind.
      (let [java (str (System/getProperty "java.home") File/separator "bin"
                      File/separator "java")
            opts (assoc (options root "early-exit")
                        :command [java "-version"]
                        :startup-timeout-ms 4000)
            failure (reason-of #(session/start! opts))]
        (is (= :child-exited failure))
        (is (not-any? #(.startsWith (.getName ^File %) "jvm-session-")
                      (.listFiles (.toFile root)))
            "Failed launch must remove its private control socket")
        (is (Files/exists (.resolve root "early-exit.err") nofollow)
            "Failed child output remains available for diagnosis")))))

(defn- fixture-ns! [^Path root namespace body]
  (let [relative (str (-> namespace
                          (str/replace "." "/")
                          (str/replace "-" "_")) ".clj")
        p (.resolve root relative)]
    (Files/createDirectories (.getParent p)
                             (make-array java.nio.file.attribute.FileAttribute 0))
    (spit (str p)
          (str "(ns " namespace " (:require [clojure.test :refer [deftest is]]))\n"
               body "\n"))
    p))

(defn- java-cmd [classpath]
  [(str (System/getProperty "java.home") File/separator "bin"
        File/separator "java")
   "-Xmx256m" "-cp" classpath "clojure.main" "-m" "shard.worker.jvm"])

(defn- live-options [^Path root classpath worker-id]
  (assoc (options root worker-id)
         :command (java-cmd classpath)
         :startup-timeout-ms 30000
         :shutdown-timeout-ms 10000))

(defn- wait-until! [pred timeout-ms message]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (loop []
      (cond
        (pred) true
        (>= (System/nanoTime) deadline)
        (throw (ex-info message {:deadline-ms timeout-ms}))
        :else (do (Thread/sleep 15) (recur))))))

(defn- worker-error? [reply reason]
  (and (= :error (:type reply)) (= reason (:reason reply))))

(defn- live-two-jvms! [^Path root runtime-classpath]
  (let [source-dir (.toAbsolutePath (Path/of "src" (make-array String 0)))
        classpath (str runtime-classpath File/pathSeparator source-dir
                       File/pathSeparator root)
        started (.resolve root "slow.started")
        release (.resolve root "slow.release")
        started-text (pr-str (str started))
        release-text (pr-str (str release))
        slow-body (str "(deftest wait-for-host\n"
                       "  (spit " started-text " \"ready\")\n"
                       "  (let [deadline (+ (System/nanoTime) 10000000000)]\n"
                       "    (loop []\n"
                       "      (when-not (.exists (java.io.File. " release-text "))\n"
                       "        (if (< (System/nanoTime) deadline)\n"
                       "          (do (Thread/sleep 15) (recur))\n"
                       "          (throw (ex-info \"Host never released slow job\" {}))))))\n"
                       "  (is true))")
        _ (fixture-ns! root "shard.session-pass-test"
                       "(deftest passes (println \"JVM-TEST-STDOUT\") (is (= 2 (+ 1 1))) (is true))")
        _ (fixture-ns! root "shard.session-fail-test"
                       "(deftest fails (is (= :left :right)))")
        _ (fixture-ns! root "shard.session-slow-test" slow-body)
        first-session (atom nil)
        second-session (atom nil)
        slow-finish (promise)
        slow-thread (atom nil)]
    (try
      (reset! first-session (session/start! (live-options root classpath "worker.1")))
      (reset! second-session (session/start! (live-options root classpath "worker.2")))
      (let [s1 @first-session
            s2 @second-session
            pid1 (:pid s1)
            pid2 (:pid s2)]
        (is (and (pos-int? pid1) (pos-int? pid2) (not= pid1 pid2))
            "Exactly two distinct live JVM processes are launched")
        (is (and (.isAlive ^Process (:process s1))
                 (.isAlive ^Process (:process s2))))
        (let [one-a (session/run! s1 "one-a" "shard.session-pass-test" 10000)
              two-a (session/run! s2 "two-a" "shard.session-pass-test" 10000)
              one-b (session/run! s1 "one-b" "shard.session-fail-test" 10000)
              two-b (session/run! s2 "two-b" "shard.session-missing-test" 10000)]
          (is (= [:passed :passed :failed nil]
                 (mapv :status [one-a two-a one-b two-b])))
          (is (worker-error? two-b :unknown-namespace))
          (is (= [:result :result :result :error]
                 (mapv :type [one-a two-a one-b two-b])))
          (is (= ["one-a" "two-a" "one-b" "two-b"]
                 (mapv :job-id [one-a two-a one-b two-b])))
          (is (= [1 1 2 2] (mapv :seq [one-a two-a one-b two-b])))
          (is (= [pid1 pid2 pid1 pid2]
                 (mapv :pid [one-a two-a one-b two-b])))
          (is (= [2 2 0] (mapv :passes [one-a two-a one-b])))
          (is (= [0 0 1] (mapv :failures [one-a two-a one-b])))
          (is (every? #(= 2 (:assertions %)) [one-a two-a]))
          (is (= :duplicate-job
                 (reason-of #(session/run! s1 "one-a" "shard.session-pass-test" 10000))))
          (is (= :active @(:phase s1)) "Duplicate refusal must not poison healthy JVM")
          (is (= 3 @(:next-seq s1)) "Rejected duplicate MUST NOT consume sequence"))
        ;; This is real simultaneous work, not just round-robin serial dispatch:
        ;; JVM 1 remains inside a blocked test while JVM 2 completes another.
        (let [t (Thread. (fn []
                           (deliver slow-finish
                                    (try {:reply (session/run! s1 "one-c"
                                                               "shard.session-slow-test" 15000)}
                                         (catch Throwable e {:exception e}))))
                         "shard-test-slow-control")]
          (.setDaemon t true)
          (reset! slow-thread t)
          (.start t)
          (wait-until! #(Files/exists started nofollow) 10000
                       "First JVM never entered the coordinated slow test")
          (is (.isAlive t) "First worker still busy executing its independent job")
          (let [two-c (session/run! s2 "two-c" "shard.session-pass-test" 10000)]
            (is (= :passed (:status two-c)))
            (is (= pid2 (:pid two-c)))
            (is (.isAlive t) "Second worker completed while first was still busy"))
          (spit (str release) "go")
          (let [outcome (deref slow-finish 10000 ::not-delivered)]
            (is (map? outcome))
            (is (nil? (:exception outcome)))
            (is (= :passed (get-in outcome [:reply :status])))
            (is (= pid1 (get-in outcome [:reply :pid]))))
          (.join t 2000)
          (is (not (.isAlive t)) "Slow-control thread must be finished"))
        ;; Poison one JVM on a real host deadline; the second must remain valid.
        (let [no-release (.resolve root "slow.release")]
          (Files/deleteIfExists no-release)
          (Files/deleteIfExists started)
          (is (= :response-timeout
                 (reason-of #(session/run! s1 "one-d" "shard.session-slow-test" 200))))
          (is (= :poisoned @(:phase s1)))
          (is (not (.isAlive ^Process (:process s1))))
          (is (= :session-not-active
                 (reason-of #(session/run! s1 "one-e" "shard.session-pass-test" 10000)))))
        (let [two-d (session/run! s2 "two-d" "shard.session-pass-test" 10000)]
          (is (= :passed (:status two-d)))
          (is (= pid2 (:pid two-d)))
          (is (= [1 2 3 4] (range 1 @(:next-seq s2))))
          (let [stop-result (session/stop! s2)]
            (is (= {:status :stopped :pid pid2 :jobs 4} stop-result))
            (is (= :stopped @(:phase s2)))
            (is (not (.isAlive ^Process (:process s2))))
            (is (= :session-not-active (reason-of #(session/stop! s2))))))
        (is (str/includes? (slurp (str (.resolve root "worker.1.out")))
                           "JVM-TEST-STDOUT"))
        (is (str/includes? (slurp (str (.resolve root "worker.2.out")))
                           "JVM-TEST-STDOUT"))
        (is (not (str/includes? (slurp (str (.resolve root "worker.2.err")))
                                "Exception in thread"))))
      (finally
        (spit (str release) "cleanup")
        (doseq [s [@first-session @second-session]]
          (when s (session/abort! s)))
        (when-let [^Thread t @slow-thread] (.join t 3000))
        (is (not-any? #(and % (.isAlive ^Process (:process %)))
                      [@first-session @second-session])
            "No directly owned JVM may remain after fixture cleanup")))))

(deftest real-jvm-concurrency-and-failure-isolation
  (let [cp (not-empty (System/getenv "SHARD_TEST_JVM_CLASSPATH"))
        required? (= "1" (System/getenv "SHARD_TEST_REQUIRE_JVM"))]
    (cond
      cp (with-temp-dir #(live-two-jvms! % cp))
      required? (is false "Real JVM session evidence required but SHARD_TEST_JVM_CLASSPATH is absent")
      :else (println "SKIP real-jvm-concurrency-and-failure-isolation: set SHARD_TEST_JVM_CLASSPATH to run native two-JVM gate"))))
