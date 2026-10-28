(ns shard.jvm-worker-test
  "Regression tests for the Shard persistent JVM-side namespace executor.

  Ordinary `bb test` exercises the portable, real clojure.test transition path
  using caller-owned control streams. Set SHARD_TEST_JVM_CLASSPATH to the
  actual Clojure runtime JAR classpath to ALSO exercise a separate, persistent
  Java process with a real Unix-domain control socket. A supplied but unusable
  classpath MUST fail this suite; native execution is never silently faked.

  Host-side timeout, job claiming, runtime attestation, and Bubblewrap
  admission remain separate milestones. The JVM worker does none of those."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t :refer [deftest is testing]]
            [shard.protocol :as p]
            [shard.worker.jvm :as worker])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels Channels ServerSocketChannel]
           [java.nio.file Files Path]
           [java.util.concurrent TimeUnit]))

(def ^:private session (apply str (repeat 32 "a")))
(def ^:private runtime (apply str (repeat 64 "b")))
(def ^:private session-identity {:session-id session :worker-id "jvm.01"
                         :runtime-fingerprint runtime})

(defn- message
  ([type seq-number] (message type seq-number {}))
  ([type seq-number extra]
   (merge {:version p/protocol-version :type type :seq seq-number}
          session-identity extra)))

(defn- run-command [n job namespace]
  (message :run n {:job-id job :namespace namespace :timeout-ms 30000}))

(defn- input-bytes [messages]
  (let [out (ByteArrayOutputStream.)]
    (doseq [m messages]
      (p/write-frame! out m :host-to-worker))
    (.toByteArray out)))

(defn- results [^ByteArrayOutputStream out]
  (let [in (ByteArrayInputStream. (.toByteArray out))]
    (loop [seen []]
      (if-let [next-frame (p/read-frame! in :worker-to-host)]
        (recur (conj seen next-frame))
        seen))))

(defn- transcript
  "Call the real worker transition loop and decode only its control output."
  ([commands] (transcript commands session-identity))
  ([commands admitted]
   (let [out (ByteArrayOutputStream.)
         printed (java.io.StringWriter.)
         outcome (binding [*out* printed t/*test-out* printed]
                   (try
                     {:returned (worker/serve!
                                 (ByteArrayInputStream. (input-bytes commands))
                                 out admitted)}
                     (catch clojure.lang.ExceptionInfo e
                       {:reason (or (:shard.worker.jvm/reason (ex-data e))
                                    (:shard.protocol/reason (ex-data e)))})))]
     (assoc outcome :frames (results out) :stdout (str printed)))))

(defn- fixture-ns!
  "Install actual clojure.test vars without loading an additional source file."
  [namespace tests]
  (let [n (create-ns namespace)]
    (doseq [[name f] tests]
      (let [v (intern n name f)]
        (alter-meta! v assoc :test f)))
    namespace))

(defn- with-test-namespaces [f]
  (let [pass 'shard.jvm-worker-fixture-pass-test
        fail 'shard.jvm-worker-fixture-fail-test
        broken 'shard.jvm-worker-fixture-error-test
        empty 'shard.jvm-worker-fixture-empty-test]
    (try
      (fixture-ns! pass {'passes (fn [] (println "TEST-OUTPUT-NOT-CONTROL")
                                            (t/is (= 3 (+ 1 2)))
                                            (t/is (= :ok :ok)))})
      (fixture-ns! fail {'fails (fn [] (t/is (= 1 2)))})
      (fixture-ns! broken {'throws (fn [] (throw (ex-info "intentional test exception" {})))})
      (fixture-ns! empty {})
      ;; The fixtures already exist; suppress require's attempted filesystem load.
      ;; This is a test-only classpath substitute, not a fake result executor.
      (with-redefs [clojure.core/require (fn [& _] nil)]
        (f {:pass (str pass) :fail (str fail)
            :broken (str broken) :empty (str empty)}))
      (finally
        (doseq [sym [pass fail broken empty]]
          (when (find-ns sym) (remove-ns sym)))))))

(deftest real-test-results-reuse-one-inprocess-jvm
  (with-test-namespaces
    (fn [{:keys [pass fail broken empty]}]
      (let [commands [(message :hello 0)
                      (run-command 1 "one" pass)
                      (run-command 2 "two" fail)
                      (run-command 3 "three" broken)
                      (run-command 4 "four" empty)
                      (run-command 5 "five" pass)
                      (message :shutdown 6)]
            {:keys [frames stdout returned]} (transcript commands)
            [ready first-job second-job third-job fourth-job fifth-job stop] frames
            pid (:pid ready)]
        (is (= :stopped returned))
        (is (= 7 (count frames)))
        (is (= [:ready :result :result :result :error :result :stopped]
               (mapv :type frames)))
        (is (= (range 7) (map :seq frames)))
        (is (= #{pid} (set (map :pid frames))))
        (is (= (.pid (java.lang.ProcessHandle/current)) pid))
        (is (= (repeat 7 session-identity)
               (map #(select-keys % (keys session-identity)) frames)))
        (is (= [:passed :failed :failed :passed]
               (mapv :status [first-job second-job third-job fifth-job])))
        (is (= [2 0 0 2] (mapv :passes [first-job second-job third-job fifth-job])))
        (is (= [0 1 0 0] (mapv :failures [first-job second-job third-job fifth-job])))
        (is (= [0 0 1 0] (mapv :errors [first-job second-job third-job fifth-job])))
        (is (= ["one" "two" "three" "four" "five"]
               (mapv :job-id (rest (butlast frames)))))
        (is (= :execution-error (:reason fourth-job)))
        (is (= "four" (:job-id fourth-job)))
        (is (every? #(and (integer? (:elapsed-ms %)) (<= 0 (:elapsed-ms %)))
                    [first-job second-job third-job fifth-job]))
        (is (str/includes? stdout "TEST-OUTPUT-NOT-CONTROL"))
        (is (str/includes? stdout "intentional test exception"))
        ;; Decoding each frame succeeded despite arbitrary test stdout.
        (is (= :stopped (:type stop)))))))

(deftest missing-namespace-does-not-kill-session
  (with-test-namespaces
    (fn [{:keys [pass]}]
      (let [{:keys [frames returned]}
            (transcript [(message :hello 0)
                         (run-command 1 "bad" "shard.jvm-worker-not-found-test")
                         (run-command 2 "good" pass)
                         (message :shutdown 3)])]
        (is (= :stopped returned))
        (is (= [:ready :error :result :stopped] (mapv :type frames)))
        (is (= :unknown-namespace (:reason (nth frames 1))))
        (is (= "bad" (:job-id (nth frames 1))))
        (is (= :passed (:status (nth frames 2))))
        (is (= "good" (:job-id (nth frames 2))))))))

(deftest duplicate-job-is-fatal-after-first-result
  (with-test-namespaces
    (fn [{:keys [pass]}]
      (let [{:keys [frames reason]}
            (transcript [(message :hello 0)
                         (run-command 1 "same" pass)
                         (run-command 2 "same" pass)
                         (message :shutdown 3)])]
        (is (= :duplicate-job reason))
        (is (= [:ready :result] (mapv :type frames)))
        (is (= 1 (:seq (second frames))))))))

(deftest wrong-handshake-and-unexpected-eof-fail-closed
  (testing "invalid handshake does not emit readiness"
    (let [{:keys [reason frames]}
          (transcript [(message :shutdown 1)])]
      (is (= :handshake reason))
      (is (empty? frames))))
  (testing "wrong session-identity is never admitted"
    (let [{:keys [reason frames]}
          (transcript [(assoc (message :hello 0) :worker-id "other.worker")])]
      (is (= :identity reason))
      (is (empty? frames))))
  (testing "EOF following handshake is fatal, not a normal stop"
    (let [{:keys [reason frames]}
          (transcript [(message :hello 0)])]
      (is (= :eof reason))
      (is (= [:ready] (mapv :type frames)))))
  (testing "EOF after a completed job is not a successful run completion"
    (with-test-namespaces
      (fn [{:keys [pass]}]
        (let [{:keys [reason frames]}
              (transcript [(message :hello 0) (run-command 1 "only" pass)])]
          (is (= :eof reason))
          (is (= [:ready :result] (mapv :type frames))))))))

(deftest identity-sequence-and-message-types-are-fatal
  (doseq [[case-label commands expected]
          [["wrong runtime" [(message :hello 0)
                              (assoc (message :shutdown 1)
                                     :runtime-fingerprint (apply str (repeat 64 "c")))]
            :identity]
           ["wrong session" [(message :hello 0)
                              (assoc (message :shutdown 1)
                                     :session-id (apply str (repeat 32 "c")))]
            :identity]
           ["repeated sequence" [(message :hello 0)
                                  (run-command 1 "one" "shard.jvm-worker-not-found-test")
                                  (run-command 1 "two" "shard.jvm-worker-not-found-test")]
            :sequence]
           ["repeated handshake" [(message :hello 0) (message :hello 0)]
            :sequence]]]
    (testing case-label
      (let [{:keys [reason frames]} (transcript commands)]
        (is (= expected reason))
        (is (= :ready (:type (first frames))))))))

(deftest invalid-launch-identity-rejected-before-handshake
  (doseq [[admitted expected]
          [[(dissoc session-identity :session-id) :configuration]
           [(assoc session-identity :extra :unadmitted) :configuration]
           [(assoc session-identity :worker-id "unsafe/id") :worker]
           [(assoc session-identity :runtime-fingerprint "wrong") :runtime]]]
    (let [{:keys [reason frames]} (transcript [(message :hello 0)] admitted)]
      (is (= expected reason))
      (is (empty? frames)))))

(defn- delete-tree! [^Path root]
  (when (Files/exists root (make-array java.nio.file.LinkOption 0))
    (doseq [entry (reverse (file-seq (.toFile root)))]
      (Files/deleteIfExists (.toPath ^java.io.File entry)))))

(defn- fixture-file!
  [^Path base namespace test-body]
  (let [relative (str (str/replace (str/replace namespace "." "/") "-" "_") ".clj")
        path (.resolve base relative)]
    (Files/createDirectories (.getParent path) (make-array java.nio.file.attribute.FileAttribute 0))
    (spit (.toFile path)
          (str "(ns " namespace " (:require [clojure.test :refer [deftest is]]))\n"
               test-body "\n"))
    path))

(defn- java-command [classpath]
  [(str (System/getProperty "java.home") java.io.File/separator
        "bin" java.io.File/separator "java")
   "-Xmx256m" "-cp" classpath "clojure.main" "-m" "shard.worker.jvm"])

(defn- run-live-jvm!
  "Real process + UDS fixture. Deadlines bound startup and shutdown; always kill leftovers."
  [jar-classpath]
  (let [tmp (Files/createTempDirectory "shard-jvm-worker-test-"
                                       (make-array java.nio.file.attribute.FileAttribute 0))
        socket-path (.resolve tmp "control.sock")
        stdout (.resolve tmp "stdout.log")
        stderr (.resolve tmp "stderr.log")
        root (.toAbsolutePath (Path/of "" (make-array String 0)))
        classpath (str jar-classpath java.io.File/pathSeparator
                       (.resolve root "src") java.io.File/pathSeparator tmp)
        process (atom nil)]
    (try
      (fixture-file! tmp "shard.jvm-live-pass-test"
                     "(deftest pass (println \"LIVE-JVM-OUTPUT\") (is (= 1 1)) (is (= 4 4)))")
      (fixture-file! tmp "shard.jvm-live-fail-test"
                     "(deftest fail (is (= 1 2)))")
      (fixture-file! tmp "shard.jvm-live-empty-test" "(defn helper [] :no-tests)")
      (with-open [listener (ServerSocketChannel/open StandardProtocolFamily/UNIX)]
        (.bind listener (UnixDomainSocketAddress/of socket-path))
        (.configureBlocking listener false)
        (let [builder (ProcessBuilder. (java-command classpath))
              env (.environment builder)]
          (.directory builder (.toFile root))
          (.redirectOutput builder (.toFile stdout))
          (.redirectError builder (.toFile stderr))
          (.put env "SHARD_CONTROL_SOCKET" (str socket-path))
          (.put env "SHARD_SESSION_ID" session)
          (.put env "SHARD_WORKER_ID" (:worker-id session-identity))
          (.put env "SHARD_RUNTIME_FINGERPRINT" runtime)
          (reset! process (.start builder)))
        (let [deadline (+ (System/nanoTime) (* 20 1000000000))
              channel (loop []
                        (if-let [channel (.accept listener)]
                          channel
                          (if (< (System/nanoTime) deadline)
                            (do (Thread/sleep 25) (recur))
                            (throw (ex-info "JVM never connected to control socket"
                                            {:stdout (slurp (.toFile stdout))
                                             :stderr (slurp (.toFile stderr))})))))]
          (with-open [channel channel]
            (let [in (Channels/newInputStream channel)
                  out (Channels/newOutputStream channel)
                  exchange! (fn [msg]
                              (p/write-frame! out msg :host-to-worker)
                              (p/read-frame! in :worker-to-host))]
              (let [ready (exchange! (message :hello 0))
                    responses (mapv (fn [[n id namespace]]
                                      (exchange! (run-command n id namespace)))
                                    [[1 "pass-1" "shard.jvm-live-pass-test"]
                                     [2 "fail-2" "shard.jvm-live-fail-test"]
                                     [3 "zero-3" "shard.jvm-live-empty-test"]
                                     [4 "missing-4" "shard.jvm-live-missing-test"]
                                     [5 "pass-5" "shard.jvm-live-pass-test"]])
                    stopped (exchange! (message :shutdown 6))]
                (is (= :ready (:type ready)))
                (is (= (.pid ^Process @process) (:pid ready)))
                (is (= [:result :result :error :error :result]
                       (mapv :type responses)))
                (is (= [:passed :failed nil nil :passed]
                       (mapv :status responses)))
                (is (= [:execution-error :unknown-namespace]
                       (mapv :reason (subvec responses 2 4))))
                (is (= ["pass-1" "fail-2" "zero-3" "missing-4" "pass-5"]
                       (mapv :job-id responses)))
                (is (= (range 1 6) (map :seq responses)))
                (is (= #{(:pid ready)} (set (map :pid (concat responses [stopped])))))
                (is (= :stopped (:type stopped)))
                (is (= 6 (:seq stopped)))))))
        (is (.waitFor ^Process @process 12 TimeUnit/SECONDS)
            "Persistent JVM must exit after admitted shutdown")
        (when-not (.isAlive ^Process @process)
          (is (zero? (.exitValue ^Process @process))))
        (is (str/includes? (slurp (.toFile stdout)) "LIVE-JVM-OUTPUT"))
        (is (not (str/includes? (slurp (.toFile stderr)) "Exception in thread"))))
      (finally
        (when-let [^Process proc @process]
          (when (.isAlive proc)
            (.destroyForcibly proc)
            (.waitFor proc 5 TimeUnit/SECONDS)))
        (delete-tree! tmp)))))

(deftest real-jvm-process-reuses-the-same-pid
  ;; This is an explicit integration gate. A missing optional JVM runtime is
  ;; disclosed as not exercised, NOT advertised as native acceptance.
  (if-let [cp (not-empty (System/getenv "SHARD_TEST_JVM_CLASSPATH"))]
    (run-live-jvm! cp)
    (do
      (println "SKIP real-jvm-process-reuses-the-same-pid: set SHARD_TEST_JVM_CLASSPATH to the Clojure JAR classpath")
      (is true "Portable transition tests remain mandatory"))))
