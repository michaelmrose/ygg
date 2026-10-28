(ns shard.runtime.jvm-session
  "Host-side, supervised persistent JVM session for test-only or admitted execution.

  A host launches ONE JVM, accepts its connection on a private Unix-domain
  socket, validates the launch handshake and then runs several independently
  identified test namespaces over that SAME process. This namespace NEVER
  claims jobs or reports a run as complete: Shard's external supervisor owns
  the immutable inventory, atomic claims, scheduling, logs and reconciliation.

  In production, start! requires a complete :admission configuration. It
  executes the native admission probe, starts precisely the validated
  Bubblewrap/setsid invocation, checks process-group and namespace PID
  correspondence, and supervises the entire process group. No production
  bypass through :test-only? is allowed by the Shard CLI.

  Per-command timeout, socket corruption, unexpected EOF and shutdown failure
  POISON the session. If group cleanup cannot be verified, the session remains
  unsafe for workspace cleanup and the exception is retained."
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [shard.protocol :as protocol]
            [shard.runtime.admission :as admission])
  (:import [java.io File InputStream OutputStream]
           [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels Channels ServerSocketChannel SocketChannel]
           [java.nio.file Files Path LinkOption]
           [java.nio.file.attribute PosixFilePermission]
           [java.util.concurrent TimeUnit]))

(def ^:private default-startup-ms 20000)
(def ^:private default-shutdown-ms 10000)
(def ^:private timeout-value ::timed-out)

(defn- refuse!
  ([reason message] (refuse! reason message {}))
  ([reason message details]
   (throw (ex-info message (assoc details :shard.runtime.jvm-session/reason reason)))))

(defn- require-valid! [condition reason message]
  (when-not condition (refuse! reason message)))

(defn- pos-ms! [value name]
  (require-valid! (and (integer? value) (<= 1 value 86400000))
                  :configuration (str name " must be a positive bounded integer"))
  (long value))

(defn- path! [value name]
  (require-valid! (or (string? value) (instance? Path value) (instance? File value))
                  :configuration (str name " must be a path"))
  (.toAbsolutePath (cond
                     (instance? Path value) value
                     (instance? File value) (.toPath ^File value)
                     :else (Path/of ^String value (make-array String 0)))))

(defn- close-quietly! [closeable]
  (when closeable
    (try (.close ^java.lang.AutoCloseable closeable)
         (catch Exception _ nil))))

(def ^:private no-follow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private namespace-names ["user" "ipc" "pid" "net" "mnt"])

(defn- host-ns [name]
  (str (Files/readSymbolicLink
        (Path/of (str "/proc/self/ns/" name) (make-array String 0)))))

(defn- proc-details [pid]
  (let [base (Path/of (str "/proc/" pid) (make-array String 0))
        stat (.resolve base "stat")
        status (.resolve base "status")]
    (try
      (let [raw (Files/readString stat)
            i (.lastIndexOf raw ")")
            _ (require-valid! (pos? i) :proc-unavailable "Malformed process status")
            fields (str/split (subs raw (+ i 2)) #"\s+")
            _ (require-valid! (>= (count fields) 3) :proc-unavailable "Malformed process group")
            text (Files/readString status)
            pid-line (some #(when (str/starts-with? % "NSpid:") %)
                           (str/split-lines text))
            ns-pids (when pid-line
                      (mapv #(Long/parseLong %)
                            (re-seq #"[0-9]+" pid-line)))]
        {:pid (long pid) :state (first fields)
         :pgid (Long/parseLong (nth fields 2))
         :ns-pids ns-pids :base base})
      (catch java.nio.file.NoSuchFileException _ nil)
      (catch java.io.IOException e
        (if (Files/exists stat no-follow)
          (refuse! :proc-unavailable "Cannot inspect process status"
                   {:pid pid :cause (.getMessage e)})
          nil)))))

(defn- group-members! [pgid]
  (let [entries (.listFiles (File. "/proc"))]
    (require-valid! (some? entries) :proc-unavailable "Cannot enumerate /proc")
    (into [] (keep (fn [^File f]
                     (when (re-matches #"[0-9]+" (.getName f))
                       (when-let [details (proc-details (Long/parseLong (.getName f)))]
                         (when (and (= pgid (:pgid details))
                                    (not (#{"Z" "X"} (:state details))))
                           details))))) entries)))

(defn- signal-group! [signal pgid]
  (let [p (.start (ProcessBuilder. ^java.util.List
                                   ["kill" (str "-" signal) "--" (str "-" pgid)]))]
    (require-valid! (.waitFor p 3 TimeUnit/SECONDS)
                    :termination-unverified "Group signal helper did not exit")
    (zero? (.exitValue p))))

(defn- verify-group-leader! [^Process process]
  (let [pid (.pid process)]
    ;; setsid may not have execed when ProcessBuilder.start returns. Wait
    ;; briefly for the new PGID, never signal an unverified inherited group.
    (loop [attempt 0]
      (let [details (proc-details pid)]
        (cond
          (and details (= pid (:pgid details))) pid
          (or (not (.isAlive process)) (>= attempt 100))
          (refuse! :group-unverified
                   "Bubblewrap wrapper did not establish its own process group"
                   {:pid pid})
          :else (do (Thread/sleep 10) (recur (inc attempt))))))))

(defn- stop-child!
  "Terminate a test child directly, or a production process group; refuse
  to claim complete teardown unless no live group members remain."
  [^Process process admitted?]
  (when process
    (if-not admitted?
      (do (when (.isAlive process) (.destroyForcibly process))
          (require-valid! (.waitFor process 5 TimeUnit/SECONDS)
                          :termination-unverified "Test JVM child has not terminated"))
      (let [pgid (.pid process)]
        ;; Signal only a known group. Failure here MUST NOT be called cleanup.
        (when (.isAlive process)
          (try
            (verify-group-leader! process)
            (catch Throwable t
              ;; Unknown PGID: never send a negative-PID group signal. Try
              ;; best-effort direct/descendant teardown, but preserve the
              ;; original refusal as an unverified safety boundary.
              (doseq [^java.lang.ProcessHandle child (iterator-seq
                                            (.iterator (.descendants (.toHandle process))))]
                (.destroyForcibly child))
              (.destroyForcibly process)
              (.waitFor process 2 TimeUnit/SECONDS)
              (throw t))))
        (when (seq (group-members! pgid))
          (signal-group! "TERM" pgid))
        (.waitFor process 1 TimeUnit/SECONDS)
        (when (seq (group-members! pgid))
          (signal-group! "KILL" pgid))
        (.waitFor process 2 TimeUnit/SECONDS)
        (loop [tries 0]
          (let [live (group-members! pgid)]
            (when (seq live)
              (require-valid! (< tries 20) :termination-unverified
                              "Production JVM descendants remain alive")
              (Thread/sleep 100)
              (recur (inc tries)))))
        (require-valid! (not (.isAlive process)) :termination-unverified
                        "Sandbox group wrapper has not terminated")))))

(defn- attest-jvm-pid! [session namespace-pid]
  (let [pgid (:process-group session)
        members (group-members! pgid)
        matches (filterv
                 (fn [{:keys [pid ns-pids]}]
                   (and (> (count ns-pids) 1)
                        (= namespace-pid (peek ns-pids))
                        (every? (fn [name]
                                  (not= (host-ns name)
                                        (str (Files/readSymbolicLink
                                              (Path/of (str "/proc/" pid "/ns/" name)
                                                       (make-array String 0))))))
                                namespace-names))) members)]
    (require-valid! (= 1 (count matches)) :pid
                    "Cannot uniquely attest JVM PID inside isolated namespaces")
    (:pid (first matches))))

(defn- release-transport! [session]
  (close-quietly! (:channel session))
  (close-quietly! (:listener session)))

(defn- cleanup-socket! [session]
  (when-let [^Path dir (:socket-dir session)]
    (Files/deleteIfExists (.resolve dir "control.sock"))
    (Files/deleteIfExists dir)))

(defn- poison!
  "Invalidate before teardown; do not clean sockets if descendants remain."
  [session]
  (reset! (:phase session) :poisoned)
  (release-transport! session)
  (stop-child! (:process session) (:admitted? session))
  (cleanup-socket! session))

(defn- accept-before! [^ServerSocketChannel listener ^Process process timeout-ms]
  (.configureBlocking listener false)
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (loop []
      (if-let [channel (.accept listener)]
        channel
        (cond
          (not (.isAlive process))
          (refuse! :child-exited "JVM exited before connecting" {:exit (.exitValue process)})
          (<= deadline (System/nanoTime))
          (refuse! :startup-timeout "JVM did not connect before deadline")
          :else (do (Thread/sleep 15) (recur)))))))

(defn- bounded-exchange!
  "Execute blocking socket I/O with a deadline on a daemon control thread.

  A daemon (rather than Clojure's global future executor) ensures that a
  completed host JVM can exit without waiting for unused executor threads.
  The caller MUST close the socket and poison the child on timeout."
  [session request timeout-ms]
  (let [^OutputStream out (:output session)
        ^InputStream in (:input session)
        result (promise)
        thread (Thread.
                (fn []
                  (deliver result
                           (try
                             {:reply (do
                                       (protocol/write-frame! out request :host-to-worker)
                                       (protocol/read-frame! in :worker-to-host))}
                             (catch Throwable t {:failure t}))))
                (str "shard-jvm-control-" (:worker-id (:identity session))))]
    (.setDaemon thread true)
    (.start thread)
    (let [answer (deref result timeout-ms timeout-value)]
      (when (= timeout-value answer)
        (refuse! :response-timeout "Timed out awaiting JVM control reply"
                 {:type (:type request) :seq (:seq request)}))
      (when-let [failure (:failure answer)]
        (throw failure))
      (when-not (:reply answer)
        (refuse! :unexpected-eof "JVM control socket closed before reply"
                 {:type (:type request) :seq (:seq request)}))
      (:reply answer))))

(defn- verify-reply!
  [session request reply expected-type]
  (doseq [field [:session-id :worker-id :runtime-fingerprint]]
    (when-not (= (get (:identity session) field) (get reply field))
      (refuse! :identity "JVM reply has wrong identity"
               {:field field :seq (:seq request)})))
  (require-valid! (= (:seq request) (:seq reply)) :sequence
                  "JVM reply has wrong command sequence")
  (if (:admitted? session)
    (if (= :ready (:type reply))
      (let [host-pid (attest-jvm-pid! session (:pid reply))]
        (reset! (:attested-host-pid session) host-pid))
      (do
        (require-valid! (= (:namespace-pid session) (:pid reply)) :pid
                        "Reply JVM PID changed within isolated namespace")
        (when-not (= :stopped (:type reply))
          (require-valid! (= @(:attested-host-pid session)
                             (attest-jvm-pid! session (:pid reply))) :pid
                          "Host JVM process identity changed"))))
    (require-valid! (= (.pid ^Process (:process session)) (:pid reply)) :pid
                    "JVM reply PID is not the launched process"))
  (require-valid! (expected-type (:type reply)) :unexpected-response
                  "Unexpected JVM reply type")
  reply)

(defn- complete-result!
  [session request reply]
  (verify-reply! session request reply #{:result :error})
  (require-valid! (= (:job-id request) (:job-id reply)) :job
                  "JVM replied for a different or unknown claimed job")
  (when (= :result (:type reply))
    (require-valid! (= (:namespace request) (:namespace reply)) :namespace
                    "JVM result was attributed to a different namespace"))
  reply)

(defn start!
  "Start one persistent JVM with an explicit trust boundary.

  TEST: :test-only? true uses a direct JVM, never a production claim token.
  PRODUCTION: :admission must contain owned :workspace, :run-dir,
  :control-dir, :worker-env and permitted sandbox binaries. A genuine probe
  MUST succeed before launching the sandboxed JVM. :cwd/:control-dir must
  match the owned clone configuration. No unsandboxed fallback exists.

  The caller must stop!/abort! every returned session and retain workspaces
  when termination evidence is missing. This function does not claim jobs."
  [{:keys [test-only? admission command cwd control-dir stdout-file stderr-file
           worker-id runtime-fingerprint startup-timeout-ms shutdown-timeout-ms]}]
  (require-valid! (not= (true? test-only?) (some? admission)) :not-admitted
                  "Choose exactly one of explicit test-only or native admission")
  (require-valid! (and (vector? command) (seq command)
                       (every? #(and (string? %) (not (empty? %))) command))
                  :configuration "JVM command must be a nonempty vector of strings")
  (let [startup-ms (pos-ms! (or startup-timeout-ms default-startup-ms) :startup-timeout-ms)
        shutdown-ms (pos-ms! (or shutdown-timeout-ms default-shutdown-ms) :shutdown-timeout-ms)
        cwd (path! cwd :cwd)
        control-dir (path! control-dir :control-dir)
        stdout (path! stdout-file :stdout-file)
        stderr (path! stderr-file :stderr-file)
        identity {:session-id (protocol/new-session-id)
                  :worker-id worker-id :runtime-fingerprint runtime-fingerprint}
        _ (protocol/validate-message!
           (merge {:version protocol/protocol-version :type :hello :seq 0} identity)
           :host-to-worker)
        admitted? (some? admission)
        _ (when admitted?
            (require-valid! (map? admission) :configuration
                            "Production admission options must be a map")
            (require-valid! (= (path! (:workspace admission) :workspace) cwd)
                            :configuration "Session cwd must equal admitted workspace")
            (require-valid! (= (path! (:control-dir admission) :control-dir)
                               control-dir)
                            :configuration "Session control dir must be admitted"))]
    (require-valid! (Files/isDirectory cwd (make-array java.nio.file.LinkOption 0))
                    :configuration "Working directory does not exist")
    (require-valid! (Files/isDirectory control-dir (make-array java.nio.file.LinkOption 0))
                    :configuration "Control parent directory does not exist")
    (require-valid! (and (not= stdout stderr)
                         (Files/isDirectory (.getParent stdout) (make-array java.nio.file.LinkOption 0))
                         (Files/isDirectory (.getParent stderr) (make-array java.nio.file.LinkOption 0)))
                    :configuration "Independent log parent directories must exist")
    ;; A successful probe alone is not admission of the later JVM. The exact
    ;; launch plan and live process/PID must also be attested before :active.
    ;; This runs BEFORE socket creation or process startup.
    (let [launch (when admitted?
                   (let [options (assoc admission :command command)
                         checked-before (admission/plan! options)
                         _ (admission/probe! options)
                         checked-after (admission/plan! options)]
                     (require-valid! (= checked-before checked-after)
                                     :admission-changed
                                     "Sandbox plan changed between probe and launch")
                     checked-after))
          socket-dir (Files/createTempDirectory control-dir "jvm-session-"
                                               (make-array java.nio.file.attribute.FileAttribute 0))
          listener (atom nil)
          process (atom nil)
          channel (atom nil)
          session-base {:identity identity :socket-dir socket-dir
                        :admitted? admitted?
                        :startup-timeout-ms startup-ms :shutdown-timeout-ms shutdown-ms}]
      (try
        (let [socket (.resolve socket-dir "control.sock")
              server (ServerSocketChannel/open StandardProtocolFamily/UNIX)]
          (reset! listener server)
          (.bind server (UnixDomainSocketAddress/of socket))
          (let [builder (ProcessBuilder. ^java.util.List
                                         (if admitted? (:argv launch) command))
                env (.environment builder)]
            (.directory builder (.toFile cwd))
            (when admitted? (.putAll env (:env launch)))
            (.redirectOutput builder (.toFile stdout))
            (.redirectError builder (.toFile stderr))
            (.put env "SHARD_CONTROL_SOCKET" (str socket))
            (.put env "SHARD_SESSION_ID" (:session-id identity))
            (.put env "SHARD_WORKER_ID" worker-id)
            (.put env "SHARD_RUNTIME_FINGERPRINT" runtime-fingerprint)
            (reset! process (.start builder)))
          (when admitted? (verify-group-leader! @process))
          (reset! channel (accept-before! server @process startup-ms))
          (let [session (assoc session-base
                               :listener server :process @process :channel @channel
                               :input (Channels/newInputStream ^SocketChannel @channel)
                               :output (Channels/newOutputStream ^SocketChannel @channel)
                               :phase (atom :starting)
                               :process-group (when admitted? (.pid ^Process @process))
                               :attested-host-pid (atom nil)
                               :next-seq (atom 1)
                               :completed-job-ids (atom #{}))
                request (merge {:version protocol/protocol-version
                                :type :hello :seq 0} identity)
                ready (bounded-exchange! session request startup-ms)]
            (verify-reply! session request ready #{:ready})
            (reset! (:phase session) :active)
            (assoc session :pid (:pid ready)
                           :namespace-pid (:pid ready)
                           :host-jvm-pid @(:attested-host-pid session))))
        (catch Throwable t
          (close-quietly! @channel)
          (close-quietly! @listener)
          (stop-child! @process admitted?)
          (Files/deleteIfExists (.resolve socket-dir "control.sock"))
          (Files/deleteIfExists socket-dir)
          (throw t))))))

(defn run!
  "Run ONE namespace using this same JVM; claim ownership remains with Shard.

  Returns protocol :result or :error exactly as received and validated.
  Ordinary assertion failure is :result with :failed status and does not
  poison a healthy session. An execution error is NOT silently a pass.
  A lost/malformed/late response poisons the session and is thrown."
  [session job-id namespace timeout-ms]
  (locking session
    (require-valid! (= :active @(:phase session)) :session-not-active
                    "Cannot dispatch to a closed or poisoned JVM session")
    (let [seq-number @(:next-seq session)
          request (merge {:version protocol/protocol-version
                          :type :run :seq seq-number
                          :job-id job-id :namespace namespace :timeout-ms timeout-ms}
                         (:identity session))]
      (protocol/validate-message! request :host-to-worker)
      (require-valid! (not (contains? @(:completed-job-ids session) job-id))
                      :duplicate-job "Job already addressed to this JVM")
      (try
        (let [reply (->> (bounded-exchange! session request (long timeout-ms))
                         (complete-result! session request))]
          (swap! (:completed-job-ids session) conj job-id)
          (swap! (:next-seq session) inc)
          reply)
        (catch Throwable t
          (poison! session)
          (throw t))))))

(defn stop!
  "Request an orderly JVM shutdown and verify its exit status.

  An unresponsive JVM poisons the session; production teardown verifies that
  its process group has no remaining live members."
  [session]
  (locking session
    (require-valid! (= :active @(:phase session)) :session-not-active
                    "Cannot stop an inactive JVM session")
    (let [request (merge {:version protocol/protocol-version
                          :type :shutdown :seq @(:next-seq session)}
                         (:identity session))]
      (try
        (let [reply (bounded-exchange! session request (:shutdown-timeout-ms session))]
          (verify-reply! session request reply #{:stopped})
          (require-valid! (.waitFor ^Process (:process session)
                                    (:shutdown-timeout-ms session) TimeUnit/MILLISECONDS)
                          :shutdown-timeout "JVM failed to terminate after shutdown")
          (require-valid! (zero? (.exitValue ^Process (:process session)))
                          :nonzero-exit "JVM exited nonzero after shutdown")
          (when (:admitted? session)
            (require-valid! (empty? (group-members! (:process-group session)))
                            :termination-unverified
                            "Production JVM group still contains live descendants"))
          (reset! (:phase session) :stopped)
          (release-transport! session)
          (cleanup-socket! session)
          {:status :stopped :pid (:pid session) :jobs (count @(:completed-job-ids session))})
        (catch Throwable t
          (poison! session)
          (throw t))))))

(defn abort!
  "Force a poisoned session to stop; production mode verifies group cleanup."
  [session]
  (locking session
    (when-not (= :stopped @(:phase session))
      (poison! session))
    nil))
