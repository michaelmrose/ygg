(ns shard.runtime.jvm-session
  "Host-side, test-only first implementation of a persistent JVM session.

  A host launches ONE JVM, accepts its connection on a private Unix-domain
  socket, validates the launch handshake and then runs several independently
  identified test namespaces over that SAME process. This namespace NEVER
  claims jobs or reports a run as complete: Shard's external supervisor owns
  the immutable inventory, atomic claims, scheduling, logs and reconciliation.

  SECURITY BOUNDARY (v1114): `start!` admits ONLY :test-only? true. It does not
  implement Bubblewrap admission or independently attest a runtime fingerprint;
  therefore it MUST NOT be used in a production Shard run. Subsequent revisions
  must bind the process to an admitted sandbox and verified runtime identity.

  Per-command timeout, socket corruption, unexpected EOF, and shutdown failure
  POISON the session. This version kills the direct child as a test-only safety
  precaution, NOT as proof of complete descendant/process-group termination.
  Production use must wait for supervised process-group teardown and evidence."
  (:refer-clojure :exclude [run!])
  (:require [shard.protocol :as protocol])
  (:import [java.io File InputStream OutputStream]
           [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels Channels ServerSocketChannel SocketChannel]
           [java.nio.file Files Path]
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

(defn- stop-child!
  "Test-only direct-child teardown. Never treated as process-group evidence."
  [^Process process]
  (when process
    (when (.isAlive process)
      (.destroyForcibly process))
    (when-not (.waitFor process 5 TimeUnit/SECONDS)
      (refuse! :termination-unverified "JVM child has not terminated" {:pid (.pid process)}))))

(defn- release-transport! [session]
  (close-quietly! (:channel session))
  (close-quietly! (:listener session)))

(defn- cleanup-socket! [session]
  (when-let [^Path dir (:socket-dir session)]
    (Files/deleteIfExists (.resolve dir "control.sock"))
    (Files/deleteIfExists dir)))

(defn- poison!
  "Invalidate session before stopping a failed test-only child."
  [session]
  (reset! (:phase session) :poisoned)
  (release-transport! session)
  (stop-child! (:process session))
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
  (require-valid! (= (.pid ^Process (:process session)) (:pid reply)) :pid
                  "JVM reply PID is not the launched process")
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
  "Start ONE test-only JVM and establish its typed control session.

  Requires {:test-only? true, :command [string ...], :cwd path,
  :control-dir path, :stdout-file path, :stderr-file path, :worker-id string,
  :runtime-fingerprint 64-hex-string}. Optional :startup-timeout-ms and
  :shutdown-timeout-ms are positive millisecond bounds.

  The caller MUST supply a real JVM command loading shard.worker.jvm, usually
  [java -cp <classpath> clojure.main -m shard.worker.jvm]. All test stdout and
  stderr go to distinct caller-selected files. No shell is involved.

  The returned session is mutable and MUST be closed via stop! or abort!.
  It is NOT an admission token and MUST NOT be used as proof of sandboxing."
  [{:keys [test-only? command cwd control-dir stdout-file stderr-file
           worker-id runtime-fingerprint startup-timeout-ms shutdown-timeout-ms]}]
  (require-valid! (true? test-only?) :not-admitted
                  "v1114 JVM launcher is test-only; production sandbox admission is not implemented")
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
           :host-to-worker)]
    (require-valid! (Files/isDirectory cwd (make-array java.nio.file.LinkOption 0))
                    :configuration "Working directory does not exist")
    (require-valid! (Files/isDirectory control-dir (make-array java.nio.file.LinkOption 0))
                    :configuration "Control parent directory does not exist")
    (require-valid! (and (not= stdout stderr)
                         (Files/isDirectory (.getParent stdout) (make-array java.nio.file.LinkOption 0))
                         (Files/isDirectory (.getParent stderr) (make-array java.nio.file.LinkOption 0)))
                    :configuration "Independent log parent directories must exist")
    ;; Only caller-specified values are forwarded; mandatory identity overrides
    ;; any inherited environment and is never taken from untrusted job input.
    (let [socket-dir (Files/createTempDirectory control-dir "jvm-session-"
                                               (make-array java.nio.file.attribute.FileAttribute 0))
          listener (atom nil)
          process (atom nil)
          channel (atom nil)
          session-base {:identity identity :socket-dir socket-dir
                        :startup-timeout-ms startup-ms :shutdown-timeout-ms shutdown-ms}]
      (try
        (let [socket (.resolve socket-dir "control.sock")
              server (ServerSocketChannel/open StandardProtocolFamily/UNIX)]
          (reset! listener server)
          (.bind server (UnixDomainSocketAddress/of socket))
          (let [builder (ProcessBuilder. ^java.util.List command)
                env (.environment builder)]
            (.directory builder (.toFile cwd))
            (.redirectOutput builder (.toFile stdout))
            (.redirectError builder (.toFile stderr))
            (.put env "SHARD_CONTROL_SOCKET" (str socket))
            (.put env "SHARD_SESSION_ID" (:session-id identity))
            (.put env "SHARD_WORKER_ID" worker-id)
            (.put env "SHARD_RUNTIME_FINGERPRINT" runtime-fingerprint)
            (reset! process (.start builder)))
          (reset! channel (accept-before! server @process startup-ms))
          (let [session (assoc session-base
                               :listener server :process @process :channel @channel
                               :input (Channels/newInputStream ^SocketChannel @channel)
                               :output (Channels/newOutputStream ^SocketChannel @channel)
                               :phase (atom :starting)
                               :next-seq (atom 1)
                               :completed-job-ids (atom #{}))
                request (merge {:version protocol/protocol-version
                                :type :hello :seq 0} identity)
                ready (bounded-exchange! session request startup-ms)]
            (verify-reply! session request ready #{:ready})
            (reset! (:phase session) :active)
            (assoc session :pid (:pid ready))))
        (catch Throwable t
          (close-quietly! @channel)
          (close-quietly! @listener)
          (stop-child! @process)
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

  An unresponsive JVM is poisoned and forcibly terminated (DIRECT child only).
  v1114 must not be used to claim complete process-group cleanup."
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
          (reset! (:phase session) :stopped)
          (release-transport! session)
          (cleanup-socket! session)
          {:status :stopped :pid (:pid session) :jobs (count @(:completed-job-ids session))})
        (catch Throwable t
          (poison! session)
          (throw t))))))

(defn abort!
  "Test-only forced session teardown. Never evidence of safe group cleanup."
  [session]
  (locking session
    (when-not (= :stopped @(:phase session))
      (poison! session))
    nil))
