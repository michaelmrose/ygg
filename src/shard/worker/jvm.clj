(ns shard.worker.jvm
  "Generic persistent JVM-side test executor for the Shard v1 control protocol.

  One JVM receives multiple independent namespace jobs. Each :run loads and
  executes its named clojure.test namespace IN THIS PROCESS; it never shells
  out to another JVM. The host alone owns immutable inventory, claims, job
  deadlines, workspace admission, log capture, and process-group termination.

  Control uses a separately configured Unix-domain socket, never stdin or
  stdout. SHARD_CONTROL_SOCKET, SHARD_SESSION_ID, SHARD_WORKER_ID, and
  SHARD_RUNTIME_FINGERPRINT are mandatory launch-time bindings. These are
  assertions of the host-admitted runtime identity, not a measurement of the
  classpath; host-side admission must establish that binding independently.

  A namespace may fail without stopping the JVM. A hung namespace cannot be
  recovered inside a trusted JVM: the host MUST time out and terminate the
  entire process group, retaining the unresolved job's evidence."
  (:require [clojure.test :as t]
            [shard.protocol :as protocol])
  (:import [java.io InputStream OutputStream]
           [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels Channels SocketChannel]
           [java.nio.file Path]))

(def ^:private required-launch-keys
  [:session-id :worker-id :runtime-fingerprint])

(defn- fail!
  ([code message] (fail! code message {}))
  ([code message data]
   (throw (ex-info message (assoc data :shard.worker.jvm/reason code)))))

(defn- launch-identity!
  "Validate the host-supplied identity before reading anything from the socket."
  [opts]
  (when-not (map? opts)
    (fail! :configuration "JVM worker identity must be a map"))
  (let [missing (filterv #(not (contains? opts %)) required-launch-keys)]
    (when (seq missing)
      (fail! :configuration "Missing JVM worker identity fields"
             {:missing missing})))
  (when-not (= (set required-launch-keys) (set (keys opts)))
    (fail! :configuration "Unexpected JVM worker identity fields"))
  (protocol/validate-message!
   (merge {:version protocol/protocol-version :type :hello :seq 0} opts)
   :host-to-worker)
  opts)

(defn- verify-binding!
  [identity message]
  (doseq [field required-launch-keys]
    (when-not (= (get identity field) (get message field))
      (fail! :identity "Control message does not match admitted JVM session"
             {:field field :type (:type message)}))))

(defn- response
  [identity type seq-number payload]
  (merge {:version protocol/protocol-version
          :type type
          :seq seq-number}
         identity payload))

(defn- worker-pid []
  (.pid (java.lang.ProcessHandle/current)))

(defn- error-message
  "Bound throwable text without relying on its arbitrary-length printed form."
  [^Throwable cause]
  (let [raw (str (.getName (class cause))
                 (when-let [detail (.getMessage cause)]
                   (str ": " detail)))
        normalized (if (empty? raw) "Unspecified test execution failure" raw)]
    (subs normalized 0 (min 2048 (count normalized)))))

(defn- run-tests!
  "Return a checked result payload for one namespace or an explicit error.

  `:passes` counts passing clojure.test assertions, NOT passed test vars.
  `:tests` counts visited test vars, as reported by clojure.test.
  A zero-test/zero-assertion run is NOT an admitted success."
  [{:keys [namespace job-id]}]
  (let [started (System/nanoTime)]
    (try
      (let [sym (symbol namespace)]
        (try
          (require sym)
          (catch java.io.FileNotFoundException e
            (fail! :unknown-namespace "Requested test namespace is absent"
                   {:namespace namespace :cause (.getMessage e)})))
        (when-not (find-ns sym)
          (fail! :unknown-namespace "Requested test namespace was not loaded"
                 {:namespace namespace}))
        (let [{:keys [test pass fail error] :as counters} (t/run-tests sym)
              elapsed-ms (quot (- (System/nanoTime) started) 1000000)]
          (when-not (and (every? integer? [test pass fail error])
                         (pos? test) (<= 0 pass) (<= 0 fail) (<= 0 error)
                         (pos? (+ pass fail error)))
            (fail! :execution-error "No verifiable test executions were recorded"
                   {:namespace namespace :counters counters}))
          {:type :result
           :job-id job-id :namespace namespace :pid (worker-pid)
           :status (if (zero? (+ fail error)) :passed :failed)
           :tests test :passes pass :failures fail :errors error
           :assertions (+ pass fail error) :elapsed-ms elapsed-ms}))
      (catch Exception e
        {:type :error :job-id job-id :pid (worker-pid)
         :reason (if (= :unknown-namespace
                        (:shard.worker.jvm/reason (ex-data e)))
                   :unknown-namespace :execution-error)
         :message (error-message e)}))))

(defn serve!
  "Serve one persistent JVM session on caller-owned, private control streams.

  The first incoming frame MUST be :hello with sequence zero. Every subsequent
  host frame MUST use a strictly increasing sequence; a :run receives exactly
  one :result or :error with that SAME sequence, job ID and session identity.
  A :shutdown receives :stopped and terminates the session. Unexpected EOF,
  malformed frames, identity/sequence failures and unknown message types are
  FATAL: the host sees a lost session, never a fabricated successful job.

  Timeouts are enforced by the host terminating this JVM's entire process
  group, not by unsafe thread interruption. Stream ownership remains with the
  caller. The function returns normally ONLY after accepted :shutdown."
  [^InputStream control-in ^OutputStream control-out launch-identity]
  (let [identity (launch-identity! launch-identity)
        hello (protocol/read-frame! control-in :host-to-worker)]
    (when-not (= :hello (:type hello))
      (fail! :handshake "JVM control session must begin with :hello"))
    (verify-binding! identity hello)
    (protocol/write-frame! control-out
                           (response identity :ready 0 {:pid (worker-pid)})
                           :worker-to-host)
    (loop [previous-seq 0 completed-job-ids #{}]
      (let [command (protocol/read-frame! control-in :host-to-worker)]
        (when-not command
          (fail! :eof "Control socket closed before admitted shutdown"))
        (verify-binding! identity command)
        (let [seq-number (:seq command)]
          (when-not (> seq-number previous-seq)
            (fail! :sequence "Nonmonotonic host control sequence"
                   {:received seq-number :previous previous-seq}))
          (case (:type command)
            :run (do
                   (when (contains? completed-job-ids (:job-id command))
                     (fail! :duplicate-job "Job ID repeated within live JVM session"
                            {:job-id (:job-id command)}))
                   (let [{:keys [type] :as outcome} (run-tests! command)]
                     (protocol/write-frame! control-out
                                            (response identity type seq-number
                                                      (dissoc outcome :type))
                                            :worker-to-host)
                     (recur seq-number (conj completed-job-ids (:job-id command)))))
            :shutdown (do
                        (protocol/write-frame! control-out
                                               (response identity :stopped seq-number
                                                         {:pid (worker-pid)})
                                               :worker-to-host)
                        :stopped)
            (fail! :lifecycle "Unexpected control message after :ready"
                   {:type (:type command)})))))))

(defn- required-env!
  [variable]
  (let [value (System/getenv variable)]
    (when (or (nil? value) (empty? value))
      (fail! :configuration "Missing mandatory JVM worker environment variable"
             {:variable variable}))
    value))

(defn -main
  "Connect to the host's admitted private Unix socket and serve jobs.

  The supervisor MUST supply the socket inside the sandbox, restrict its
  visibility/permissions and independently bind process, identity, classpath
  and job eligibility. This program never accepts stdin/stdout as control."
  [& args]
  (when (seq args)
    (fail! :configuration "JVM worker does not accept command-line arguments"))
  (let [identity {:session-id (required-env! "SHARD_SESSION_ID")
                  :worker-id (required-env! "SHARD_WORKER_ID")
                  :runtime-fingerprint (required-env! "SHARD_RUNTIME_FINGERPRINT")}
        socket-path (required-env! "SHARD_CONTROL_SOCKET")]
    (launch-identity! identity)
    (with-open [channel (SocketChannel/open StandardProtocolFamily/UNIX)]
      (.connect channel (UnixDomainSocketAddress/of (Path/of socket-path
                                                        (make-array String 0))))
      ;; Test output uses ordinary stdout/stderr, not this framed side channel.
      (serve! (Channels/newInputStream channel)
              (Channels/newOutputStream channel)
              identity))))
