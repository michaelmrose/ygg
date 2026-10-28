(ns shard.runtime.admission
  "Fail-closed preparation and live probing of a persistent JVM worker's
  Bubblewrap environment. This module does NOT launch or attest the JVM
  session itself: production session PID mapping, group supervision and
  per-session admission are separate required integration gates.

  All scheduling, claims and run evidence remain outside the sandbox. Only
  the validated worker clone is writable; /tmp and /dev/shm are private.
  This module never falls back to an unsandboxed command."
  (:require [clojure.string :as str])
  (:import [java.io File]
           [java.net InetAddress ServerSocket Socket InetSocketAddress]
           [java.nio.file Files Path LinkOption]
           [java.nio.file.attribute FileAttribute PosixFilePermission]
           [java.util UUID]
           [java.util.concurrent TimeUnit]))

(def ^:private no-follow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private empty-attrs (make-array FileAttribute 0))
(def ^:private required-env ["HOME" "TMPDIR" "XDG_RUNTIME_DIR"])
(def ^:private dependency-caches [".m2" ".gitlibs" ".clojure"])
(def ^:private probe-timeout-ms 15000)

(defn- refuse! [reason message & [details]]
  (throw (ex-info message (assoc (or details {}) :shard.runtime.admission/reason reason))))

(defn- demand! [truth reason message & [details]]
  (when-not truth (refuse! reason message details)))

(defn- full-path! [value label]
  (demand! (or (string? value) (instance? File value) (instance? Path value))
           :configuration (str label " must be a path"))
  (let [p (.normalize (.toAbsolutePath (cond
                                        (instance? Path value) value
                                        (instance? File value) (.toPath ^File value)
                                        :else (Path/of ^String value (make-array String 0)))))]
    (demand! (Files/isDirectory p no-follow) :configuration
             (str label " must be an existing non-symlink directory") {:path (str p)})
    ;; Reject symbolic links in *every* ancestor, not just the final path.
    (loop [q p]
      (when q
        (demand! (not (Files/isSymbolicLink q)) :unsafe-path
                 (str label " contains a symlink") {:path (str q)})
        (recur (.getParent q))))
    p))

(defn- child-of? [^Path parent ^Path child]
  (and (not= parent child) (.startsWith child parent)))

(defn- arg-vector! [value label]
  (demand! (and (vector? value) (seq value)
                (every? #(and (string? %) (not (str/blank? %))
                              (not (str/includes? % "\u0000"))) value))
           :configuration (str label " must be a nonempty string argv vector"))
  value)

(defn- binary! [value label]
  (demand! (and (string? value) (re-matches #"[A-Za-z0-9/_.+-]+" value)
                (not (str/blank? value)))
           :configuration (str label " must be an executable name or absolute path"))
  (when (str/includes? value "/")
    (let [p (Path/of value (make-array String 0))]
      (demand! (.isAbsolute p) :configuration
               (str label " must be absolute when it contains a slash"))
      (demand! (and (Files/isRegularFile p no-follow)
                    (Files/isExecutable p)) :configuration
               (str label " must be an existing executable"))))
  value)

(defn- cache-binds! [^Path private-home ^Path host-home]
  (if (nil? host-home)
    []
    (vec
     (mapcat
      (fn [name]
        (let [src (.resolve host-home name)
              dst (.resolve private-home name)]
          (demand! (Files/isDirectory dst no-follow) :unsafe-cache
                   "Private dependency cache destination is missing or a symlink"
                   {:path (str dst)})
          (if (Files/exists src no-follow)
            (do
              (demand! (Files/isDirectory src no-follow) :unsafe-cache
                       "Dependency cache source must be a real directory"
                       {:path (str src)})
              ["--ro-bind" (str src) (str dst)])
            [])))
      dependency-caches))))

(defn plan!
  "Build a persistent, per-WORKER Bubblewrap invocation, without running it.

  Required: :workspace (the owned clone), :run-dir (external run basket),
  :control-dir (a private directory WITHIN the clone), :worker-env (at least
  HOME, TMPDIR, XDG_RUNTIME_DIR inside the clone), and :command argv.

  Optional: :host-home for read-only caches, :bwrap and :setsid executables.
  No control socket or input from the run basket is exposed as writable.
  The returned map is only a PLAN, never proof of an admitted session."
  [{:keys [workspace run-dir control-dir worker-env command host-home bwrap setsid]}]
  (arg-vector! command :command)
  (demand! (map? worker-env) :configuration ":worker-env must be a map")
  (let [workspace (full-path! workspace :workspace)
        run-dir (full-path! run-dir :run-dir)
        control (full-path! control-dir :control-dir)
        host-home (when host-home (full-path! host-home :host-home))
        bwrap (binary! (or bwrap "bwrap") :bwrap)
        setsid (binary! (or setsid "setsid") :setsid)
        _ (demand! (and (not (.startsWith workspace run-dir))
                        (not (.startsWith run-dir workspace)))
                   :run-overlap "Run basket and worker clone must not overlap")
        _ (demand! (child-of? workspace control) :unsafe-control
                   "Control socket directory must be inside the owned worker clone")
        _ (let [perms (Files/getPosixFilePermissions control no-follow)
                disallowed #{PosixFilePermission/GROUP_READ PosixFilePermission/GROUP_WRITE
                             PosixFilePermission/GROUP_EXECUTE PosixFilePermission/OTHERS_READ
                             PosixFilePermission/OTHERS_WRITE PosixFilePermission/OTHERS_EXECUTE}]
            (demand! (empty? (filter #(.contains perms %) disallowed)) :unsafe-control
                     "Control socket directory must have private (0700 or stricter) permissions"))
        env (into {} (for [[k v] worker-env]
                       (do (demand! (and (string? k) (string? v)
                                         (re-matches #"[A-Z][A-Z0-9_]*" k))
                                    :configuration "Worker environment must contain string keys and values")
                           [k v])))
        _ (doseq [k required-env]
            (demand! (contains? env k) :configuration
                     (str "Missing required worker environment: " k)))
        home (full-path! (get env "HOME") :HOME)
        _ (doseq [k required-env]
            (let [p (full-path! (get env k) k)]
              (demand! (child-of? workspace p) :unsafe-environment
                       (str k " must be inside the worker clone"))))
        _ (demand! (child-of? workspace home) :unsafe-environment
                   "HOME must be within the clone")
        cache-mounts (cache-binds! home host-home)
        clone-path (str workspace)
        argv (into
              [setsid "--wait" "--" bwrap
               "--unshare-user" "--unshare-ipc" "--unshare-pid" "--unshare-net"
               "--die-with-parent"
               "--ro-bind" "/" "/"
               "--tmpfs" "/tmp"
               "--bind" clone-path clone-path
               "--proc" "/proc" "--dev" "/dev"
               "--dir" "/dev/shm" "--tmpfs" "/dev/shm"]
              (concat cache-mounts ["--chdir" clone-path "--"] command))]
    {:argv argv :cwd clone-path :env env :workspace clone-path
     :control-dir (str control) :run-dir (str run-dir)
     :isolation #{:user :ipc :pid :network :mount :private-tmp :private-shm}
     :status :unverified-plan}))

(defn- namespace-id! [name]
  (str (Files/readSymbolicLink
        (Path/of (str "/proc/self/ns/" name) (make-array String 0)))))

;; This is passed as a literal argument to `bb -e`. It requires NO mounted
;; Shard source and does not parse control messages from test output.
(def ^:private probe-expression
  (str "(do "
       "(require '[clojure.string :as s]) "
       "(import '[java.nio.file Files Path] "
       "        '[java.net ServerSocket Socket InetAddress InetSocketAddress]) "
       "(doseq [n [\"user\" \"ipc\" \"pid\" \"net\" \"mnt\"]] "
       "  (let [actual (str (Files/readSymbolicLink (Path/of (str \"/proc/self/ns/\" n) (make-array String 0))))] "
       "    (when (= actual (System/getenv (str \"SHARD_PARENT_NS_\" (s/upper-case n)))) "
       "      (throw (ex-info \"No isolated namespace\" {:name n}))))) "
       "(let [addr (InetAddress/getByName \"127.0.0.1\")] "
       "  (with-open [server (ServerSocket. 0 1 addr) client (Socket.)] "
       "    (.setSoTimeout server 2000) "
       "    (.connect client (InetSocketAddress. addr (.getLocalPort server)) 2000) "
       "    (with-open [accepted (.accept server)] "
       "      (when-not (.isConnected accepted) (throw (ex-info \"Loopback failed\" {})))))) "
       "(doseq [d [\"/tmp\" \"/dev/shm\"]] "
       "  (let [f (Files/createTempFile (Path/of d (make-array String 0)) \"shard-probe-\" \".tmp\" "
       "             (make-array java.nio.file.attribute.FileAttribute 0))] "
       "    (try (Files/writeString f \"ready\" (make-array java.nio.file.OpenOption 0)) "
       "         (when-not (= \"ready\" (Files/readString f)) (throw (ex-info \"Scratch failed\" {}))) "
       "         (finally (Files/deleteIfExists f))))) "
       "(println (str \"SHARD-PERSISTENT-PROBE-OK:\" (System/getenv \"SHARD_PROBE_NONCE\"))))"))

(defn- processes-in-group! [pgid]
  ;; Never turn unreadable /proc entries into false evidence of termination.
  (let [proc (File. "/proc")
        entries (.listFiles proc)]
    (demand! (some? entries) :proc-unavailable "Cannot enumerate host /proc")
    (reduce
     (fn [alive ^File entry]
       (if-not (re-matches #"[0-9]+" (.getName entry))
         alive
         (let [stat (.toPath (File. entry "stat"))]
           (try
             (let [data (Files/readString stat)
                   i (.lastIndexOf data ")")]
               (demand! (not (neg? i)) :proc-unavailable "Malformed /proc status")
               (let [fields (str/split (subs data (+ i 2)) #"\s+")]
                 (demand! (>= (count fields) 3) :proc-unavailable "Malformed /proc fields")
                 (if (and (= pgid (Long/parseLong (nth fields 2)))
                          (not (contains? #{"Z" "X"} (nth fields 0))))
                   (conj alive (Long/parseLong (.getName entry))) alive)))
             (catch java.nio.file.NoSuchFileException _ alive)
             (catch java.io.IOException e
               (if (Files/exists stat no-follow)
                 (refuse! :proc-unavailable "Cannot read host process status"
                          {:cause (.getMessage e)})
                 alive))))))
     [] entries)))

(defn- signal-group! [signal pgid]
  (let [p (.start (ProcessBuilder. ^java.util.List
                                   ["kill" (str "-" signal) "--" (str "-" pgid)]))]
    (demand! (.waitFor p 3 TimeUnit/SECONDS)
             :termination-unverified "Signal helper did not terminate")
    (zero? (.exitValue p))))

(defn- terminate-group! [^Process process]
  (let [pid (.pid process)]
    (signal-group! "TERM" pid)
    (.waitFor process 1000 TimeUnit/MILLISECONDS)
    (when (seq (processes-in-group! pid))
      (signal-group! "KILL" pid))
    (.waitFor process 1500 TimeUnit/MILLISECONDS)
    (loop [attempt 0]
      (let [remaining (processes-in-group! pid)]
        (when (seq remaining)
          (demand! (< attempt 20) :termination-unverified
                   "Probe process group may still be alive; do not admit worker"
                   {:pgid pid :remaining remaining})
          (Thread/sleep 100)
          (recur (inc attempt)))))))

(defn probe!
  "Perform a real sandbox admission PROBE, using exactly the plan's mounts
  and namespace flags. Does NOT attest or launch the later JVM session.
  The planned clone/run dirs MUST already exist. Failure is always fatal."
  [options]
  (let [bb (binary! (or (:probe-bb options) "bb") :probe-bb)
        nonce (str (UUID/randomUUID))
        parent-ids (into {} (for [name ["user" "ipc" "pid" "net" "mnt"]]
                              [(str "SHARD_PARENT_NS_" (str/upper-case name))
                               (namespace-id! name)]))
        plan (plan! (assoc options :command [bb "-e" probe-expression]))
        log (Files/createTempFile "shard-persistent-admission-" ".log" empty-attrs)
        pb (ProcessBuilder. ^java.util.List (:argv plan))]
    (try
      (.directory pb (File. ^String (:cwd plan)))
      (.redirectErrorStream pb true)
      (.redirectOutput pb (.toFile log))
      (.putAll (.environment pb) (:env plan))
      (.putAll (.environment pb) parent-ids)
      (.put (.environment pb) "SHARD_PROBE_NONCE" nonce)
      (let [process (try (.start pb)
                         (catch Exception e
                           (refuse! :sandbox-unavailable "Cannot start mandatory Bubblewrap probe"
                                    {:cause (.getMessage e)})))]
        (try
          (let [finished? (.waitFor process probe-timeout-ms TimeUnit/MILLISECONDS)]
            (when-not finished? (terminate-group! process))
            (let [output (Files/readString log)]
              (demand! (and finished? (zero? (.exitValue process))
                            (str/includes? output (str "SHARD-PERSISTENT-PROBE-OK:" nonce)))
                       :sandbox-rejected "Mandatory persistent sandbox probe failed"
                       {:exit (when finished? (.exitValue process))
                        :diagnostic (subs output 0 (min 1800 (count output)))}))
            (demand! (empty? (processes-in-group! (.pid process)))
                     :termination-unverified "Sandbox probe left live descendants")
            {:status :probe-passed :isolation (:isolation plan)
             :workspace (:workspace plan)})
          (catch Throwable t
            (terminate-group! process)
            (throw t))))
      (finally (Files/deleteIfExists log)))))
