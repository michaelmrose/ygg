(ns ygg-test
  "Black-box regression tests of Ygg's standalone ZFS lifecycle.

   The test suite executes the actual root-level `ygg` executable against a
   disposable source repository and a lightweight, filesystem-backed fake ZFS command, never the host's ZFS.
   The Bash fake is bundled here and launches no Python interpreter.
   Live ZFS smoke testing remains a separate requirement. The v1074
   command-lock tests exercise the real cross-process FileChannel lock."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing run-tests]])
  (:import [java.nio.channels FileChannel]
           [java.nio.file Files LinkOption OpenOption StandardOpenOption]
           [java.nio.file.attribute BasicFileAttributes FileAttribute]))

(def ^:private fake-zfs-source
  "#!/usr/bin/env bash\n# Filesystem-backed mock ZFS. One lightweight shell per command, no Python or ZFS.\nset -euo pipefail\nroot=${YGG_MOCK_DIR:?}\nobjects=\"$root/objects\"\nprintf '%s' \"$1\" >> \"$root/calls\"\nfor arg in \"${@:2}\"; do printf '\\t%s' \"$arg\" >> \"$root/calls\"; done\nprintf '\\n' >> \"$root/calls\"\nfail() { printf '%s\\n' \"${1:-dataset does not exist}\" >&2; exit 1; }\nencode() { file=\"$objects/${1//\\//!}\"; file=\"${file//@/\\~}\"; }\ndecode() { object=\"${1##*/}\"; object=\"${object//!//}\"; object=\"${object//\\~/@}\"; }\nread_object() {\n  encode \"$1\"\n  [[ -f \"$file\" ]] || fail\n  IFS='|' read -r guid origin mountpoint mounted < \"$file\" || true\n}\nwrite_object() { printf '%s|%s|%s|%s\\n' \"$guid\" \"$origin\" \"$mountpoint\" \"$mounted\" > \"$file\"; }\nnext_guid() {\n  local current\n  read -r current < \"$root/sequence\"\n  guid=\"${1}-${current}\"\n  printf '%s\\n' \"$((current + 1))\" > \"$root/sequence\"\n}\ncase \"$1\" in\n  list)\n    kind=filesystem; fields=name; recursive=0; target=''\n    shift\n    while (($#)); do\n      case \"$1\" in\n        -H) shift ;;\n        -r) recursive=1; shift ;;\n        -t) kind=$2; shift 2 ;;\n        -o) fields=$2; shift 2 ;;\n        *) target=$1; shift ;;\n      esac\n    done\n    found=0\n    shopt -s nullglob\n    for entry in \"$objects\"/*; do\n      decode \"$entry\"; dataset=$object\n      case \"$kind\" in\n        snapshot) [[ \"$dataset\" == *@* ]] || continue ;;\n        *) [[ \"$dataset\" != *@* ]] || continue ;;\n      esac\n      if [[ -n \"$target\" ]]; then\n        if ((recursive)); then\n          if [[ \"$kind\" == snapshot ]]; then\n            [[ \"$dataset\" == \"$target\"@* || \"$dataset\" == \"$target\"/* ]] || continue\n          else\n            [[ \"$dataset\" == \"$target\" || \"$dataset\" == \"$target\"/* ]] || continue\n          fi\n        else\n          [[ \"$dataset\" == \"$target\" ]] || continue\n        fi\n      fi\n      found=1\n      if [[ \"$fields\" == name,mountpoint ]]; then\n        IFS='|' read -r guid origin mountpoint mounted < \"$entry\" || true\n        printf '%s\\t%s\\n' \"$dataset\" \"$mountpoint\"\n      else\n        printf '%s\\n' \"$dataset\"\n      fi\n    done\n    if ((found == 0)) && [[ -n \"$target\" ]] && ((recursive == 0)); then fail; fi\n    ;;\n  get)\n    prop=${@: -2:1}; name=${@: -1}\n    read_object \"$name\"\n    if [[ \"$prop\" == guid && \"$name\" == */ygg-* && \"$name\" != *@* && -f \"$root/fail_once\" ]]; then\n      read -r failure < \"$root/fail_once\" || true\n      if [[ \"$failure\" == clone-guid ]]; then\n        rm -f \"$root/fail_once\"\n        fail 'injected clone GUID read failure'\n      fi\n    fi\n    case \"$prop\" in\n      guid) printf '%s\\n' \"$guid\" ;;\n      origin) printf '%s\\n' \"$origin\" ;;\n      mountpoint) printf '%s\\n' \"$mountpoint\" ;;\n      mounted) printf '%s\\n' \"$mounted\" ;;\n      *) printf -- '-\\n' ;;\n    esac\n    ;;\n  snapshot)\n    name=$2\n    encode \"$name\"\n    [[ ! -e \"$file\" ]] || fail 'already exists'\n    next_guid snapshot\n    origin=-; mountpoint=-; mounted=-\n    write_object\n    ;;\n  clone)\n    snapshot=${@: -2:1}; name=${@: -1}\n    read_object \"$snapshot\"\n    proposed=''\n    for arg in \"$@\"; do\n      if [[ \"$arg\" == mountpoint=* ]]; then proposed=${arg#mountpoint=}; fi\n    done\n    [[ -n \"$proposed\" ]] || fail 'invalid clone mountpoint'\n    encode \"$name\"\n    [[ ! -e \"$file\" ]] || fail 'already exists'\n    next_guid clone\n    origin=$snapshot; mountpoint=$proposed; mounted=no\n    write_object\n    ;;\n  mount|unmount)\n    action=$1; name=$2\n    if [[ \"$action\" == mount && -f \"$root/fail_once\" ]]; then\n      read -r failure < \"$root/fail_once\" || true\n      if [[ \"$failure\" == mount ]]; then\n        rm -f \"$root/fail_once\"\n        fail 'injected mount failure'\n      fi\n    fi\n    read_object \"$name\"\n    if [[ \"$action\" == mount ]]; then\n      mkdir -p \"$mountpoint\"\n      mounted=yes\n    else\n      mounted=no\n    fi\n    write_object\n    ;;\n  destroy)\n    for arg in \"$@\"; do\n      case \"$arg\" in -r|-R|-f) fail 'destructive recursive option forbidden' ;; esac\n    done\n    name=${@: -1}; read_object \"$name\"\n    rm -- \"$file\"\n    ;;\n  *) fail \"unsupported fake ZFS operation: $1\" ;;\nesac\n")

(def ^:private no-follow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

(defn- path [& xs] (apply io/file xs))
(defn- exists? [p] (Files/exists (.toPath (io/file p)) no-follow))
(defn- write-executable! [p source]
  (spit p source)
  (when-not (.setExecutable (io/file p) true)
    (throw (ex-info "Could not mark test shim executable" {:path (str p)})))
  p)

(defn- remove-tree! [p]
  (when (exists? p)
    (with-open [stream (Files/walk (.toPath (io/file p)) (make-array java.nio.file.FileVisitOption 0))]
      (doseq [entry (reverse (sort-by #(.getNameCount %) (iterator-seq (.iterator stream))))]
        (Files/deleteIfExists entry)))))

(defn- bb-command []
  ;; Works from `bb test/ygg_test.clj`, `bb -cp test -m ygg-test`, and bb tasks.
  (or (System/getenv "YGG_TEST_BB") "bb"))

(defn- locate-ygg! []
  (let [p (path (System/getProperty "user.dir") "ygg")]
    (when-not (.isFile p)
      (throw (ex-info "Run Ygg tests from the Ygg/Shard repository root"
                      {:expected (.getAbsolutePath p)})))
    (.getCanonicalPath p)))

(defn- dataset-file [fx dataset]
  (path (:state fx) "objects"
        (-> (str dataset)
            (str/replace "/" "!")
            (str/replace "@" "~"))))

(defn- read-object [entry]
  (let [[guid origin mountpoint mounted]
        (str/split (str/trim (slurp entry)) #"\|" -1)]
    {:guid guid :origin origin :mountpoint mountpoint :mounted mounted}))

(defn- mock-state [fx]
  (let [objects-dir (path (:state fx) "objects")
        obj (into {}
                  (for [entry (or (seq (.listFiles objects-dir)) [])
                        :when (.isFile entry)]
                    [(-> (.getName entry)
                         (str/replace "!" "/")
                         (str/replace "~" "@"))
                     (read-object entry)]))
        calls-file (path (:state fx) "calls")
        calls (if (exists? calls-file)
                (mapv #(str/split % #"\t" -1)
                      (str/split-lines (slurp calls-file)))
                [])
        failure-file (path (:state fx) "fail_once")]
    {:objects obj
     :calls calls
     :sequence (Long/parseLong (str/trim (slurp (path (:state fx) "sequence"))))
     :fail_once (when (exists? failure-file)
                  (str/trim (slurp failure-file)))}))

(defn- update-mock! [fx change]
  (let [state (change (mock-state fx))
        objects-dir (path (:state fx) "objects")]
    (doseq [entry (or (seq (.listFiles objects-dir)) [])]
      (Files/delete (.toPath entry)))
    (doseq [[dataset props] (:objects state)]
      (let [{:keys [guid origin mountpoint mounted]} props]
        (spit (dataset-file fx dataset)
              (str (or guid "-") "|" (or origin "-") "|"
                   (or mountpoint "-") "|" (or mounted "-") "\n"))))
    (spit (path (:state fx) "sequence") (str (:sequence state) "\n"))
    (spit (path (:state fx) "calls")
          (str (str/join "\n" (map #(str/join "\t" %) (:calls state)))
               (when (seq (:calls state)) "\n")))
    (let [f (path (:state fx) "fail_once")]
      (if-let [failure (:fail_once state)]
        (spit f (str failure "\n"))
        (Files/deleteIfExists (.toPath f))))))

(defn- make-fixture []
  (let [base (.toFile (Files/createTempDirectory "ygg-lifecycle-"
                                                (make-array FileAttribute 0)))
        root (path base "source")
        fake-bin (path base "bin")
        state (path base "mock-zfs")]
    (.mkdir root)
    (.mkdir (path root ".git"))
    (.mkdir fake-bin)
    (.mkdir state)
    (.mkdir (path state "objects"))
    (write-executable! (path fake-bin "zfs") fake-zfs-source)
    (write-executable! (path fake-bin "sudo")
                       (str "#!/bin/sh\n"
                            "[ \"$1\" = -n ] || exit 91\n"
                            "[ \"$2\" = -- ] || exit 92\n"
                            "[ \"$3\" = zfs ] || exit 93\n"
                            "shift 2\nexec \"$@\"\n"))
    (spit (path state "sequence") "1\n")
    (spit (path state "calls") "")
    (spit (path state "objects" "tank!fixture")
          (str "source-original|-|" (.getCanonicalPath root) "|yes\n"))
    {:base base :root root :bin fake-bin :state state
     :ygg (locate-ygg!)}))

(defn- run-ygg [fx & args]
  (let [environment (assoc (into {} (System/getenv))
                           "YGG_MOCK_DIR" (.getAbsolutePath (:state fx))
                           "PATH" (str (.getAbsolutePath (:bin fx))
                                       java.io.File/pathSeparator
                                       (System/getenv "PATH")))
        ;; GNU timeout is available on the Linux host Ygg requires. The
        ;; timeout guards against orphaned child processes in a test fixture.
        ;; An expired deadline is an ERROR, not an expected command refusal.
        cmd (concat ["timeout" "--kill-after=5s" "120s"
                     (bb-command) (:ygg fx)]
                    args [(.getAbsolutePath (:root fx))])]
    (try
      (let [result (apply sh/sh (concat cmd [:env environment]))]
        (when (#{124 137} (:exit result))
          (throw (ex-info "Timed out or forcibly killed Ygg test process"
                          {:command (vec cmd) :exit (:exit result)})))
        result)
      (catch Exception e
        (throw (ex-info (str "Cannot complete Ygg command: " (pr-str cmd))
                        {:command (vec cmd)} e))))))

(defn- successful! [result]
  (is (zero? (:exit result))
      (str "Ygg should succeed, stdout=" (:out result) ", stderr=" (:err result)))
  result)
(defn- refused! [result]
  (is (not (zero? (:exit result)))
      (str "Ygg should refuse operation, stdout=" (:out result)))
  result)
(defn- status! [fx]
  (let [r (successful! (run-ygg fx "status" "--edn"))]
    (edn/read-string (str/trim (:out r)))))
(defn- clones [fx]
  (->> (mock-state fx) :objects
       (filter (fn [[key _]] (str/includes? (str key) "/ygg-")))
       (into {})))
(defn- calls [fx]
  (:calls (mock-state fx)))
(defn- command-lock [fx]
  (path (:root fx) ".ygg" ".command.lock"))
(defn- legacy-lock [fx]
  (path (:root fx) ".ygg.lock"))
(defn- lock-file-key [fx]
  (let [lock (command-lock fx)]
    (is (.isFile lock) "Command lock must be a regular file inside .ygg")
    (is (not (exists? (legacy-lock fx)))
        "No ordinary invocation may create a root-level .ygg.lock")
    (.fileKey (Files/readAttributes (.toPath lock)
                                   BasicFileAttributes no-follow))))
(defn- mutation-calls [fx]
  (filter (fn [args] (contains? #{"snapshot" "clone" "mount"
                                  "unmount" "destroy"} (first args)))
          (calls fx)))

(defn- with-fixture [test-fn]
  (let [fx (make-fixture)]
    (try (test-fn fx)
         (finally (remove-tree! (:base fx))))))

(deftest idle-status-is-machine-readable-and-does-not-require-existing-workspace
  (with-fixture
    (fn [fx]
      (testing "No run means :idle and an empty clone list"
        (is (= {:version 1 :status :idle
                :source-path (.getCanonicalPath (:root fx))
                :source-dataset "tank/fixture" :clones []}
               (status! fx)))
        (is (empty? (clones fx)))
        (let [inode (lock-file-key fx)]
          (is (= inode (lock-file-key fx)))
          (successful! (run-ygg fx "status" "--edn"))
          (is (= inode (lock-file-key fx))))))))

(deftest clones-are-separate-workspaces-and-clean-preserves-run-history
  (with-fixture
    (fn [fx]
      (let [runs (path (:root fx) ".ygg" "runs")
            old (path runs "prior.edn")]
        (.mkdirs runs)
        (spit old "{:prior :result}\n")
        (successful! (run-ygg fx "-n" "2" "create"))
        (let [lock-inode (lock-file-key fx)
              report (status! fx)]
          (is (= lock-inode (lock-file-key fx)))
          (is (= :ready (:status report)))
          (is (= 2 (count (:clones report))))
          (is (= #{(.getCanonicalPath (path (:root fx) ".ygg/workers/1"))
                   (.getCanonicalPath (path (:root fx) ".ygg/workers/2"))}
                 (set (map :mountpoint (:clones report)))))
          (is (every? :mounted? (:clones report)))
          (is (every? #(not (str/blank? (:guid %))) (:clones report)))
          (is (= 1 (count (filter #(= "snapshot" (first %)) (calls fx))))))
        (is (= "{:prior :result}\n" (slurp old)))
        (let [lock-inode (lock-file-key fx)]
          (successful! (run-ygg fx "clean"))
          (is (= lock-inode (lock-file-key fx))))
        (is (= :idle (:status (status! fx))))
        (is (empty? (clones fx)))
        (is (= "{:prior :result}\n" (slurp old)))
        (is (= 1 (count (filter #(str/includes? (str (key %)) "@")
                                (:objects (mock-state fx))))))
        (is (not (exists? (path (:root fx) ".ygg" ".ygg-state.edn"))))
        (is (not-any? #(and (= "destroy" (first %))
                             (some #{"-r" "-R" "-f"} %))
                      (calls fx)))))))

(deftest a-second-create-is-refused-without-making-another-snapshot
  (with-fixture
    (fn [fx]
      (successful! (run-ygg fx "create"))
      (let [before (count (filter #(= "snapshot" (first %)) (calls fx)))]
        (refused! (run-ygg fx "-n" "2" "create"))
        (is (= before (count (filter #(= "snapshot" (first %)) (calls fx)))))
        (is (= 1 (count (clones fx))))))))

(deftest verified-status-and-clean-reject-modified-zfs-identities
  (doseq [[label modify]
          [["clone guid" (fn [s id] (assoc-in s [:objects id :guid] "impostor"))]
           ["clone origin" (fn [s id] (assoc-in s [:objects id :origin] "tank/other@fake"))]
           ["clone mountpoint" (fn [s id] (assoc-in s [:objects id :mountpoint] "/tmp/not-a-clone"))]
           ["clone unmounted" (fn [s id] (assoc-in s [:objects id :mounted] "no"))]
           ["snapshot guid" (fn [s _] (let [snap (->> (:objects s) keys (filter #(str/includes? (str %) "@")) first)]
                                                  (assoc-in s [:objects snap :guid] "fake-snapshot")))]]]
    (with-fixture
      (fn [fx]
        (testing label
          (successful! (run-ygg fx "create"))
          (let [id (first (keys (clones fx)))]
            (update-mock! fx #(modify % id)))
          (refused! (run-ygg fx "status" "--edn"))
          (when (not= label "clone unmounted")
            (refused! (run-ygg fx "clean"))
            (is (seq (clones fx)))))))))

(deftest active-shard-worker-lock-prevents-cleanup
  (with-fixture
    (fn [fx]
      (successful! (run-ygg fx "create"))
      (let [lock-path (.toPath (path (:root fx) ".ygg" ".workers.lock"))]
        (with-open [channel (FileChannel/open lock-path
                                             (into-array OpenOption
                                                         [StandardOpenOption/WRITE]))]
          ;; Closing the channel releases the OS lock. Babashka intentionally
          ;; disallows direct calls on sun.nio.ch.FileLockImpl.
          (.lock channel)
          (refused! (run-ygg fx "clean"))
          (is (= 1 (count (clones fx)))))
        (successful! (run-ygg fx "clean"))
        (is (empty? (clones fx)))))))

(deftest unrecorded-clone-after-interrupted-creation-is-not-destroyed
  (with-fixture
    (fn [fx]
      (update-mock! fx #(assoc % :fail_once "clone-guid"))
      (refused! (run-ygg fx "create"))
      (is (= 1 (count (clones fx))))
      (let [state (edn/read-string (slurp (path (:root fx) ".ygg" ".ygg-state.edn")))]
        (is (nil? (get-in state [:clones 0 :guid])))
        (is (string? (:snapshot-guid state))))
      (refused! (run-ygg fx "status" "--edn"))
      (refused! (run-ygg fx "clean"))
      (is (= 1 (count (clones fx))))
      (is (not-any? #(= "destroy" (first %)) (calls fx))))))

(deftest existing-mountpoints-and-symlinked-state-are-rejected
  (with-fixture
    (fn [fx]
      (let [workers (path (:root fx) ".ygg" "workers")]
        (.mkdirs (path workers "unexpected"))
        (refused! (run-ygg fx "create"))
        (is (not-any? #(= "snapshot" (first %)) (calls fx))))) )
  (with-fixture
    (fn [fx]
      (.mkdirs (path (:root fx) ".ygg"))
      (Files/createSymbolicLink (.toPath (path (:root fx) ".ygg" ".ygg-state.edn"))
                                (.toPath (path (:root fx) "not-state"))
                                (make-array java.nio.file.attribute.FileAttribute 0))
      (refused! (run-ygg fx "create"))
      (is (empty? (clones fx))))))

(deftest a-nested-filesystem-is-refused-rather-than-omitted
  (with-fixture
    (fn [fx]
      (update-mock! fx
                    #(assoc-in % [:objects "tank/fixture/child"]
                               {:guid "nested" :mountpoint
                                (.getAbsolutePath (path (:root fx) "other-mounted"))
                                :mounted "yes"}))
      (refused! (run-ygg fx "create"))
      (is (not-any? #(= "snapshot" (first %)) (calls fx))))))

(deftest invalid-cli-arguments-are-refused
  (with-fixture
    (fn [fx]
      (doseq [argv [["--edn" "create"] ["-n" "0" "create"]
                    ["-n" "999" "create"] ["destroy"]]]
        (refused! (apply run-ygg fx argv)))
      (is (empty? (clones fx))))))

(deftest command-lock-is-exclusive-across-processes-and-retained
  (with-fixture
    (fn [fx]
      (successful! (run-ygg fx "status" "--edn"))
      (let [lock (command-lock fx)
            prior-key (lock-file-key fx)]
        (with-open [channel (FileChannel/open (.toPath lock)
                                             (into-array OpenOption
                                                         [StandardOpenOption/WRITE]))]
          ;; The outer test JVM holds the lock; each CLI invocation runs in
          ;; a distinct Babashka process and must fail without ZFS mutation.
          ;; Channel close releases the lock (SCI does not expose .release).
          (.lock channel)
          (doseq [operation [["status" "--edn"] ["create"] ["clean"]]]
            (let [result (apply run-ygg fx operation)]
              (refused! result)
              (is (str/includes? (str (:err result))
                                 "Another Ygg command is running"))))
          (is (empty? (mutation-calls fx)))
          (is (= prior-key (lock-file-key fx))))
        (successful! (run-ygg fx "status" "--edn"))
        (is (= prior-key (lock-file-key fx)))
        (is (empty? (mutation-calls fx)))))))

(deftest legacy-root-lock-requires-explicit-migration
  (with-fixture
    (fn [fx]
      (spit (legacy-lock fx) "legacy inode must not be unlinked by Ygg")
      (with-open [channel (FileChannel/open (.toPath (legacy-lock fx))
                                           (into-array OpenOption
                                                       [StandardOpenOption/WRITE]))]
        ;; Simulate an older process still holding the old inode. Merely
        ;; acquiring the *new* lock must never bypass that legacy lease.
        (.lock channel)
        (let [result (run-ygg fx "status" "--edn")]
          (refused! result)
          (is (str/includes? (str (:err result)) "Legacy .ygg.lock"))
          (is (= "legacy inode must not be unlinked by Ygg"
                 (slurp (legacy-lock fx))))
          (is (empty? (mutation-calls fx)))
          (is (not (exists? (command-lock fx))))))
      ;; Still refuse after the old process exits: presence, not lock
      ;; availability, is the migration barrier.
      (refused! (run-ygg fx "status" "--edn"))
      ;; Only the operator, after retiring older Ygg processes, may unlink
      ;; the legacy inode. The test fixture guarantees no legacy process.
      (Files/delete (.toPath (legacy-lock fx)))
      (successful! (run-ygg fx "status" "--edn"))
      (lock-file-key fx)
      (is (empty? (mutation-calls fx))))))

(deftest root-workspace-symlink-is-refused-before-lock-creation
  (with-fixture
    (fn [fx]
      (let [target (path (:base fx) "external-workspace")
            root-workspace (path (:root fx) ".ygg")]
        (.mkdirs target)
        (Files/createSymbolicLink (.toPath root-workspace) (.toPath target)
                                  (make-array FileAttribute 0))
        (refused! (run-ygg fx "status" "--edn"))
        (refused! (run-ygg fx "create"))
        (is (not (exists? (path target ".command.lock"))))
        (is (empty? (mutation-calls fx)))
        (is (not (exists? (legacy-lock fx))))))))

(deftest malicious-command-lock-target-is-refused
  (doseq [variant [:symlink :directory]]
    (with-fixture
      (fn [fx]
        (testing (str "Command lock is " (name variant))
          (let [workspace (path (:root fx) ".ygg")
                target (path (:base fx) "external-lock")]
            (.mkdirs workspace)
            (spit target "unmodified")
            (case variant
              :symlink (Files/createSymbolicLink (.toPath (command-lock fx))
                                                 (.toPath target)
                                                 (make-array FileAttribute 0))
              :directory (.mkdir (command-lock fx)))
            (refused! (run-ygg fx "status" "--edn"))
            (refused! (run-ygg fx "create"))
            (is (= "unmodified" (slurp target)))
            (is (empty? (mutation-calls fx)))
            (is (not (exists? (legacy-lock fx))))))))))


(deftest timeline-shows-stable-clone-slots-and-process-handoffs
  (with-fixture
    (fn [fx]
      (let [run (path (:root fx) ".ygg" "runs" "run-slot-fixture")
            worker-a "11111111-1111-1111-1111-111111111111"
            worker-b "22222222-2222-2222-2222-222222222222"
            worker-c "33333333-3333-3333-3333-333333333333"
            entry (fn [id runtime worker started finished duration]
                    {:id id :status :passed :runtime runtime :worker-id worker
                     :started-at started :completed-at finished :duration-ms duration})
            jobs [(entry "first" :gesso worker-a
                         "2026-01-01T00:00:00Z" "2026-01-01T00:00:10Z" 10000)
                  (entry "second" :gesso-jvm worker-b
                         "2026-01-01T00:00:15Z" "2026-01-01T00:00:30Z" 15000)
                  (entry "third" :gesso-jvm worker-c
                         "2026-01-01T00:00:03Z" "2026-01-01T00:00:23Z" 20000)]
            report {:format :shard/report :version 1
                    :inventory-sha256 "timeline-test"
                    :overall :pass :total 3 :jobs jobs}]
        (.mkdirs (path run "logs"))
        (spit (path run "jobs.edn") "{}\n")
        (write-executable! (path (:bin fx) "shard")
                           (str "#!/bin/sh\nprintf '%s\\n' '"
                                (pr-str report) "'\n"))
        (spit (path run "worker-1.log")
              (str "SHARD WORKER " worker-a " runtime :gesso jobs 1\n"))
        (spit (path run "worker-1-phase-1.log")
              (str "SHARD WORKER " worker-b " runtime :gesso-jvm jobs 1\n"))
        (spit (path run "worker-2.log")
              (str "SHARD WORKER " worker-c " runtime :gesso-jvm jobs 1\n"))
        (let [full (successful! (run-ygg fx "timeline" "--run" "run-slot-fixture"))
              text (:out full)
              summary (successful! (run-ygg fx "timeline" "--summary"
                                            "--run" "run-slot-fixture"))]
          (is (str/includes? text "Clone slots observed: 2"))
          (is (re-find #"S1\s+2\s+2\s+0ms\s+30\.0s\s+25\.0s\s+5\.0s\s+0ms" text))
          (is (str/includes? text "HANDOFF / between-process gap: 5.0s"))
          (is (str/includes? text (str "Phase 1 | runtime gesso-jvm | worker-process UUID " worker-b)))
          (is (not (str/includes? (:out summary) "PER-SLOT JOB DETAILS")))
          (is (str/includes? (:out summary) "Clone slots observed: 2")))
        ;; A missing worker log must NOT cause two process UUIDs to be merged
        ;; by observed timing or falsely assigned to an apparent physical slot.
        (Files/delete (.toPath (path run "worker-1-phase-1.log")))
        (let [unknown (successful! (run-ygg fx "timeline" "--summary"
                                            "--run" "run-slot-fixture"))]
          (is (str/includes? (:out unknown) "UNMAPPED: 1"))
          (is (str/includes? (:out unknown) "Clone slots observed: 2"))
          (is (str/includes? (:out unknown) "unmapped 22222222")))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'ygg-test)]
    (when (pos? (+ fail error)) (System/exit 1))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
