(ns ygg-test
  "Black-box regression tests of Ygg's standalone ZFS lifecycle.

   The test suite executes the actual root-level `ygg` executable against a
   disposable source repository and a fake ZFS command, never the host's ZFS.
   The fake is bundled here so the repository needs no external test fixture.
   Live ZFS smoke testing remains a separate requirement. The v1074
   command-lock tests exercise the real cross-process FileChannel lock."
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing run-tests]])
  (:import [java.nio.channels FileChannel]
           [java.nio.file Files LinkOption OpenOption StandardOpenOption]
           [java.nio.file.attribute BasicFileAttributes FileAttribute]))

(def ^:private fake-zfs-source
  "#!/usr/bin/env python3
# Simulated ZFS command for Ygg lifecycle tests. Never touches real ZFS.
import json
import os
import pathlib
import sys

path = pathlib.Path(os.environ['YGG_MOCK_STATE'])
state = json.loads(path.read_text())
objects = state['objects']
args = sys.argv[1:]
state.setdefault('calls', []).append(args)

def save():
    temp = path.with_suffix('.writing')
    temp.write_text(json.dumps(state, sort_keys=True))
    temp.replace(path)

def ok(value=''):
    save()
    if value:
        print(value)
    sys.exit(0)

def no(message='dataset does not exist'):
    save()
    print(message, file=sys.stderr)
    sys.exit(1)

def descendants(name):
    return [key for key in objects if key == name or key.startswith(name + '/')]

if not args:
    no('missing command')
command = args[0]
if command == 'list':
    kind = args[args.index('-t') + 1]
    fields = args[args.index('-o') + 1]
    recursive = '-r' in args
    name = args[-1] if args[-1] not in ('name', 'name,mountpoint') else None
    matches = [key for key in objects if ('@' in key) == (kind == 'snapshot')]
    if name:
        if recursive:
            if kind == 'snapshot':
                matches = [key for key in matches if key.startswith(name + '@') or key.startswith(name + '/')]
            else:
                matches = [key for key in matches if key == name or key.startswith(name + '/')]
        else:
            matches = [key for key in matches if key == name]
        if not matches and not recursive:
            no()
    if fields == 'name,mountpoint':
        ok('\\n'.join(key + '\\t' + objects[key].get('mountpoint', '-') for key in matches))
    ok('\\n'.join(matches))
if command == 'get':
    name = args[-1]
    prop = args[-2]
    if name not in objects:
        no()
    if (state.get('fail_once') == 'clone-guid'
            and prop == 'guid' and '/ygg-' in name and '@' not in name):
        state['fail_once'] = None
        no('injected clone GUID read failure')
    ok(str(objects[name].get(prop, '-')))
if command == 'snapshot':
    name = args[1]
    if name in objects:
        no('already exists')
    objects[name] = {'guid': 'snapshot-' + str(state.get('sequence', 1))}
    state['sequence'] = state.get('sequence', 1) + 1
    ok()
if command == 'clone':
    snapshot, name = args[-2:]
    mountpoint = next((x.partition('=')[2] for x in args if x.startswith('mountpoint=')), None)
    if not snapshot in objects or not mountpoint or name in objects:
        no('invalid clone')
    objects[name] = {'guid': 'clone-' + str(state.get('sequence', 1)),
                     'origin': snapshot, 'mountpoint': mountpoint, 'mounted': 'no'}
    state['sequence'] = state.get('sequence', 1) + 1
    ok()
if command == 'mount':
    name = args[1]
    if state.get('fail_once') == 'mount':
        state['fail_once'] = None
        no('injected mount failure')
    if name not in objects:
        no()
    pathlib.Path(objects[name]['mountpoint']).mkdir(parents=True, exist_ok=True)
    objects[name]['mounted'] = 'yes'
    ok()
if command == 'unmount':
    name = args[1]
    if name not in objects:
        no()
    objects[name]['mounted'] = 'no'
    ok()
if command == 'destroy':
    if any(option in args for option in ('-r', '-R', '-f')):
        no('destructive recursive option forbidden')
    name = args[-1]
    if name not in objects:
        no()
    del objects[name]
    ok()
no('unsupported fake ZFS operation: ' + repr(args))
")

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

(defn- make-fixture []
  (let [base (.toFile (Files/createTempDirectory "ygg-lifecycle-"
                                                (make-array FileAttribute 0)))
        root (path base "source")
        fake-bin (path base "bin")
        state (path base "mock-state.json")]
    (.mkdir root)
    (.mkdir (path root ".git"))
    (.mkdir fake-bin)
    (write-executable! (path fake-bin "zfs") fake-zfs-source)
    (write-executable! (path fake-bin "sudo")
                       (str "#!/bin/sh\n"
                            "[ \"$1\" = -n ] || exit 91\n"
                            "[ \"$2\" = -- ] || exit 92\n"
                            "[ \"$3\" = zfs ] || exit 93\n"
                            "shift 2\nexec \"$@\"\n"))
    (spit state (json/generate-string
                 {:objects {"tank/fixture" {:guid "source-original"
                                              :mountpoint (.getCanonicalPath root)
                                              :mounted "yes"}}
                  :sequence 1 :calls []}))
    {:base base :root root :bin fake-bin :state state
     :ygg (locate-ygg!)}))

(defn- mock-state [fx] (json/parse-string (slurp (:state fx)) true))
(defn- update-mock! [fx change]
  (spit (:state fx) (json/generate-string (change (mock-state fx)))))
(defn- run-ygg [fx & args]
  (let [environment (assoc (into {} (System/getenv))
                           "YGG_MOCK_STATE" (.getAbsolutePath (:state fx))
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

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'ygg-test)]
    (when (pos? (+ fail error)) (System/exit 1))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
