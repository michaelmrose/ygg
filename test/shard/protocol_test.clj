(ns shard.protocol-test
  "Contract tests for the generic persistent JVM control protocol.

  These tests exercise framing, strict EDN admission, message shapes, and
  transport boundaries. They do NOT assert session ordering, runtime trust,
  supervised timeouts, or successful Bubblewrap admission; those belong to
  later worker/session/sandbox tests."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [shard.protocol :as p])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStream]
           [java.nio.charset StandardCharsets]
           [java.util Arrays]))

(def ^:private session (apply str (repeat 32 "a")))
(def ^:private fingerprint (apply str (repeat 64 "b")))

(defn- message
  ([type] (message type {}))
  ([type overrides]
   (merge {:version p/protocol-version
           :type type
           :session-id session
           :worker-id "jvm.01"
           :runtime-fingerprint fingerprint
           :seq (if (#{:hello :ready} type) 0 1)}
          (case type
            :ready {:pid 31415}
            :run {:job-id "jvm--gesso.model-test"
                  :namespace "gesso.model-test"
                  :timeout-ms 10000}
            :result {:job-id "jvm--gesso.model-test"
                     :namespace "gesso.model-test"
                     :pid 31415
                     :status :passed
                     :tests 2 :passes 3 :failures 0 :errors 0
                     :assertions 3 :elapsed-ms 17}
            :error {:job-id nil :pid 31415 :reason :execution-error
                    :message "Could not load test namespace"}
            :stopped {:pid 31415}
            {})
          overrides)))

(def ^:private host-types [:hello :run :shutdown])
(def ^:private worker-types [:ready :result :error :stopped])

(defn- rejected-reason [f]
  (try
    (f)
    ::accepted
    (catch clojure.lang.ExceptionInfo e
      (:shard.protocol/reason (ex-data e)))))

(defn- invalid! [reason f]
  (is (= reason (rejected-reason f))))

(defn- utf8-bytes [text]
  (.getBytes ^String text StandardCharsets/UTF_8))

(defn- stream [^bytes data]
  (ByteArrayInputStream. data))

(defn- frame-with-payload [payload]
  (let [^bytes body (if (string? payload) (utf8-bytes payload) payload)
        ^bytes header (utf8-bytes (format "%08x\n" (alength body)))
        out (ByteArrayOutputStream.)]
    (.write out header)
    (.write out body)
    (.toByteArray out)))

(deftest versioned-types-and-roundtrips
  (testing "every admitted message shape survives encoding and decoding"
    (doseq [type (concat host-types worker-types)]
      (let [m (message type)
            direction (if ((set host-types) type)
                        :host-to-worker :worker-to-host)
            wire (p/encode-frame m direction)]
        (is (= m (p/validate-message! m direction)) (str type))
        (is (Arrays/equals ^bytes wire ^bytes (p/encode-frame m)) (str type))
        (is (= m (p/read-frame! (stream wire) direction)) (str type))
        (is (= (format "%08x\n" (- (alength ^bytes wire) 9))
               (String. wire 0 9 StandardCharsets/US_ASCII))
            (str type))))))

(deftest header-counts-utf8-bytes-not-characters
  (let [m (message :error {:message "Greek λ / 測試 / café"})
        wire (p/encode-frame m)
        header (String. ^bytes wire 0 9 StandardCharsets/US_ASCII)]
    (is (> (alength ^bytes wire) (+ 9 (count (pr-str m)))))
    (is (= (format "%08x\n" (- (alength ^bytes wire) 9)) header))
    (is (= m (p/read-frame! (stream wire))))))

(deftest stream-framing-and-owned-streams
  (testing "multiple messages share one stream without leaking or combining frames"
    (let [messages [(message :hello) (message :run) (message :shutdown)]
          output (ByteArrayOutputStream.)]
      (doseq [m messages]
        (is (nil? (p/write-frame! output m :host-to-worker))))
      (let [input (stream (.toByteArray output))]
        (is (= messages (mapv (fn [_] (p/read-frame! input :host-to-worker)) messages)))
        (is (nil? (p/read-frame! input)))
        (is (nil? (p/read-frame! input))))
      ;; The caller owns the OutputStream, including its subsequent uses.
      (.write output (int 42))
      (is (= 42 (bit-and 0xff (last (seq (.toByteArray output)))))))))

(deftest generated-session-nonces
  (let [samples (repeatedly 25 p/new-session-id)]
    (is (every? #(boolean (re-matches #"[0-9a-f]{32}" %)) samples))
    (is (= (count samples) (count (distinct samples))))
    (is (every? #(not= session %) samples))))

(deftest message-shapes-are-closed
  (doseq [type (concat host-types worker-types)]
    (let [m (message type)]
      (invalid! :worker #(p/validate-message! (dissoc m :worker-id)))
      (invalid! :shape #(p/validate-message! (assoc m :unexpected "hidden")))
      (invalid! :shape #(p/validate-message! (assoc m :unused nil)))))
  (invalid! :shape #(p/validate-message! []))
  (invalid! :shape #(p/validate-message! nil)))

(deftest common-identity-and-version-validation
  (let [m (message :run)]
    (doseq [version [nil 0 (inc p/protocol-version) "1"]]
      (invalid! :version #(p/validate-message! (assoc m :version version))))
    (doseq [id [nil "" "a/b" " hello" "line\nbreak" (apply str (repeat 129 "x"))]]
      (invalid! :worker #(p/validate-message! (assoc m :worker-id id))))
    (doseq [nonce [nil "" (str/upper-case session) (subs session 1)
                   (str session "0") (apply str (repeat 32 "g"))]]
      (invalid! :session #(p/validate-message! (assoc m :session-id nonce))))
    (doseq [fp [nil "" (str/upper-case fingerprint) (subs fingerprint 1)
                (apply str (repeat 64 "g"))]]
      (invalid! :runtime #(p/validate-message! (assoc m :runtime-fingerprint fp))))
    (doseq [seq [nil -1 0 "1" 1.5 (inc (bigint Long/MAX_VALUE))]]
      (invalid! :sequence #(p/validate-message! (assoc m :seq seq))))
    (invalid! :type #(p/validate-message! (assoc m :type :arbitrary)))
    (invalid! :type #(p/validate-message! (assoc m :type "run")))))

(deftest sequencing-is-type-specific
  (doseq [type [:hello :ready]]
    (invalid! :sequence #(p/validate-message! (assoc (message type) :seq 1))))
  (doseq [type [:run :result :error :shutdown :stopped]]
    (invalid! :sequence #(p/validate-message! (assoc (message type) :seq 0)))
    (is (= Long/MAX_VALUE
           (:seq (p/validate-message! (assoc (message type) :seq Long/MAX_VALUE)))))))

(deftest host-and-worker-directions
  (doseq [type host-types]
    (invalid! :direction #(p/validate-message! (message type) :worker-to-host))
    (is (= (message type) (p/validate-message! (message type) :host-to-worker))))
  (doseq [type worker-types]
    (invalid! :direction #(p/validate-message! (message type) :host-to-worker))
    (is (= (message type) (p/validate-message! (message type) :worker-to-host))))
  (invalid! :direction #(p/validate-message! (message :hello) :sideways))
  (invalid! :direction #(p/read-frame! (stream (p/encode-frame (message :ready)))
                                              :host-to-worker))
  (invalid! :direction #(p/encode-frame (message :run) :worker-to-host)))

(deftest run-job-admission
  (let [m (message :run)]
    (doseq [id [nil "" "two words" "job/path" (apply str (repeat 129 "a"))]]
      (invalid! :job #(p/validate-message! (assoc m :job-id id))))
    (doseq [name [nil "" "no-dot" "gesso..bad" "9gesso.model" "gesso/tx-test"
                  (str "gesso." (apply str (repeat 250 "n")))]]
      (invalid! :namespace #(p/validate-message! (assoc m :namespace name))))
    (doseq [timeout [nil 0 -1 "100" 86400001]]
      (invalid! :timeout #(p/validate-message! (assoc m :timeout-ms timeout))))
    (is (= 86400000 (:timeout-ms
                    (p/validate-message! (assoc m :timeout-ms 86400000)))))))

(deftest worker-lifecycle-pids-and-errors
  (doseq [type worker-types]
    (doseq [pid [nil 0 -1 "31415" (inc (bigint Long/MAX_VALUE))]]
      (invalid! :pid #(p/validate-message! (assoc (message type) :pid pid)))))
  (let [m (message :error)]
    (doseq [reason [nil :bad :timeout :invisible]]
      (invalid! :reason #(p/validate-message! (assoc m :reason reason))))
    (doseq [msg [nil "" "  \n\t  " (apply str (repeat 2049 "a"))]]
      (invalid! :message #(p/validate-message! (assoc m :message msg))))
    (invalid! :job #(p/validate-message! (assoc m :job-id "invalid/job")))
    (is (= (apply str (repeat 2048 "a"))
           (:message (p/validate-message!
                      (assoc m :message (apply str (repeat 2048 "a")))))))
    (doseq [reason [:unknown-namespace :execution-error :protocol-error
                    :runtime-mismatch :worker-unavailable]]
      (is (= reason (:reason (p/validate-message! (assoc m :reason reason))))))))

(deftest result-counters-and-status
  (let [m (message :result)]
    (doseq [[field values] {:tests [nil -1 0 "1"]
                            :passes [nil -1 "1"]
                            :failures [nil -1 "1"]
                            :errors [nil -1 "1"]
                            :assertions [nil -1 0 "1"]
                            :elapsed-ms [nil -1 "1"]}]
      (doseq [value values]
        (invalid! :counts #(p/validate-message! (assoc m field value)))))
    (invalid! :counts #(p/validate-message! (assoc m :passes 2)))
    (invalid! :counts #(p/validate-message! (assoc m :failures 1)))
    (invalid! :status #(p/validate-message! (assoc m :status :unknown)))
    (invalid! :status #(p/validate-message! (assoc m :status :failed)))
    (invalid! :status #(p/validate-message! (assoc m :status :passed :failures 1
                                                        :passes 2)))
    (let [failed (assoc m :status :failed :passes 2 :failures 1)]
      (is (= failed (p/validate-message! failed))))
    (let [errored (assoc m :status :failed :passes 2 :errors 1)]
      (is (= errored (p/validate-message! errored))))))

(deftest stream-rejects-bad-header-and-size-before-payload
  (doseq [header ["" "bogus000\n" "00000001 " "0000000A\n" "FFFFFFFF\n"
                  "00000000\n" "00100001\n" "0000001g\n" " 0000001\n"
                  "0000001\n" "000000001\n"]]
    (let [reason (cond
                   (= header "") nil
                   (< (count header) 9) :truncated
                   (#{"00000000\n" "00100001\n"} header) :size
                   :else :header)]
      (if reason
        (invalid! reason #(p/read-frame! (stream (utf8-bytes header))))
        (is (nil? (p/read-frame! (stream (utf8-bytes header))))))))
  ;; Valid header with no payload is truncation, never a successful EOF.
  (invalid! :truncated #(p/read-frame! (stream (utf8-bytes "00000004\n")))))

(deftest partial-frame-and-malformed-payload
  (let [frame (p/encode-frame (message :hello))
        first-9 (Arrays/copyOfRange ^bytes frame 0 9)]
    (doseq [n [1 4 8]]
      (invalid! :truncated
                #(p/read-frame! (stream (Arrays/copyOfRange ^bytes frame 0 n)))))
    (invalid! :truncated #(p/read-frame! (stream first-9)))
    (invalid! :truncated #(p/read-frame!
                            (stream (Arrays/copyOfRange ^bytes frame 0
                                                        (dec (alength ^bytes frame)))))))
  (invalid! :utf8 #(p/read-frame! (stream (frame-with-payload (byte-array [(byte -61)])))))
  (invalid! :size #(p/read-frame! (stream (frame-with-payload ""))))
  (invalid! :edn #(p/read-frame! (stream (frame-with-payload "{"))))
  (invalid! :edn #(p/read-frame! (stream (frame-with-payload "nil nil"))))
  (invalid! :edn #(p/read-frame! (stream (frame-with-payload "#evil/x 1")))))

(deftest stdout-data-cannot-be-mistaken-for-control
  (let [control (p/encode-frame (message :hello))
        stdout (utf8-bytes "Starting test...\n")
        mixed (byte-array (+ (alength ^bytes stdout) (alength ^bytes control)))
        trailing (byte-array (+ (alength ^bytes control) (alength ^bytes stdout)))]
    (System/arraycopy stdout 0 mixed 0 (alength ^bytes stdout))
    (System/arraycopy control 0 mixed (alength ^bytes stdout) (alength ^bytes control))
    (System/arraycopy control 0 trailing 0 (alength ^bytes control))
    (System/arraycopy stdout 0 trailing (alength ^bytes control) (alength ^bytes stdout))
    (invalid! :header #(p/read-frame! (stream mixed)))
    (let [input (stream trailing)]
      (is (= (message :hello) (p/read-frame! input)))
      (invalid! :header #(p/read-frame! input)))))

(deftest stream-chunking-and-no-progress
  (let [data (p/encode-frame (message :run))
        cursor (atom 0)
        input (proxy [InputStream] []
                (read
                  ([] (let [pos @cursor]
                        (if (>= pos (alength ^bytes data)) -1
                            (do (swap! cursor inc)
                                (bit-and 0xff (aget ^bytes data pos))))))
                  ([buffer off len]
                   (let [pos @cursor]
                     (if (>= pos (alength ^bytes data)) -1
                         (do (aset-byte buffer off (aget ^bytes data pos))
                             (swap! cursor inc)
                             1))))))]
    (is (= (message :run) (p/read-frame! input)))
    (is (nil? (p/read-frame! input))))
  (let [zero-input (proxy [InputStream] []
                     (read
                       ([] 0)
                       ([buffer off len] 0)))]
    (invalid! :stream #(p/read-frame! zero-input))))

(deftest caller-must-supply-correct-stream-types
  (invalid! :stream #(p/write-frame! nil (message :hello)))
  (invalid! :stream #(p/read-frame! nil)))

