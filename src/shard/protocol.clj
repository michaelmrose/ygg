(ns shard.protocol
  "Strict, transport-independent protocol for long-lived Shard JVM sessions.

  Host owns job claims and journals; a JVM worker never owns the job basket.
  Messages travel ONLY over a private control stream, never stdout/stderr.
  The nine-byte header is eight lowercase ASCII hex digits and LF, followed
  by exactly that many UTF-8 EDN bytes. Streams are caller-owned.

  This module validates individual messages and bounded framing. Session
  state, allowed sequencing, runtime admission, timeouts, and sandbox setup
  belong to the supervisor/worker implementations in later revisions."
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import [java.io ByteArrayOutputStream InputStream OutputStream
            PushbackReader StringReader]
           [java.nio ByteBuffer]
           [java.nio.charset CodingErrorAction StandardCharsets]
           [java.security SecureRandom]))

(def protocol-version 1)
(def max-frame-bytes (* 1024 1024))
(def ^:private header-size 9)
(def ^:private max-seq Long/MAX_VALUE)
(def ^:private max-timeout-ms 86400000)
(def ^:private max-message-length 2048)
(def ^:private id-pattern #"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
(def ^:private nonce-pattern #"[0-9a-f]{32}")
(def ^:private fingerprint-pattern #"[0-9a-f]{64}")
(def ^:private namespace-pattern
  #"[A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)+")
(def ^:private header-pattern #"[0-9a-f]{8}\n")
(def ^:private types-host #{:hello :run :shutdown})
(def ^:private types-worker #{:ready :result :error :stopped})
(def ^:private types-all (set/union types-host types-worker))
(def ^:private shared-keys
  #{:version :type :session-id :worker-id :runtime-fingerprint :seq})
(def ^:private extra-keys
  {:hello #{}
   :ready #{:pid}
   :run #{:job-id :namespace :timeout-ms}
   :result #{:job-id :namespace :pid :status :tests :passes
             :failures :errors :assertions :elapsed-ms}
   :error #{:job-id :pid :reason :message}
   :shutdown #{}
   :stopped #{:pid}})
(def ^:private error-reasons
  #{:unknown-namespace :execution-error :protocol-error
    :runtime-mismatch :worker-unavailable})

(defn- reject! [reason message details]
  (throw (ex-info message (assoc details :shard.protocol/reason reason))))

(defn- demand! [test reason message details]
  (when-not test (reject! reason message details)))

(defn- id? [x] (and (string? x) (boolean (re-matches id-pattern x))))
(defn- hex? [pattern x]
  (and (string? x) (boolean (re-matches pattern x))))
(defn- whole? [x lo hi] (and (integer? x) (<= lo x hi)))

(defn- valid-common! [{:keys [version type session-id worker-id runtime-fingerprint seq] :as m}]
  (demand! (= protocol-version version) :version "Unsupported protocol version"
           {:version version :expected protocol-version})
  (demand! (types-all type) :type "Unsupported control message type" {:type type})
  (demand! (hex? nonce-pattern session-id) :session "Invalid session nonce" {})
  (demand! (id? worker-id) :worker "Invalid worker ID" {})
  (demand! (hex? fingerprint-pattern runtime-fingerprint)
           :runtime "Invalid runtime fingerprint" {})
  (demand! (whole? seq 0 max-seq) :sequence "Invalid message sequence" {:seq seq})
  (demand! (if (#{:hello :ready} type) (zero? seq) (pos? seq))
           :sequence "Invalid sequence for message type" {:type type :seq seq})
  (let [expected (set/union shared-keys (get extra-keys type))
        actual (set (keys m))]
    (demand! (= expected actual) :shape "Missing or unknown control message keys"
             {:type type :missing (set/difference expected actual)
              :unknown (set/difference actual expected)}))
  m)

(defn- validate-specific! [{:keys [type pid job-id namespace timeout-ms status
                                    tests passes failures errors assertions elapsed-ms
                                    reason message] :as m}]
  (when (#{:ready :result :error :stopped} type)
    (demand! (whole? pid 1 max-seq) :pid "Invalid JVM PID" {:pid pid}))
  (when (#{:run :result} type)
    (demand! (id? job-id) :job "Invalid job ID" {:job-id job-id})
    (demand! (and (string? namespace)
                  (<= (count namespace) 255)
                  (boolean (re-matches namespace-pattern namespace)))
             :namespace "Invalid JVM test namespace" {:namespace namespace}))
  (when (= :run type)
    (demand! (whole? timeout-ms 1 max-timeout-ms) :timeout
             "Invalid job timeout" {:timeout-ms timeout-ms}))
  (when (= :result type)
    (doseq [[field value lo] [[:tests tests 1] [:passes passes 0]
                             [:failures failures 0] [:errors errors 0]
                             [:assertions assertions 1] [:elapsed-ms elapsed-ms 0]]]
      (demand! (whole? value lo max-seq) :counts "Invalid test result count"
               {:field field :value value}))
    (demand! (and (whole? passes 0 max-seq)
                  (whole? failures 0 max-seq)
                  (whole? errors 0 max-seq)
                  (whole? assertions 1 max-seq)
                  (= (bigint assertions)
                     (+ (bigint passes) (bigint failures) (bigint errors))))
             :counts "Assertion counts do not reconcile" {})
    (demand! (#{:passed :failed} status) :status "Invalid test result status"
             {:status status})
    (demand! (= (= :passed status) (and (zero? failures) (zero? errors)))
             :status "Result status contradicts failure/error counters" {}))
  (when (= :error type)
    (demand! (or (nil? job-id) (id? job-id)) :job "Invalid failing job ID" {})
    (demand! (error-reasons reason) :reason "Unknown worker error reason"
             {:reason reason})
    (demand! (and (string? message) (not (str/blank? message))
                  (<= (count message) max-message-length))
             :message "Invalid bounded worker error message" {}))
  m)

(defn validate-message!
  "Validate an EDN control message, returning it unchanged or throwing ex-info.

  Optional direction is :host-to-worker or :worker-to-host. This validates
  message *shape*, NOT its position in a session transcript. The session
  implementation MUST check the expected nonce, fingerprint, sequence,
  worker/job binding and lifecycle before accepting an incoming message."
  ([message] (validate-message! message nil))
  ([message direction]
   (demand! (map? message) :shape "Control message must be an EDN map" {})
   (demand! (or (nil? direction)
                (#{:host-to-worker :worker-to-host} direction))
            :direction "Unknown protocol direction" {:direction direction})
   (valid-common! message)
   (when direction
     (demand! ((if (= direction :host-to-worker) types-host types-worker)
               (:type message))
              :direction "Control message has incorrect direction"
              {:type (:type message) :direction direction}))
   (validate-specific! message)))

(defn new-session-id
  "Generate a cryptographically random 128-bit session nonce (lowercase hex)."
  []
  (let [bytes (byte-array 16)]
    (.nextBytes (SecureRandom.) bytes)
    (apply str (map #(format "%02x" (bit-and 0xff %)) bytes))))

(defn- decode-utf8! [^bytes bytes]
  (try
    (let [decoder (doto (.newDecoder StandardCharsets/UTF_8)
                    (.onMalformedInput CodingErrorAction/REPORT)
                    (.onUnmappableCharacter CodingErrorAction/REPORT))]
      (str (.decode decoder (ByteBuffer/wrap bytes))))
    (catch java.nio.charset.CharacterCodingException e
      (reject! :utf8 "Malformed UTF-8 control payload" {}))))

(defn- read-edn! [^String text]
  (try
    (with-open [reader (PushbackReader. (StringReader. text))]
      (let [opts {:eof ::end :readers {}
                  :default (fn [tag _]
                             (reject! :edn "Tagged EDN is not admitted" {:tag tag}))}
            value (edn/read opts reader)]
        (demand! (not= ::end value) :edn "Empty control payload" {})
        (demand! (= ::end (edn/read opts reader)) :edn
                 "Multiple EDN forms in control frame" {})
        value))
    (catch clojure.lang.ExceptionInfo e (throw e))
    (catch Exception e
      (reject! :edn "Invalid EDN control payload" {:cause (.getMessage e)}))))

(defn encode-frame
  "Encode and validate one message into a bounded UTF-8 frame.

  Framing: exactly eight lowercase ASCII hexadecimal byte-count digits, LF,
  and the EDN payload bytes. Does not close streams or include test output."
  ([message] (encode-frame message nil))
  ([message direction]
   (validate-message! message direction)
   (let [^bytes payload (.getBytes (pr-str message) StandardCharsets/UTF_8)
         length (alength payload)]
     (demand! (<= 1 length max-frame-bytes) :size "Control frame exceeds size limit"
              {:bytes length :maximum max-frame-bytes})
     (let [header (.getBytes (format "%08x\n" length) StandardCharsets/US_ASCII)
           out (ByteArrayOutputStream. (+ header-size length))]
       (.write out header)
       (.write out payload)
       (.toByteArray out)))))

(defn write-frame!
  "Write one complete bounded frame to the dedicated control OutputStream."
  ([^OutputStream stream message] (write-frame! stream message nil))
  ([^OutputStream stream message direction]
   (demand! (instance? OutputStream stream) :stream
            "Control writer requires an OutputStream" {})
   (let [bytes (encode-frame message direction)]
     (.write stream ^bytes bytes)
     (.flush stream)
     nil)))

(defn- read-exactly!
  [^InputStream stream length allow-clean-eof?]
  (let [bytes (byte-array length)]
    (loop [offset 0 zero-reads 0]
      (if (= offset length)
        bytes
        (let [n (.read stream bytes offset (- length offset))]
          (cond
            (= -1 n)
            (if (and allow-clean-eof? (zero? offset))
              nil
              (reject! :truncated "Truncated control frame"
                       {:expected length :received offset}))
            (zero? n)
            (if (>= zero-reads 4)
              (reject! :stream "Control stream made no progress" {:received offset})
              (recur offset (inc zero-reads)))
            :else (recur (+ offset n) 0)))))))

(defn read-frame!
  "Read and validate one frame from a dedicated control InputStream.

  Returns nil only on clean EOF before a new header. Rejects malformed,
  oversize, partial, untrusted or wrong-direction messages. The stream must
  be bounded by the owner for wall-clock timeout; this function does not
  implement a read deadline and does not close the stream."
  ([^InputStream stream] (read-frame! stream nil))
  ([^InputStream stream direction]
   (demand! (instance? InputStream stream) :stream
            "Control reader requires an InputStream" {})
   (when-let [header-bytes (read-exactly! stream header-size true)]
     (let [header (String. ^bytes header-bytes StandardCharsets/US_ASCII)]
       (demand! (boolean (re-matches header-pattern header))
                :header "Malformed control frame header" {})
       (let [length (Long/parseLong (subs header 0 8) 16)]
         (demand! (<= 1 length max-frame-bytes) :size
                  "Control frame length outside admitted range"
                  {:length length :maximum max-frame-bytes})
         (let [payload (read-exactly! stream (int length) false)
               value (read-edn! (decode-utf8! payload))]
           (validate-message! value direction)))))))
