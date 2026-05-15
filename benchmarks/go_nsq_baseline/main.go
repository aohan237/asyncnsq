package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

const (
	headerSize = 16
	mib        = 1024 * 1024
)

var allScenarios = []string{"pub", "mpub", "e2e"}

type profile struct {
	messages              int
	payloadSize           int
	concurrency           int
	batchSize             int
	maxInFlight           int
	timeout               time.Duration
	outputBufferTimeoutMS int
}

var profiles = map[string]profile{
	"quick": {
		messages:              5000,
		payloadSize:           256,
		concurrency:           64,
		batchSize:             100,
		maxInFlight:           256,
		timeout:               60 * time.Second,
		outputBufferTimeoutMS: 25,
	},
	"pr": {
		messages:              50000,
		payloadSize:           512,
		concurrency:           256,
		batchSize:             250,
		maxInFlight:           1024,
		timeout:               180 * time.Second,
		outputBufferTimeoutMS: 25,
	},
	"stress": {
		messages:              250000,
		payloadSize:           1024,
		concurrency:           512,
		batchSize:             500,
		maxInFlight:           2048,
		timeout:               600 * time.Second,
		outputBufferTimeoutMS: 25,
	},
}

type config struct {
	profile               string
	runID                 string
	tcpAddresses          []string
	httpAddresses         []string
	scenarios             []string
	messages              int
	payloadSize           int
	concurrency           int
	batchSize             int
	maxInFlight           int
	timeout               time.Duration
	outputBufferTimeoutMS int
	warmup                int
	cleanup               bool
	markdownPath          string
	jsonPath              string
}

type result struct {
	Scenario       string   `json:"scenario"`
	Messages       int      `json:"messages"`
	PayloadBytes   int      `json:"payload_bytes"`
	BatchSize      *int     `json:"batch_size"`
	Concurrency    int      `json:"concurrency"`
	DurationS      float64  `json:"duration_s"`
	ThroughputMsgS float64  `json:"throughput_msg_s"`
	ThroughputMiBS float64  `json:"throughput_mib_s"`
	P50MS          *float64 `json:"p50_ms"`
	P95MS          *float64 `json:"p95_ms"`
	P99MS          *float64 `json:"p99_ms"`
	Errors         int64    `json:"errors"`
	Notes          string   `json:"notes"`
}

type report struct {
	Generated             string   `json:"generated"`
	RunID                 string   `json:"run_id"`
	Profile               string   `json:"profile"`
	Go                    string   `json:"go"`
	Client                string   `json:"client"`
	OS                    string   `json:"os"`
	Arch                  string   `json:"arch"`
	NSQDTCP               []string `json:"nsqd_tcp_addresses"`
	NSQDHTTP              []string `json:"nsqd_http_addresses"`
	OutputBufferTimeoutMS int      `json:"output_buffer_timeout_ms"`
	MaxInFlight           int      `json:"max_in_flight"`
	GoMaxProcs            int      `json:"gomaxprocs"`
	Results               []result `json:"results"`
	SuccessPolicy         string   `json:"success_policy"`
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	results, err := run(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "go-nsq baseline failed: %v\n", err)
		os.Exit(2)
	}
	markdown := markdownReport(cfg, results)
	fmt.Print(markdown)
	if cfg.markdownPath != "" {
		if err := os.WriteFile(cfg.markdownPath, []byte(markdown), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write markdown: %v\n", err)
			os.Exit(2)
		}
	}
	if cfg.jsonPath != "" {
		payload, err := json.MarshalIndent(jsonReport(cfg, results), "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
			os.Exit(2)
		}
		if err := os.WriteFile(cfg.jsonPath, payload, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			os.Exit(2)
		}
	}
	for _, result := range results {
		if result.Errors != 0 {
			os.Exit(1)
		}
	}
}

func parseFlags() (config, error) {
	profileName := flag.String("profile", "quick", "quick, pr, or stress")
	tcpAddresses := flag.String("nsqd-tcp-addresses", "127.0.0.1:4150,127.0.0.1:4250,127.0.0.1:4350", "comma-separated nsqd TCP addresses")
	httpAddresses := flag.String("nsqd-http-addresses", "127.0.0.1:4151,127.0.0.1:4251,127.0.0.1:4351", "comma-separated nsqd HTTP addresses")
	scenariosRaw := flag.String("scenarios", "all", "all or comma-separated pub,mpub,e2e")
	messages := flag.Int("messages", 0, "message count")
	payloadSize := flag.Int("payload-size", 0, "payload size in bytes")
	concurrency := flag.Int("concurrency", 0, "concurrent workers")
	batchSize := flag.Int("batch-size", 0, "MPUB batch size")
	maxInFlight := flag.Int("max-in-flight", 0, "consumer max in-flight")
	timeoutSeconds := flag.Float64("timeout", 0, "scenario timeout in seconds")
	outputBufferTimeoutMS := flag.Int("output-buffer-timeout-ms", -1, "consumer output_buffer_timeout in milliseconds")
	_ = flag.Int("consumer-processes", 1, "accepted for asyncnsq comparison scripts; ignored by go-nsq")
	warmup := flag.Int("warmup-messages", -1, "warmup messages")
	noCleanup := flag.Bool("no-cleanup", false, "skip topic cleanup")
	markdownPath := flag.String("markdown", "", "write markdown report")
	jsonPath := flag.String("json", "", "write JSON report")
	flag.Parse()

	base, ok := profiles[*profileName]
	if !ok {
		return config{}, fmt.Errorf("unknown profile %q", *profileName)
	}
	scenarios, err := parseScenarios(*scenariosRaw)
	if err != nil {
		return config{}, err
	}
	cfg := config{
		profile:               *profileName,
		runID:                 fmt.Sprintf("%08x", time.Now().UnixNano()),
		tcpAddresses:          splitCSV(*tcpAddresses),
		httpAddresses:         splitCSV(*httpAddresses),
		scenarios:             scenarios,
		messages:              valueOrDefault(*messages, base.messages),
		payloadSize:           valueOrDefault(*payloadSize, base.payloadSize),
		concurrency:           valueOrDefault(*concurrency, base.concurrency),
		batchSize:             valueOrDefault(*batchSize, base.batchSize),
		maxInFlight:           valueOrDefault(*maxInFlight, base.maxInFlight),
		timeout:               base.timeout,
		outputBufferTimeoutMS: base.outputBufferTimeoutMS,
		warmup:                *warmup,
		cleanup:               !*noCleanup,
		markdownPath:          *markdownPath,
		jsonPath:              *jsonPath,
	}
	if *timeoutSeconds > 0 {
		cfg.timeout = time.Duration(*timeoutSeconds * float64(time.Second))
	}
	if *outputBufferTimeoutMS >= 0 {
		cfg.outputBufferTimeoutMS = *outputBufferTimeoutMS
	}
	if cfg.warmup < 0 {
		cfg.warmup = min(1000, cfg.messages/10)
	}
	if len(cfg.tcpAddresses) == 0 || len(cfg.httpAddresses) == 0 {
		return config{}, fmt.Errorf("at least one TCP and HTTP address is required")
	}
	if cfg.payloadSize < headerSize {
		return config{}, fmt.Errorf("payload size must be >= %d bytes", headerSize)
	}
	return cfg, nil
}

func valueOrDefault(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(strings.TrimPrefix(part, "tcp://"), "http://")
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func parseScenarios(raw string) ([]string, error) {
	if raw == "all" {
		return allScenarios, nil
	}
	allowed := map[string]bool{"pub": true, "mpub": true, "e2e": true}
	scenarios := splitCSV(raw)
	for _, scenario := range scenarios {
		if !allowed[scenario] {
			return nil, fmt.Errorf("unknown scenario %q", scenario)
		}
	}
	return scenarios, nil
}

func run(cfg config) ([]result, error) {
	if err := waitForCluster(cfg); err != nil {
		return nil, err
	}
	if cfg.warmup > 0 {
		topic := topicName(cfg.runID, "warmup")
		_, _ = runPubAck(cfg, topic, cfg.warmup, "warmup")
		cleanupTopic(cfg, topic)
	}
	results := make([]result, 0, len(cfg.scenarios))
	for _, scenario := range cfg.scenarios {
		topic := topicName(cfg.runID, scenario)
		var (
			result result
			err    error
		)
		switch scenario {
		case "pub":
			result, err = runPubAck(cfg, topic, cfg.messages, "go-nsq TCP PUB ack")
		case "mpub":
			result, err = runMPubAck(cfg, topic, cfg.messages)
		case "e2e":
			result, err = runE2E(cfg, topic)
		}
		cleanupTopic(cfg, topic)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func waitForCluster(cfg config) error {
	client := http.Client{Timeout: 2 * time.Second}
	for _, address := range cfg.httpAddresses {
		resp, err := client.Get("http://" + address + "/ping")
		if err != nil {
			return err
		}
		body := make([]byte, 2)
		_, _ = resp.Body.Read(body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 400 || string(body) != "OK" {
			return fmt.Errorf("nsqd HTTP endpoint is not ready: %s", address)
		}
	}
	return nil
}

func cleanupTopic(cfg config, topic string) {
	if !cfg.cleanup {
		return
	}
	for _, address := range cfg.httpAddresses {
		req, err := http.NewRequest(http.MethodPost, "http://"+address+"/topic/delete?topic="+topic, nil)
		if err != nil {
			continue
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}
}

func newProducers(cfg config) ([]*nsq.Producer, error) {
	producers := make([]*nsq.Producer, 0, len(cfg.tcpAddresses))
	logger := log.New(io.Discard, "", log.LstdFlags)
	for _, address := range cfg.tcpAddresses {
		producer, err := nsq.NewProducer(address, nsq.NewConfig())
		if err != nil {
			stopProducers(producers)
			return nil, err
		}
		producer.SetLogger(logger, nsq.LogLevelError)
		producers = append(producers, producer)
	}
	return producers, nil
}

func stopProducers(producers []*nsq.Producer) {
	for _, producer := range producers {
		producer.Stop()
	}
}

func buildPayload(payloadSize int, seq int) []byte {
	payload := make([]byte, payloadSize)
	binary.BigEndian.PutUint64(payload[0:8], uint64(seq))
	binary.BigEndian.PutUint64(payload[8:16], uint64(time.Now().UnixNano()))
	copy(payload[16:], bytesRepeat("go-nsq-baseline|", payloadSize-headerSize))
	return payload
}

func bytesRepeat(seed string, size int) []byte {
	if size <= 0 {
		return nil
	}
	src := []byte(seed)
	out := make([]byte, size)
	for offset := 0; offset < size; offset += len(src) {
		copy(out[offset:], src)
	}
	return out
}

func parsePayload(body []byte) (int, int64, error) {
	if len(body) < headerSize {
		return 0, 0, fmt.Errorf("message too small")
	}
	seq := int(binary.BigEndian.Uint64(body[0:8]))
	sentNS := int64(binary.BigEndian.Uint64(body[8:16]))
	return seq, sentNS, nil
}

func runPubAck(cfg config, topic string, messages int, scenarioName string) (result, error) {
	producers, err := newProducers(cfg)
	if err != nil {
		return result{}, err
	}
	defer stopProducers(producers)

	workerCount := min(cfg.concurrency, max(1, messages))
	latencies := make([]uint64, 0, messages)
	var latencyMu sync.Mutex
	var errors int64
	var wg sync.WaitGroup
	started := time.Now()
	cancel := make(chan struct{})
	done := make(chan struct{})

	for workerID := 0; workerID < workerCount; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			producer := producers[workerID%len(producers)]
			for seq := workerID; seq < messages; seq += workerCount {
				select {
				case <-cancel:
					return
				default:
				}
				startedNS := time.Now()
				if err := producer.Publish(topic, buildPayload(cfg.payloadSize, seq)); err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				latencyMu.Lock()
				latencies = append(latencies, uint64(time.Since(startedNS).Nanoseconds()))
				latencyMu.Unlock()
			}
		}(workerID)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(cfg.timeout):
		close(cancel)
		<-done
		latencyMu.Lock()
		successes := len(latencies)
		latencyMu.Unlock()
		missing := messages - successes - int(atomic.LoadInt64(&errors))
		if missing > 0 {
			atomic.AddInt64(&errors, int64(missing))
		}
	}
	duration := time.Since(started)
	return makeResult(
		scenarioName, messages, cfg.payloadSize, nil, workerCount,
		duration, latencies, atomic.LoadInt64(&errors),
		"official go-nsq producer Publish ACK latency",
	), nil
}

func runMPubAck(cfg config, topic string, messages int) (result, error) {
	producers, err := newProducers(cfg)
	if err != nil {
		return result{}, err
	}
	defer stopProducers(producers)

	totalBatches := int(math.Ceil(float64(messages) / float64(cfg.batchSize)))
	workerCount := min(cfg.concurrency, max(1, totalBatches))
	latencies := make([]uint64, 0, totalBatches)
	var latencyMu sync.Mutex
	var errors int64
	var sent int64
	var wg sync.WaitGroup
	started := time.Now()
	cancel := make(chan struct{})
	done := make(chan struct{})

	for workerID := 0; workerID < workerCount; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			producer := producers[workerID%len(producers)]
			for batchID := workerID; batchID < totalBatches; batchID += workerCount {
				select {
				case <-cancel:
					return
				default:
				}
				startSeq := batchID * cfg.batchSize
				endSeq := min(startSeq+cfg.batchSize, messages)
				payloads := make([][]byte, 0, endSeq-startSeq)
				for seq := startSeq; seq < endSeq; seq++ {
					payloads = append(payloads, buildPayload(cfg.payloadSize, seq))
				}
				startedNS := time.Now()
				if err := producer.MultiPublish(topic, payloads); err != nil {
					atomic.AddInt64(&errors, int64(len(payloads)))
					continue
				}
				atomic.AddInt64(&sent, int64(len(payloads)))
				latencyMu.Lock()
				latencies = append(latencies, uint64(time.Since(startedNS).Nanoseconds()))
				latencyMu.Unlock()
			}
		}(workerID)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(cfg.timeout):
		close(cancel)
		<-done
		missing := int64(messages) - atomic.LoadInt64(&sent) - atomic.LoadInt64(&errors)
		if missing > 0 {
			atomic.AddInt64(&errors, missing)
		}
	}
	duration := time.Since(started)
	batch := cfg.batchSize
	return makeResult(
		"go-nsq TCP MPUB batch ack", messages, cfg.payloadSize, &batch,
		workerCount, duration, latencies, atomic.LoadInt64(&errors),
		"official go-nsq MultiPublish ACK latency per batch",
	), nil
}

type handler struct {
	expected int
	seen     map[int]struct{}
	done     chan struct{}
	latency  []uint64
	mu       sync.Mutex
	errors   int64
}

func (h *handler) HandleMessage(message *nsq.Message) error {
	seq, sentNS, err := parsePayload(message.Body)
	if err != nil {
		atomic.AddInt64(&h.errors, 1)
		return nil
	}
	h.mu.Lock()
	if _, ok := h.seen[seq]; ok {
		atomic.AddInt64(&h.errors, 1)
	} else {
		h.seen[seq] = struct{}{}
		h.latency = append(h.latency, uint64(time.Now().UnixNano()-sentNS))
		if len(h.seen) >= h.expected {
			select {
			case <-h.done:
			default:
				close(h.done)
			}
		}
	}
	h.mu.Unlock()
	return nil
}

func runE2E(cfg config, topic string) (result, error) {
	channel := channelName(cfg.runID, "e2e")
	consumerCfg := nsq.NewConfig()
	consumerCfg.MaxInFlight = cfg.maxInFlight
	consumerCfg.OutputBufferTimeout = time.Duration(cfg.outputBufferTimeoutMS) * time.Millisecond
	consumer, err := nsq.NewConsumer(topic, channel, consumerCfg)
	if err != nil {
		return result{}, err
	}
	consumer.SetLogger(log.New(io.Discard, "", log.LstdFlags), nsq.LogLevelError)
	handler := &handler{
		expected: cfg.messages,
		seen:     make(map[int]struct{}, cfg.messages),
		done:     make(chan struct{}),
		latency:  make([]uint64, 0, cfg.messages),
	}
	consumer.AddConcurrentHandlers(handler, cfg.concurrency)
	for _, address := range cfg.tcpAddresses {
		if err := consumer.ConnectToNSQD(address); err != nil {
			consumer.Stop()
			return result{}, err
		}
	}
	time.Sleep(50 * time.Millisecond)

	started := time.Now()
	publishResult, err := runPubAck(cfg, topic, cfg.messages, "internal publisher")
	if err != nil {
		consumer.Stop()
		return result{}, err
	}
	select {
	case <-handler.done:
	case <-time.After(cfg.timeout):
	}
	duration := time.Since(started)
	consumer.Stop()
	<-consumer.StopChan

	handler.mu.Lock()
	seen := len(handler.seen)
	latencies := append([]uint64(nil), handler.latency...)
	handler.mu.Unlock()
	missing := cfg.messages - seen
	errors := publishResult.Errors + atomic.LoadInt64(&handler.errors) + int64(missing)
	notes := fmt.Sprintf("missing=%d, duplicates_or_decode_errors=%d", missing, handler.errors)
	return makeResult(
		"go-nsq end-to-end pub->fin", cfg.messages, cfg.payloadSize, nil,
		cfg.concurrency, duration, latencies, errors, notes,
	), nil
}

func makeResult(
	scenario string, messages int, payloadBytes int, batchSize *int,
	concurrency int, duration time.Duration, latencies []uint64,
	errors int64, notes string,
) result {
	durationS := math.Max(duration.Seconds(), 1e-9)
	success := float64(messages - int(errors))
	if success < 0 {
		success = 0
	}
	return result{
		Scenario:       scenario,
		Messages:       messages,
		PayloadBytes:   payloadBytes,
		BatchSize:      batchSize,
		Concurrency:    concurrency,
		DurationS:      durationS,
		ThroughputMsgS: success / durationS,
		ThroughputMiBS: success * float64(payloadBytes) / durationS / mib,
		P50MS:          percentileMS(latencies, 50),
		P95MS:          percentileMS(latencies, 95),
		P99MS:          percentileMS(latencies, 99),
		Errors:         errors,
		Notes:          notes,
	}
}

func percentileMS(values []uint64, percentile float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	ordered := append([]uint64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*percentile/100.0)) - 1
	if index < 0 {
		index = 0
	}
	value := float64(ordered[index]) / 1_000_000
	return &value
}

func topicName(runID, scenario string) string {
	name := "gnsq-bench-" + scenario + "-" + runID
	if len(name) > 64 {
		return name[:64]
	}
	return name
}

func channelName(runID, scenario string) string {
	name := "ch-" + scenario + "-" + runID
	if len(name) > 64 {
		return name[:64]
	}
	return name
}

func markdownReport(cfg config, results []result) string {
	var b strings.Builder
	fmt.Fprintln(&b, "# go-nsq Baseline Benchmark Report")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "| Field | Value |")
	fmt.Fprintln(&b, "| --- | --- |")
	fmt.Fprintf(&b, "| Generated | `%s` |\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "| Run ID | `%s` |\n", cfg.runID)
	fmt.Fprintf(&b, "| Profile | `%s` |\n", cfg.profile)
	fmt.Fprintf(&b, "| Go | `%s` |\n", runtime.Version())
	fmt.Fprintln(&b, "| Client | `github.com/nsqio/go-nsq v1.1.0` |")
	fmt.Fprintf(&b, "| Platform | `%s/%s` |\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&b, "| NSQD TCP | `%s` |\n", strings.Join(cfg.tcpAddresses, ", "))
	fmt.Fprintf(&b, "| NSQD HTTP | `%s` |\n", strings.Join(cfg.httpAddresses, ", "))
	fmt.Fprintf(&b, "| Output buffer timeout | `%dms` |\n", cfg.outputBufferTimeoutMS)
	fmt.Fprintf(&b, "| Max in-flight | `%d` |\n", cfg.maxInFlight)
	fmt.Fprintf(&b, "| GOMAXPROCS | `%d` |\n", runtime.GOMAXPROCS(0))
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "| Scenario | Messages | Payload | Batch | Concurrency | Duration | msg/s | MiB/s | p50 ms | p95 ms | p99 ms | Errors | Notes |")
	fmt.Fprintln(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
	for _, result := range results {
		fmt.Fprintf(
			&b,
			"| %s | %s | %s B | %s | %s | %.2fs | %.2f | %.2f | %s | %s | %s | %d | %s |\n",
			result.Scenario,
			formatInt(result.Messages),
			formatInt(result.PayloadBytes),
			formatBatch(result.BatchSize),
			formatInt(result.Concurrency),
			result.DurationS,
			result.ThroughputMsgS,
			result.ThroughputMiBS,
			formatMS(result.P50MS),
			formatMS(result.P95MS),
			formatMS(result.P99MS),
			result.Errors,
			result.Notes,
		)
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Success criteria: every row must report `Errors = 0`.")
	return b.String()
}

func jsonReport(cfg config, results []result) report {
	return report{
		Generated:             time.Now().UTC().Format(time.RFC3339),
		RunID:                 cfg.runID,
		Profile:               cfg.profile,
		Go:                    runtime.Version(),
		Client:                "github.com/nsqio/go-nsq v1.1.0",
		OS:                    runtime.GOOS,
		Arch:                  runtime.GOARCH,
		NSQDTCP:               cfg.tcpAddresses,
		NSQDHTTP:              cfg.httpAddresses,
		OutputBufferTimeoutMS: cfg.outputBufferTimeoutMS,
		MaxInFlight:           cfg.maxInFlight,
		GoMaxProcs:            runtime.GOMAXPROCS(0),
		Results:               results,
		SuccessPolicy:         "every row must report Errors = 0",
	}
}

func formatInt(value int) string {
	raw := fmt.Sprintf("%d", value)
	if len(raw) <= 3 {
		return raw
	}
	var out []byte
	for index, char := range reverse(raw) {
		if index > 0 && index%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(char))
	}
	return reverse(string(out))
}

func reverse(value string) string {
	runes := []rune(value)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func formatBatch(value *int) string {
	if value == nil {
		return "n/a"
	}
	return formatInt(*value)
}

func formatMS(value *float64) string {
	if value == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.3f", *value)
}
