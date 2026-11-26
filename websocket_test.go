package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	collector "github.com/luanruisong/hyperliquid-collector"
)

const (
	fillsFile     = "node_fills_streaming/hourly/20261126/3"
	miscFile      = "misc_events_streaming/hourly/20261126/3"
	twapFile      = "node_twap_statuses_streaming/hourly/20261126/3"
	fillEvents    = `[["0xabc",{"coin":"ETH","px":"3600","sz":"0.2"}]]`
	miscEvents    = `[{"type":"funding","delta":"0.5"}]`
	twapEvents    = `[{"status":"activated","twapId":42}]`
	testBlockTime = "2026-11-26T03:00:00.123456789Z"
	testLocalTime = "2026-11-26T03:00:01.987654321Z"
	testIOTimeout = 5 * time.Second
)

func TestDiskRecordsReachOnlySubscribers(t *testing.T) {
	dir := t.TempDir()
	endpoint := startTestWebSocket(t)
	first := dialTestSubscriber(t, endpoint)
	second := dialTestSubscriber(t, endpoint)
	idle := dialTestSubscriber(t, endpoint)

	subscription(t, first, "subscribe", "fills", "subscribed")
	subscription(t, first, "subscribe", "fills", "subscribed")
	subscription(t, first, "subscribe", "twapStatuses", "subscribed")
	subscription(t, second, "subscribe", "fills", "subscribed")
	subscription(t, second, "subscribe", "miscEvents", "subscribed")
	startTestCollector(t, collector.Config{DataDir: dir}, endpoint.hub)

	appendNodeFile(t, dir, fillsFile, nodeRecord(1, fillEvents))
	expectData(t, first, "fills", 1, fillEvents)
	expectData(t, second, "fills", 1, fillEvents)

	appendNodeFile(t, dir, miscFile, nodeRecord(2, miscEvents))
	expectData(t, second, "miscEvents", 2, miscEvents)
	appendNodeFile(t, dir, twapFile, nodeRecord(3, twapEvents))
	expectData(t, first, "twapStatuses", 3, twapEvents)

	// Each reply follows any data already queued for that connection.
	subscription(t, idle, "unsubscribe", "fills", "unsubscribed")
	subscription(t, first, "unsubscribe", "fills", "unsubscribed")
	appendNodeFile(t, dir, fillsFile, nodeRecord(4, fillEvents))
	expectData(t, second, "fills", 4, fillEvents)

	appendNodeFile(t, dir, twapFile, nodeRecord(5, twapEvents))
	expectData(t, first, "twapStatuses", 5, twapEvents)
	subscription(t, second, "unsubscribe", "miscEvents", "unsubscribed")
	subscription(t, idle, "unsubscribe", "twapStatuses", "unsubscribed")
}

func TestDiskBroadcastWaitsForCompleteLinesAndFollowsNewDay(t *testing.T) {
	dir := t.TempDir()
	endpoint := startTestWebSocket(t)
	subscriber := dialTestSubscriber(t, endpoint)
	subscription(t, subscriber, "subscribe", "fills", "subscribed")
	startTestCollector(t, collector.Config{DataDir: dir}, endpoint.hub)

	line := nodeRecord(10, fillEvents)
	const nextFill = `[["0xdef",{"coin":"ETH","px":"3601","sz":"0.3"}]]`
	next := strings.TrimSuffix(nodeRecord(10, nextFill), "}\n")
	appendNodeFile(t, dir, fillsFile, line+next)
	expectData(t, subscriber, "fills", 10, fillEvents)
	subscription(t, subscriber, "subscribe", "fills", "subscribed")
	appendNodeFile(t, dir, fillsFile, "}\n")
	expectData(t, subscriber, "fills", 10, nextFill)

	const nextDay = "node_fills_streaming/hourly/20261127/0"
	appendNodeFile(t, dir, nextDay, nodeRecord(11, fillEvents))
	expectData(t, subscriber, "fills", 11, fillEvents)
}

func TestDiskBroadcastResumesFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := collector.Config{DataDir: dir, CheckpointPath: filepath.Join(t.TempDir(), "checkpoint.json")}

	firstEndpoint := startTestWebSocket(t)
	first := dialTestSubscriber(t, firstEndpoint)
	subscription(t, first, "subscribe", "fills", "subscribed")
	stop := startTestCollector(t, cfg, firstEndpoint.hub)

	appendNodeFile(t, dir, fillsFile, nodeRecord(20, fillEvents))
	expectData(t, first, "fills", 20, fillEvents)
	stop()
	appendNodeFile(t, dir, fillsFile, nodeRecord(21, fillEvents))

	restarted := startTestWebSocket(t)
	subscriber := dialTestSubscriber(t, restarted)
	subscription(t, subscriber, "subscribe", "fills", "subscribed")
	startTestCollector(t, cfg, restarted.hub)
	expectData(t, subscriber, "fills", 21, fillEvents)

	appendNodeFile(t, dir, fillsFile, nodeRecord(22, fillEvents))
	expectData(t, subscriber, "fills", 22, fillEvents)
}

func TestWebSocketRejectsInvalidCommands(t *testing.T) {
	endpoint := startTestWebSocket(t)
	subscriber := dialTestSubscriber(t, endpoint)
	cases := []struct {
		name    string
		kind    int
		payload string
		channel string
	}{
		{"malformed JSON", websocket.TextMessage, `{"method":`, ""},
		{"binary command", websocket.BinaryMessage, `{"method":"subscribe","channel":"fills"}`, ""},
		{"unknown channel", websocket.TextMessage, `{"method":"subscribe","channel":"unknown"}`, "unknown"},
		{"missing channel", websocket.TextMessage, `{"method":"subscribe"}`, ""},
		{"unknown method", websocket.TextMessage, `{"method":"publish","channel":"fills"}`, "fills"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeTestFrame(t, subscriber, tc.kind, tc.payload)
			reply := expectFrame(t, subscriber, "error", tc.channel)
			if reply.Message == "" {
				t.Fatal("error reply had no message")
			}
		})
	}
	subscription(t, subscriber, "subscribe", "fills", "subscribed")
}

type testWebSocket struct {
	url string
	hub *hub
}

func startTestWebSocket(t *testing.T) testWebSocket {
	t.Helper()
	h := newHub()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", h.serveWebSocket)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	t.Cleanup(h.close)
	return testWebSocket{url: "ws" + strings.TrimPrefix(server.URL, "http") + "/ws", hub: h}
}

func dialTestSubscriber(t *testing.T, endpoint testWebSocket) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{HandshakeTimeout: testIOTimeout}
	conn, response, err := dialer.Dial(endpoint.url, nil)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		t.Fatalf("connect subscriber to %s: %v", endpoint.url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func startTestCollector(t *testing.T, cfg collector.Config, h *hub) func() {
	t.Helper()
	source, err := collector.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- source.Run(ctx, h.publish) }()
	stop := sync.OnceFunc(func() {
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("collector stopped with %v", err)
			}
		case <-time.After(testIOTimeout):
			t.Error("collector did not stop")
		}
	})
	t.Cleanup(stop)
	return stop
}

func nodeRecord(block uint64, events string) string {
	return fmt.Sprintf(`{"block_number":%d,"block_time":%q,"local_time":%q,"events":%s}`+"\n", block, testBlockTime, testLocalTime, events)
}

func appendNodeFile(t *testing.T, dir, relativeFile, content string) {
	t.Helper()
	path := filepath.Join(dir, relativeFile)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString(content)
	if err := errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
}

type receivedFrame struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
}

func expectFrame(t *testing.T, conn *websocket.Conn, kind, channel string) receivedFrame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatal(err)
	}
	opcode, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read %s/%s: %v", kind, channel, err)
	}
	if opcode != websocket.TextMessage {
		t.Fatalf("got WebSocket opcode %d, want text", opcode)
	}
	var frame receivedFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		t.Fatalf("decode WebSocket message: %v", err)
	}
	if frame.Type != kind || frame.Channel != channel {
		t.Fatalf("got %s, want %s/%s", payload, kind, channel)
	}
	return frame
}

func expectData(t *testing.T, conn *websocket.Conn, channel string, block uint64, events string) {
	t.Helper()
	frame := expectFrame(t, conn, "data", channel)
	var data struct {
		BlockNumber uint64          `json:"block_number"`
		BlockTime   string          `json:"block_time"`
		LocalTime   string          `json:"local_time"`
		Events      json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(frame.Data, &data); err != nil {
		t.Fatalf("decode %s data: %v", channel, err)
	}
	if data.BlockNumber != block || data.BlockTime != testBlockTime ||
		data.LocalTime != testLocalTime || string(data.Events) != events {
		t.Fatalf("unexpected %s data: %s", channel, frame.Data)
	}
}

func subscription(t *testing.T, conn *websocket.Conn, method, channel, ack string) {
	t.Helper()
	writeTestFrame(t, conn, websocket.TextMessage, fmt.Sprintf(`{"method":%q,"channel":%q}`, method, channel))
	expectFrame(t, conn, ack, channel)
}

func writeTestFrame(t *testing.T, conn *websocket.Conn, kind int, payload string) {
	t.Helper()
	if err := conn.SetWriteDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(kind, []byte(payload)); err != nil {
		t.Fatal(err)
	}
}
