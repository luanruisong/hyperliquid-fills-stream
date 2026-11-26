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
	"testing"
	"time"

	"github.com/gorilla/websocket"
	collector "github.com/luanruisong/hyperliquid-collector"
)

func TestDiskRecordsReachOnlySubscribers(t *testing.T) {
	dir := t.TempDir()
	writeRecord := func(stream collector.Stream, block int) {
		t.Helper()
		hour := filepath.Join(dir, string(stream), "hourly", "20261126")
		if err := os.MkdirAll(hour, 0700); err != nil {
			t.Fatal(err)
		}
		events := `[{"status":"ok"}]`
		if stream == collector.Fills {
			events = `[["0xabc",{"px":"3600"}]]`
		}
		line := fmt.Sprintf(`{"block_number":%d,"block_time":"2026-11-26T03:00:00.123456789Z","local_time":"2026-11-26T03:00:01Z","events":%s}`+"\n", block, events)
		file, err := os.OpenFile(filepath.Join(hour, "3"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.WriteString(line)
		if err := errors.Join(err, file.Close()); err != nil {
			t.Fatal(err)
		}
	}

	source, err := collector.New(collector.Config{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	h := newHub()
	server := httptest.NewServer(http.HandlerFunc(h.serveWebSocket))
	t.Cleanup(server.Close)
	t.Cleanup(h.close)

	dial := func() *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	read := func(conn *websocket.Conn, kind, channel string) message {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var reply message
		if err := conn.ReadJSON(&reply); err != nil {
			t.Fatal(err)
		}
		if reply.Type != kind || reply.Channel != channel {
			t.Fatalf("got %s/%s, want %s/%s", reply.Type, reply.Channel, kind, channel)
		}
		return reply
	}
	send := func(conn *websocket.Conn, method, channel, reply string) {
		t.Helper()
		if err := conn.WriteJSON(command{Method: method, Channel: channel}); err != nil {
			t.Fatal(err)
		}
		read(conn, reply, channel)
	}

	first, second := dial(), dial()
	send(first, "subscribe", "fills", "subscribed")
	send(second, "subscribe", "miscEvents", "subscribed")
	send(second, "subscribe", "twapStatuses", "subscribed")
	send(first, "subscribe", "unknown", "error")

	writeRecord(collector.Fills, 1)
	writeRecord(collector.MiscEvents, 2)
	writeRecord(collector.TWAPStatuses, 3)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- source.Run(ctx, h.publish) }()
	t.Cleanup(func() {
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Errorf("collector stopped with %v", err)
		}
	})

	fill := read(first, "data", "fills").Data
	if fill == nil || fill.BlockNumber != 1 || fill.BlockTime.Nanosecond() != 123456789 ||
		len(fill.Events) != 1 || string(fill.Events[0]) != `["0xabc",{"px":"3600"}]` {
		t.Fatalf("unexpected fill: %+v", fill)
	}
	blocks := map[string]uint64{"miscEvents": 2, "twapStatuses": 3}
	for range 2 {
		if err := second.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var reply message
		if err := second.ReadJSON(&reply); err != nil {
			t.Fatal(err)
		}
		block, expected := blocks[reply.Channel]
		if !expected || reply.Type != "data" || reply.Data == nil || reply.Data.BlockNumber != block ||
			len(reply.Data.Events) != 1 || !json.Valid(reply.Data.Events[0]) {
			t.Fatalf("unexpected message: %+v", reply)
		}
		delete(blocks, reply.Channel)
	}

	send(first, "unsubscribe", "fills", "unsubscribed")
	send(second, "subscribe", "fills", "subscribed")
	writeRecord(collector.Fills, 4)
	next := read(second, "data", "fills").Data
	if next == nil || next.BlockNumber != 4 {
		t.Fatalf("unexpected appended fill: %+v", next)
	}
	send(first, "subscribe", "fills", "subscribed")
}
