package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	collector "github.com/luanruisong/hyperliquid-collector"
)

var channelNames = map[collector.Stream]string{
	collector.Fills:        "fills",
	collector.MiscEvents:   "miscEvents",
	collector.TWAPStatuses: "twapStatuses",
}

const (
	writeTimeout = 5 * time.Second
	pingInterval = 30 * time.Second
	pongTimeout  = 60 * time.Second
)

var upgrader = websocket.Upgrader{HandshakeTimeout: writeTimeout}

type command struct {
	Method  string `json:"method"`
	Channel string `json:"channel"`
}

type message struct {
	Type    string      `json:"type"`
	Channel string      `json:"channel,omitempty"`
	Data    *recordData `json:"data,omitempty"`
	Message string      `json:"message,omitempty"`
}

type recordData struct {
	BlockNumber uint64            `json:"block_number"`
	BlockTime   time.Time         `json:"block_time"`
	LocalTime   time.Time         `json:"local_time"`
	Events      []json.RawMessage `json:"events"`
}

type client struct {
	conn     *websocket.Conn
	outbound chan []byte
	done     chan struct{}
}

type hub struct {
	mu          sync.Mutex
	closed      bool
	clients     map[*client]struct{}
	subscribers map[string]map[*client]struct{}
}

func newHub() *hub {
	h := &hub{
		clients:     make(map[*client]struct{}),
		subscribers: make(map[string]map[*client]struct{}),
	}
	for _, name := range channelNames {
		h.subscribers[name] = make(map[*client]struct{})
	}
	return h
}

func (h *hub) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &client{
		conn:     conn,
		outbound: make(chan []byte, 32),
		done:     make(chan struct{}),
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = conn.Close()
		return
	}
	h.clients[c] = struct{}{}
	h.mu.Unlock()

	go h.write(c)
	defer h.remove(c)
	conn.SetReadLimit(4096)
	if err := conn.SetReadDeadline(time.Now().Add(pongTimeout)); err != nil {
		return
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongTimeout))
	})

	for {
		kind, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var request command
		if kind != websocket.TextMessage || json.Unmarshal(payload, &request) != nil {
			h.handleCommand(c, command{}, "expected a JSON command in a text frame")
			continue
		}
		h.handleCommand(c, request, "")
	}
}

func (h *hub) handleCommand(c *client, request command, invalid string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, connected := h.clients[c]; !connected {
		return
	}

	reply := message{Type: "error", Channel: request.Channel}
	subscribers, supported := h.subscribers[request.Channel]
	switch {
	case invalid != "":
		reply.Message = invalid
	case request.Method != "subscribe" && request.Method != "unsubscribe":
		reply.Message = "method must be subscribe or unsubscribe"
	case !supported:
		reply.Message = "unknown channel"
	case request.Method == "subscribe":
		subscribers[c] = struct{}{}
		reply.Type = "subscribed"
	default:
		delete(subscribers, c)
		reply.Type = "unsubscribed"
	}

	// Enqueue the acknowledgement under the same lock as data delivery.
	payload, _ := json.Marshal(reply)
	h.enqueue(c, payload)
}

func (h *hub) publish(_ context.Context, record collector.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	channel := channelNames[record.Stream]
	subscribers := h.subscribers[channel]
	if len(subscribers) == 0 {
		return nil
	}
	payload, err := json.Marshal(message{
		Type:    "data",
		Channel: channel,
		Data: &recordData{
			BlockNumber: record.BlockNumber,
			BlockTime:   record.BlockTime,
			LocalTime:   record.LocalTime,
			Events:      record.Events,
		},
	})
	if err != nil {
		return err
	}
	for c := range subscribers {
		h.enqueue(c, payload)
	}
	return nil
}

// enqueue and removeLocked are called with h.mu held.
func (h *hub) enqueue(c *client, payload []byte) {
	select {
	case c.outbound <- payload:
	default:
		h.removeLocked(c)
	}
}

func (h *hub) removeLocked(c *client) {
	if _, connected := h.clients[c]; !connected {
		return
	}
	delete(h.clients, c)
	for _, subscribers := range h.subscribers {
		delete(subscribers, c)
	}
	close(c.done)
	_ = c.conn.Close()
}

func (h *hub) remove(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(c)
}

func (h *hub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for c := range h.clients {
		h.removeLocked(c)
	}
}

func (h *hub) write(c *client) {
	defer h.remove(c)
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case payload := <-c.outbound:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				return
			}
		}
	}
}
