//go:build !js

package webtorrent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anacrolix/torrent/tracker"
	"github.com/coder/websocket"
)

func TestTrackerAnnounceEventEncoding(t *testing.T) {
	for _, test := range []struct {
		name  string
		event tracker.AnnounceEvent
		want  string
	}{
		{"regular", tracker.None, ""},
		{"started", tracker.Started, "started"},
		{"completed", tracker.Completed, "completed"},
		{"stopped", tracker.Stopped, "stopped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			messages := make(chan []byte, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				_, message, err := conn.Read(r.Context())
				if err == nil {
					messages <- message
				}
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()

			tc := &TrackerClient{
				GetAnnounceRequest: func(event tracker.AnnounceEvent, _ [20]byte) (tracker.AnnounceRequest, error) {
					return tracker.AnnounceRequest{Event: event}, nil
				},
				OnAnnounceSuccessful: func(string) {},
				OnAnnounceError:      func(string, error) {},
				wsConn:               conn,
			}
			if err := tc.announce(test.event, [20]byte{}, nil, nil); err != nil {
				t.Fatal(err)
			}

			select {
			case message := <-messages:
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(message, &fields); err != nil {
					t.Fatal(err)
				}
				if got := string(fields["offers"]); got != "[]" {
					t.Fatalf("announce without offers must encode an empty array, got %s", got)
				}
				rawEvent, present := fields["event"]
				if test.event == tracker.None {
					if present {
						t.Fatalf("regular announce must omit event, got %s", rawEvent)
					}
				} else {
					if !present {
						t.Fatalf("missing event %q", test.want)
					}
					var event string
					if err := json.Unmarshal(rawEvent, &event); err != nil {
						t.Fatal(err)
					}
					if event != test.want {
						t.Fatalf("event = %q, want %q", event, test.want)
					}
				}
			case <-ctx.Done():
				t.Fatal("timed out waiting for tracker announce")
			}
		})
	}
}

func TestFailedWebsocketAnnounceRetries(t *testing.T) {
	var attempts atomic.Int32
	tc := &TrackerClient{
		GetAnnounceRequest: func(tracker.AnnounceEvent, [20]byte) (tracker.AnnounceRequest, error) {
			attempts.Add(1)
			return tracker.AnnounceRequest{}, errors.New("temporary failure")
		},
		RetryInterval:   func(int) time.Duration { return 40 * time.Millisecond },
		OnAnnounceError: func(string, error) {},
	}
	var infoHash [20]byte
	tc.Register(infoHash)
	if err := tc.Announce(tracker.Started, infoHash); err == nil {
		t.Fatal("expected failure")
	}
	deadline := time.Now().Add(10 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("announce was not retried")
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.CloseOffersForInfohash(infoHash)
	count := attempts.Load()
	time.Sleep(200 * time.Millisecond)
	if attempts.Load() != count {
		t.Fatalf("announced after unregister: before %d, after %d", count, attempts.Load())
	}
}

func TestScheduledWebsocketAnnounce(t *testing.T) {
	received := make(chan map[string]any, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, message, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var request map[string]any
			if json.Unmarshal(message, &request) != nil {
				return
			}
			event, _ := request["event"].(string)
			select {
			case received <- request:
			default:
			}
			interval := 1
			if event == "started" {
				interval = 120
			}
			response, _ := json.Marshal(map[string]any{
				"action": "announce", "info_hash": request["info_hash"], "interval": interval,
			})
			if conn.Write(r.Context(), websocket.MessageText, response) != nil {
				return
			}
		}
	}))
	defer server.Close()
	tc := &TrackerClient{
		Url: "ws" + strings.TrimPrefix(server.URL, "http"),
		GetAnnounceRequest: func(event tracker.AnnounceEvent, _ [20]byte) (tracker.AnnounceRequest, error) {
			return tracker.AnnounceRequest{Event: event}, nil
		},
		OnConnected:          func(error) {},
		OnDisconnected:       func(error) {},
		OnAnnounceSuccessful: func(string) {},
		OnAnnounceError:      func(string, error) {},
	}
	var infoHash [20]byte
	tc.Register(infoHash)
	tc.Start(func(error) {})
	defer tc.Close()
	if err := tc.Announce(tracker.Started, infoHash); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-received:
		if request["event"] != "started" {
			t.Fatalf("first event %q", request["event"])
		}
		offers, ok := request["offers"].([]any)
		if !ok || len(offers) != 1 {
			t.Fatalf("initial announce did not include an offer: %v", request)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("missing initial announce")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		tc.mu.Lock()
		interval := tc.announces[infoHash].interval
		tc.mu.Unlock()
		if interval == 120*time.Second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tracker response interval was not applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.mu.Lock()
	tc.scheduleAnnounce(infoHash, 20*time.Millisecond)
	tc.mu.Unlock()
	select {
	case request := <-received:
		if _, present := request["event"]; present {
			t.Fatalf("periodic announce must omit event: %v", request)
		}
		if request["numwant"] != float64(0) {
			t.Fatalf("periodic announce must not generate offers: %v", request)
		}
		if offers, ok := request["offers"].([]any); !ok || len(offers) != 0 {
			t.Fatalf("periodic announce must contain an empty offers array: %v", request)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("missing periodic announce")
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		tc.mu.Lock()
		interval := tc.announces[infoHash].interval
		tc.mu.Unlock()
		if interval == minimumAnnounceInterval {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tracker interval was not clamped")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRemovedTorrentCancelsWaitingAnnounce(t *testing.T) {
	var tc TrackerClient
	tc.cond.L = &tc.mu
	ready := make(chan struct{})
	tc.GetAnnounceRequest = func(event tracker.AnnounceEvent, _ [20]byte) (tracker.AnnounceRequest, error) {
		close(ready)
		return tracker.AnnounceRequest{Event: event}, nil
	}
	tc.OnAnnounceError = func(string, error) {}
	var infoHash [20]byte
	tc.Register(infoHash)
	tc.mu.Lock()
	original := tc.announces[infoHash]
	tc.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		result <- tc.announce(tracker.Started, infoHash, nil, original)
	}()
	<-ready
	tc.CloseOffersForInfohash(infoHash)
	tc.Register(infoHash) // The old announce must not be sent for the new registration.
	defer tc.CloseOffersForInfohash(infoHash)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("announce sent after its registration was removed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting announce did not return after torrent removal")
	}
}

func TestWebsocketReconnectReannouncesRegisteredTorrent(t *testing.T) {
	type trackerRequest struct {
		connection int
		fields     map[string]any
	}
	received := make(chan trackerRequest, 8)
	var connectionCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		connection := int(connectionCount.Add(1))
		firstConnectionRequests := 0
		for {
			_, message, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var fields map[string]any
			if err := json.Unmarshal(message, &fields); err != nil {
				t.Error(err)
				return
			}
			received <- trackerRequest{connection, fields}
			if connection == 1 {
				firstConnectionRequests++
				if firstConnectionRequests == 2 {
					return // Simulate losing the tracker connection.
				}
				continue
			}
			response, _ := json.Marshal(map[string]any{
				"action": "announce", "info_hash": fields["info_hash"], "interval": 121,
			})
			if err := conn.Write(r.Context(), websocket.MessageText, response); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	tc := &TrackerClient{
		Url: "ws" + strings.TrimPrefix(server.URL, "http"),
		GetAnnounceRequest: func(event tracker.AnnounceEvent, _ [20]byte) (tracker.AnnounceRequest, error) {
			return tracker.AnnounceRequest{Event: event}, nil
		},
		OnConnected:          func(error) {},
		OnDisconnected:       func(error) {},
		OnAnnounceSuccessful: func(string) {},
		OnAnnounceError:      func(string, error) {},
	}
	tc.cond.L = &tc.mu
	tc.pingTicker = time.NewTicker(time.Hour)
	defer tc.Close()

	var firstHash, removedHash [20]byte
	copy(firstHash[:], "first-registered-hash")
	copy(removedHash[:], "removed-torrent-hash")
	tc.Register(firstHash)
	tc.Register(removedHash)
	go tc.Announce(tracker.Started, firstHash)
	go tc.Announce(tracker.Started, removedHash)

	readRequest := func() trackerRequest {
		select {
		case request := <-received:
			return request
		case <-time.After(15 * time.Second):
			t.Fatal("timed out waiting for websocket tracker announce")
			return trackerRequest{}
		}
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- tc.doWebsocket() }()
	seen := make(map[string]bool)
	for i := 0; i < 2; i++ {
		request := readRequest()
		if request.connection != 1 || request.fields["event"] != "started" {
			t.Fatalf("unexpected first-connection request: %+v", request)
		}
		if offers, ok := request.fields["offers"].([]any); !ok || len(offers) != 1 {
			t.Fatalf("initial announce must contain one offer: %+v", request)
		}
		hash, _ := request.fields["info_hash"].(string)
		seen[hash] = true
	}
	if !seen[binaryToJsonString(firstHash[:])] || !seen[binaryToJsonString(removedHash[:])] {
		t.Fatalf("did not announce both torrents: %v", seen)
	}
	select {
	case <-firstDone:
	case <-time.After(10 * time.Second):
		t.Fatal("first websocket did not close")
	}

	// Only the remaining torrent should be reannounced after reconnect.
	tc.CloseOffersForInfohash(removedHash)
	secondDone := make(chan error, 1)
	go func() { secondDone <- tc.doWebsocket() }()
	request := readRequest()
	if request.connection != 2 || request.fields["event"] != "started" ||
		request.fields["info_hash"] != binaryToJsonString(firstHash[:]) {
		t.Fatalf("unexpected reconnect announce: %+v", request)
	}
	if offers, ok := request.fields["offers"].([]any); !ok || len(offers) != 1 {
		t.Fatalf("reconnect announce must contain one offer: %+v", request)
	}

	// Wait until the response interval has been processed before advancing its timer.
	deadline := time.Now().Add(5 * time.Second)
	for {
		tc.mu.Lock()
		interval := tc.announces[firstHash].interval
		tc.mu.Unlock()
		if interval == 121*time.Second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reconnect tracker response interval was not applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.mu.Lock()
	tc.scheduleAnnounce(firstHash, 20*time.Millisecond)
	tc.mu.Unlock()

	request = readRequest()
	if request.connection != 2 || request.fields["info_hash"] != binaryToJsonString(firstHash[:]) {
		t.Fatalf("unexpected scheduled announce after reconnect: %+v", request)
	}
	if _, present := request.fields["event"]; present {
		t.Fatalf("periodic announce after reconnect must omit event: %+v", request)
	}
	if request.fields["numwant"] != float64(0) {
		t.Fatalf("periodic announce after reconnect must not request offers: %+v", request)
	}
	if offers, ok := request.fields["offers"].([]any); !ok || len(offers) != 0 {
		t.Fatalf("periodic announce after reconnect must contain empty offers: %+v", request)
	}
	select {
	case extra := <-received:
		t.Fatalf("unexpected extra announce after reconnect: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}

	tc.Close()
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second websocket did not close")
	}
}

func TestScheduledRefreshDoesNotQueueAnotherOffer(t *testing.T) {
	var tc TrackerClient
	var infoHash [20]byte
	tc.Register(infoHash)
	tc.mu.Lock()
	tc.announces[infoHash].inFlight = true
	tc.mu.Unlock()

	if err := tc.announceWithOffer(tracker.None, infoHash, false); err != nil {
		t.Fatal(err)
	}
	tc.mu.Lock()
	pending := tc.announces[infoHash].pending
	tc.mu.Unlock()
	if pending {
		t.Fatal("periodic refresh queued an unnecessary offer")
	}
}

func TestWebsocketTrackerFailureResponse(t *testing.T) {
	var infoHash [20]byte
	infoHash[0] = 0x80
	infoHash[19] = 0xff
	const reason = "announce: offers field is not an array"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		message, err := json.Marshal(AnnounceResponse{
			Action:        "announce",
			InfoHash:      binaryToJsonString(infoHash[:]),
			FailureReason: reason,
		})
		if err != nil {
			t.Error(err)
			return
		}
		if err := conn.Write(r.Context(), websocket.MessageText, message); err != nil {
			t.Error(err)
			return
		}
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	type trackerError struct {
		infoHash string
		err      error
	}
	reported := make(chan trackerError, 1)
	tc := &TrackerClient{
		OnAnnounceError: func(ih string, err error) {
			reported <- trackerError{infoHash: ih, err: err}
		},
	}
	go tc.trackerReadLoop(conn)

	select {
	case failure := <-reported:
		if want := fmt.Sprintf("%x", infoHash); failure.infoHash != want {
			t.Fatalf("reported info hash %q, want %q", failure.infoHash, want)
		}
		if !strings.Contains(failure.err.Error(), reason) {
			t.Fatalf("missing tracker failure reason: %v", failure.err)
		}
	case <-ctx.Done():
		t.Fatal("tracker failure was not reported")
	}
}
