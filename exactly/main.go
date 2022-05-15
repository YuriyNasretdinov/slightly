package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/YuriyNasretdinov/chukcha/client"
)

const dedupStorage = "/tmp/dedup"

//go:embed index.html
var indexHTML string
var indexTPL = template.Must(template.New("index.html").Parse(indexHTML))

type IndexData struct {
	Messages  []string
	RequestID int64
}

type Message struct {
	Message   string
	RequestID int64
	Timestamp time.Time
}

type webHandler struct {
	cl *client.Simple

	sync.RWMutex
	messages []Message
	dedup    map[Message]bool
}

func (h *webHandler) IndexHandler(w http.ResponseWriter, req *http.Request) {
	h.RLock()
	defer h.RUnlock()

	sort.Slice(h.messages, func(i, j int) bool {
		return h.messages[i].Timestamp.Before(h.messages[j].Timestamp)
	})

	messages := make([]string, 0, len(h.messages))
	for _, m := range h.messages {
		messages = append(messages, m.Message)
	}

	indexTPL.Execute(w, &IndexData{
		Messages:  messages,
		RequestID: rand.Int63(),
	})
}

func (h *webHandler) messageAlreadyExists(m Message) bool {
	h.RLock()
	defer h.RUnlock()

	m.Timestamp = time.Time{}
	_, ok := h.dedup[m]
	return ok
}

func (h *webHandler) PostMessage(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	if rand.Intn(3) == 0 {
		http.Error(w, "I am tired", http.StatusInternalServerError)
		return
	}

	message := strings.TrimSpace(req.Form.Get("message"))
	if message == "" {
		http.Error(w, "No message", http.StatusBadRequest)
		return
	} else if strings.Contains(message, "\n") {
		http.Error(w, "Newline not allowed", http.StatusBadRequest)
		return
	} else if len(message) >= 512*1024 {
		http.Error(w, "Message too long", http.StatusBadRequest)
		return
	}

	requestID, err := strconv.ParseInt(req.Form.Get("request_id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid request_id", http.StatusBadRequest)
		return
	}

	m := Message{
		Message:   message,
		RequestID: requestID,
		Timestamp: time.Now(),
	}

	if !h.messageAlreadyExists(m) {
		msg, _ := json.Marshal(&m)
		msg = append(msg, '\n')

		if err := h.cl.Send(req.Context(), "exactly", msg); err != nil {
			http.Error(w, fmt.Sprintf("Chukcha responded with an error: %v", err), http.StatusInternalServerError)
			return
		}

		fp, err := os.OpenFile(dedupStorage, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to open dedup storage: %v", err), http.StatusInternalServerError)
			return
		}

		defer fp.Close()

		if _, err := fp.Write(msg); err != nil {
			http.Error(w, fmt.Sprintf("Failed to write to dedup storage: %v", err), http.StatusInternalServerError)
			return
		}

		h.Lock()
		m.Timestamp = time.Time{}
		h.dedup[m] = true
		h.Unlock()
	}

	rnd := rand.Intn(3)
	if rnd == 0 {
		http.Error(w, "You were not lucky", http.StatusInternalServerError)
		return
	} else if rnd == 1 {
		time.Sleep(time.Hour)
	}

	http.Redirect(w, req, "/", http.StatusFound)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	h := &webHandler{
		cl:    client.NewSimple([]string{"http://localhost:8080", "http://localhost:8081"}),
		dedup: make(map[Message]bool),
	}

	fp, err := os.Open(dedupStorage)
	if err != nil {
		log.Fatalf("Failed to open dedup storage: %v", err)
	}
	defer fp.Close()

	h.Lock()
	d := json.NewDecoder(fp)
	for {
		var m Message
		if err := d.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("Failed to decode message: %v", err)
			continue
		}
		m.Timestamp = time.Time{}
		h.dedup[m] = true
	}
	h.Unlock()

	h.cl.SetAcknowledge(false)

	go func() {
		time.Sleep(time.Second * 10)

		scratch := make([]byte, 1024*1024)

		for {
			h.cl.Process(context.Background(), "exactly", scratch, func(b []byte) error {
				h.Lock()
				defer h.Unlock()

				var m Message
				d := json.NewDecoder(bytes.NewReader(b))
				for {
					if err := d.Decode(&m); err != nil {
						if err == io.EOF {
							return nil
						}
						return err
					}

					h.messages = append(h.messages, m)
				}
			})
		}
	}()

	http.Handle("/", http.HandlerFunc(h.IndexHandler))
	http.Handle("/post-message", http.HandlerFunc(h.PostMessage))
	log.Fatal(http.ListenAndServe(":80", nil))
}
