package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "embed"
)

//go:embed index.html
var indexHTML string

var indexTPL = template.Must(template.New("index.html").Parse(indexHTML))

type IndexData struct {
	Leader      int
	LeaderAlive bool
	IsLeader    bool

	Messages []string
}

var server = flag.Bool("server", false, "Run as a server")
var port = flag.Int("port", 80, "Port to listen on")
var nodePortsArg = flag.String("nodePorts", "80,8081,8082", "List of nodes in the cluster")

var nodePorts []int

const ourGloriousLeader = 80

func main() {
	flag.Parse()

	for _, p := range strings.Split(*nodePortsArg, ",") {
		port, err := strconv.Atoi(p)
		if err != nil {
			log.Fatalf("Failed to parse port %q: %v", p, err)
		}

		nodePorts = append(nodePorts, port)
	}

	log.SetPrefix(fmt.Sprintf("[port=%d] ", *port))
	log.Printf("Listening on port %d", *port)

	if *server {
		h := &webHandler{
			leader:   ourGloriousLeader,
			isLeader: (ourGloriousLeader == *port),
		}

		go h.replicationLoop()

		http.Handle("/", http.HandlerFunc(h.List))
		http.Handle("/append", http.HandlerFunc(h.Append))

		// replication endpoints
		http.Handle("/replication/read", http.HandlerFunc(h.Read))

		// leader election endpoints
		http.Handle("/leaderelection/leaderAlive", http.HandlerFunc(h.LeaderAlive))

		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
	}

	// if err := client(); err != nil {
	// log.Fatalf("Error happened: %v", err)
	// }
}

type webHandler struct {
	sync.RWMutex

	// our "permanent" state
	data []string

	// leader election state
	leader      int
	leaderAlive bool
	isLeader    bool
}

func (h *webHandler) replicationLoop() {
	for {
		if err := h.replicationStep(); err != nil {
			log.Printf("failed replication step: %v", err)
			h.setLeaderAlive(false)
			if h.shouldTriggerReelection() {
				h.triggerReelection()
			}
		} else {
			h.setLeaderAlive(true)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (h *webHandler) shouldTriggerReelection() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	log.Printf("Talking about re-elections")

	type response struct {
		alive bool
		err   error
	}

	respCh := make(chan response, len(nodePorts))

	for _, p := range nodePorts {
		go func(p int) {
			alive, err := leaderAlive(ctx, p)
			respCh <- response{alive: alive, err: err}
		}(p)
	}

	notAliveCnt := 0
	minNotAlive := len(nodePorts)/2 + 1

	for i := 0; i < len(nodePorts); i++ {
		select {
		case resp := <-respCh:
			if !resp.alive && resp.err == nil {
				notAliveCnt++
			}
		case <-ctx.Done():
			return false
		}

		if notAliveCnt >= minNotAlive {
			return true
		}
	}

	return false
}

func (h *webHandler) triggerReelection() {
	log.Printf("REELECTION!!!")
}

func leaderAlive(ctx context.Context, port int) (bool, error) {
	url := fmt.Sprintf("http://localhost:%d/leaderelection/leaderAlive", port)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false, fmt.Errorf("making http request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("executing http request: %v", err)
	}

	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	return bytes.Equal(respBody, []byte("true")), nil
}

func (h *webHandler) setLeaderAlive(v bool) {
	h.Lock()
	defer h.Unlock()

	h.leaderAlive = v
}

func (h *webHandler) getLeaderPort() int {
	h.Lock()
	defer h.Unlock()

	return h.leader
}

func (h *webHandler) getStartIndex() int {
	h.Lock()
	defer h.Unlock()

	return len(h.data)
}

func (h *webHandler) replicationStep() error {
	if h.getLeaderPort() == *port {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	url := fmt.Sprintf("http://localhost:%d/replication/read?index=%d&replicaPort=%d", h.getLeaderPort(), h.getStartIndex(), *port)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("making http request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing http request: %v", err)
	}

	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooEarly {
		return nil
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received status code %d: %s", resp.StatusCode, respBody)
	}

	h.Lock()
	defer h.Unlock()

	h.data = append(h.data, strings.Split(string(respBody), "\n")...)

	return nil
}

func (h *webHandler) List(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	indexTPL.Execute(w, &IndexData{
		Leader:      h.leader,
		IsLeader:    h.isLeader,
		LeaderAlive: h.leaderAlive,
		Messages:    h.data,
	})
}

func (h *webHandler) append(value string) error {
	h.Lock()
	defer h.Unlock()

	if !h.isLeader {
		return fmt.Errorf("please talk to leader %d", h.leader)
	}

	h.data = append(h.data, value)
	return nil
}

func (h *webHandler) LeaderAlive(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	fmt.Fprintf(w, "%v", h.leaderAlive)
}

func (h *webHandler) Read(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	req.ParseForm()
	indexParam := req.Form.Get("index")
	if indexParam == "" {
		http.Error(w, "Index must not empty", http.StatusBadRequest)
		return
	}

	if req.Form.Get("replicaPort") == "8081" {
		http.Error(w, "You're not lucky", http.StatusBadRequest)
		return
	}

	index, err := strconv.Atoi(indexParam)
	if err != nil || index < 0 {
		http.Error(w, "Index must be a non-negative integer", http.StatusBadRequest)
		return
	}

	if index >= len(h.data) {
		http.Error(w, "Index must be less than the length of the data", http.StatusTooEarly)
		return
	}

	w.Write([]byte(strings.Join(h.data[index:], "\n")))
}

func (h *webHandler) Append(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	value := req.Form.Get("value")
	if value == "" {
		http.Error(w, "Value must not empty", http.StatusBadRequest)
		return
	} else if strings.Contains(value, "\n") {
		http.Error(w, "Value must not contain \\n character", http.StatusBadRequest)
		return
	}

	log.Printf("APPEND %q", value)
	if err := h.append(value); err != nil {
		http.Error(w, "Internal error occurred: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte("APPEND OK"))
}
