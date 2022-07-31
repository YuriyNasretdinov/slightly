package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
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
	Leader            int
	LeaderAlive       bool
	IsLeader          bool
	IsElectionRunning bool

	Messages []string
}

var server = flag.Bool("server", false, "Run as a server")
var port = flag.Int("port", 80, "Port to listen on")
var nodePortsArg = flag.String("nodePorts", "80,8081,8082", "List of nodes in the cluster")

var nodePorts []int

func main() {
	rand.Seed(time.Now().UnixNano())
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
			leader:   0,
			isLeader: false,
		}

		go h.replicationLoop()

		http.Handle("/", http.HandlerFunc(h.List))
		http.Handle("/append", http.HandlerFunc(h.Append))

		// replication endpoints
		http.Handle("/replication/read", http.HandlerFunc(h.Read))

		// leader election endpoints
		http.Handle("/leaderelection/leaderAlive", http.HandlerFunc(h.LeaderAlive))
		http.Handle("/leaderelection/proposeLeader", http.HandlerFunc(h.ProposeLeader))
		http.Handle("/leaderelection/finaliseLeader", http.HandlerFunc(h.FinaliseLeader))

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

	isElectionRunning          bool
	isLeaderPropositionRunning bool
	electionCooldown           time.Time
}

func (h *webHandler) replicationLoop() {
	for {
		if h.getIsLeader() {
			leaderPort, shouldTriggerReelection := h.getLeaderState()
			if shouldTriggerReelection {
				h.triggerReelection()
			} else if leaderPort != 0 && leaderPort != *port {
				log.Printf("Stepping down as a leader to majority leader %d", leaderPort)

				h.Lock()
				h.leader = leaderPort
				h.isLeader = false
				h.Unlock()
			}
		}

		if err := h.replicationStep(); err != nil {
			log.Printf("failed replication step: %v", err)
			h.setLeaderAlive(false)
			log.Printf("Talking about re-elections")
			leaderPort, shouldTriggerReelection := h.getLeaderState()
			if shouldTriggerReelection {
				h.triggerReelection()
			} else if leaderPort != 0 {
				log.Printf("Changing leader to majority leader %d", leaderPort)

				h.Lock()
				h.leader = leaderPort
				h.Unlock()
			}
		} else {
			h.setLeaderAlive(true)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (h *webHandler) getLeaderState() (leaderPort int, shouldTrigger bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	type response struct {
		leaderPort int
		alive      bool
		err        error
	}

	respCh := make(chan response, len(nodePorts))

	for _, p := range nodePorts {
		go func(p int) {
			leaderPort, alive, err := leaderAlive(ctx, p)
			respCh <- response{leaderPort: leaderPort, alive: alive, err: err}
		}(p)
	}

	notAliveCnt := 0
	majority := len(nodePorts)/2 + 1
	leaderPorts := make(map[int]int)

	for i := 0; i < len(nodePorts); i++ {
		select {
		case resp := <-respCh:
			if resp.err == nil {
				if !resp.alive {
					notAliveCnt++
				}

				if resp.leaderPort != 0 {
					leaderPorts[resp.leaderPort]++
				}
			}

		case <-ctx.Done():
			return 0, false
		}

		if notAliveCnt >= majority {
			return 0, true
		}
	}

	for leaderPort, nodesCnt := range leaderPorts {
		if nodesCnt >= majority {
			return leaderPort, false
		}
	}

	// there is no majority for the leader
	return 0, true
}

func (h *webHandler) triggerReelection() {
	h.setIsElectionRunning(true)
	log.Printf("Starting election")

	defer log.Printf("Election ended")
	defer h.setIsElectionRunning(false)
	defer h.setIsLeaderPropositionRunning(false)

	firstSleep := true

	for {
		h.setIsLeaderPropositionRunning(false)

		log.Printf("Doing random sleep")

		if !firstSleep || firstSleep && !h.getIsLeader() {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		}

		firstSleep = false

		if !h.getIsElectionRunning() {
			return
		}

		if time.Now().Before(h.getElectionCooldown()) {
			log.Printf("Election is on cooldown")
			continue
		}

		log.Printf("Proposing ourselves as a leader candidate")
		h.setIsLeaderPropositionRunning(true)

		num := proposeOurselvesAsLeaderCandidate()

		if num >= len(nodePorts)/2+1 {
			log.Printf("Finalising ourselves as a leader")
			finaliseOurselvesAsLeader()
			return
		}
	}
}

func finaliseOurselvesAsLeader() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup

	for _, p := range nodePorts {
		wg.Add(1)
		go func(p int) {
			if err := finaliseOurselvesAsLeaderOnPort(ctx, p); err != nil {
				log.Printf("Failed to finalise ourselves as leader on port %d: %v", p, err)
			}
			wg.Done()
		}(p)
	}

	wg.Wait()
}

func proposeOurselvesAsLeaderCandidate() (successNodes int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, len(nodePorts))

	for _, p := range nodePorts {
		go func(p int) {
			err := proposeOurselvesAsLeaderOnPort(ctx, p)
			if err == nil {
				errCh <- nil
			} else {
				errCh <- fmt.Errorf("error proposing ourselves as a leader on node port %d: %v", p, err)
			}
		}(p)
	}

	for i := 0; i < len(nodePorts); i++ {
		err := <-errCh
		if err != nil {
			log.Printf("Election error: %v", err)
		} else {
			successNodes++
		}
	}

	return successNodes
}

func finaliseOurselvesAsLeaderOnPort(ctx context.Context, nodePort int) error {
	u := url.Values{}
	u.Set("port", fmt.Sprint(*port))

	url := fmt.Sprintf("http://localhost:%d/leaderelection/finaliseLeader?"+u.Encode(), nodePort)

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

	if bytes.Equal(respBody, []byte("true")) {
		return nil
	}

	return fmt.Errorf("server responded with %s and status code %d", respBody, resp.StatusCode)
}

func proposeOurselvesAsLeaderOnPort(ctx context.Context, nodePort int) error {
	u := url.Values{}
	u.Set("port", fmt.Sprint(*port))

	url := fmt.Sprintf("http://localhost:%d/leaderelection/proposeLeader?"+u.Encode(), nodePort)

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

	if bytes.Equal(respBody, []byte("true")) {
		return nil
	}

	return fmt.Errorf("server responded with %s and status code %d", respBody, resp.StatusCode)
}

func leaderAlive(ctx context.Context, port int) (int, bool, error) {
	url := fmt.Sprintf("http://localhost:%d/leaderelection/leaderAlive", port)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, false, fmt.Errorf("making http request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("executing http request: %v", err)
	}

	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	leaderPortStr, aliveStr, found := strings.Cut(string(respBody), "/")
	if !found {
		return 0, false, fmt.Errorf("response body: %s", respBody)
	}

	leaderPort, err := strconv.Atoi(leaderPortStr)
	if err == nil {
		return leaderPort, aliveStr == "true", nil
	}

	return 0, false, fmt.Errorf("response body: %s", respBody)
}

func (h *webHandler) setLeaderAlive(v bool) {
	h.Lock()
	defer h.Unlock()

	h.leaderAlive = v
}

func (h *webHandler) setIsElectionRunning(v bool) {
	h.Lock()
	defer h.Unlock()

	h.isElectionRunning = v
}

func (h *webHandler) getIsElectionRunning() bool {
	h.Lock()
	defer h.Unlock()

	return h.isElectionRunning
}

func (h *webHandler) setIsLeaderPropositionRunning(v bool) {
	h.Lock()
	defer h.Unlock()

	h.isLeaderPropositionRunning = v
}

func (h *webHandler) getElectionCooldown() time.Time {
	h.Lock()
	defer h.Unlock()

	return h.electionCooldown
}

func (h *webHandler) getLeaderPort() int {
	h.Lock()
	defer h.Unlock()

	return h.leader
}

func (h *webHandler) getIsLeader() bool {
	h.Lock()
	defer h.Unlock()

	return h.isLeader
}

func (h *webHandler) getStartIndex() int {
	h.Lock()
	defer h.Unlock()

	return len(h.data)
}

func (h *webHandler) replicationStep() error {
	leaderPort := h.getLeaderPort()

	if leaderPort == *port {
		return nil
	} else if leaderPort == 0 {
		return fmt.Errorf("need to elect a leader (port = 0)")
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
		Leader:            h.leader,
		IsLeader:          h.isLeader,
		LeaderAlive:       h.leaderAlive,
		Messages:          h.data,
		IsElectionRunning: h.isElectionRunning,
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

	fmt.Fprintf(w, "%d/%v", h.leader, h.leaderAlive)
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

func (h *webHandler) ProposeLeader(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	req.ParseForm()
	portParam := req.Form.Get("port")
	if portParam == "" {
		http.Error(w, "Port must not empty", http.StatusBadRequest)
		return
	}

	proposedPort, err := strconv.Atoi(portParam)
	if err != nil || proposedPort < 0 {
		http.Error(w, "Port must be a non-negative integer", http.StatusBadRequest)
		return
	}

	if !h.isElectionRunning {
		http.Error(w, "Currently not in leader election", http.StatusPreconditionFailed)
		return
	}

	if h.isLeaderPropositionRunning && proposedPort != *port {
		http.Error(w, "Currently proposing ourselves as a leader", http.StatusPreconditionFailed)
		return
	}

	if proposedPort != *port {
		h.electionCooldown = time.Now().Add(10 * time.Second)
	}

	w.Write([]byte("true"))
}

func (h *webHandler) FinaliseLeader(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	req.ParseForm()
	portParam := req.Form.Get("port")
	if portParam == "" {
		http.Error(w, "Port must not empty", http.StatusBadRequest)
		return
	}

	proposedPort, err := strconv.Atoi(portParam)
	if err != nil || proposedPort < 0 {
		http.Error(w, "Port must be a non-negative integer", http.StatusBadRequest)
		return
	}

	if !h.isElectionRunning {
		http.Error(w, "Currently not in leader election", http.StatusPreconditionFailed)
		return
	}

	if h.isLeaderPropositionRunning && proposedPort != *port {
		http.Error(w, "Currently proposing ourselves as a leader", http.StatusPreconditionFailed)
		return
	}

	h.isElectionRunning = false
	h.leader = proposedPort
	h.isLeader = (h.leader == *port)
	h.leaderAlive = true

	w.Write([]byte("true"))
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
