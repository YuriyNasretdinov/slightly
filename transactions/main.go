package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var server = flag.Bool("server", false, "Run as a server")
var port = flag.Int("port", 80, "Port to listen on")

var errTransactionAlreadyStarted = errors.New("transaction already started")
var errMustBeInTransaction = errors.New("must be in a transaction")

func main() {
	flag.Parse()

	if *server {
		h := &webHandler{
			data: make(map[string]string),
		}

		http.Handle("/", http.HandlerFunc(h.List))
		http.Handle("/begin", http.HandlerFunc(h.Begin))
		http.Handle("/set", http.HandlerFunc(h.Set))
		http.Handle("/commit", http.HandlerFunc(h.Commit))
		http.Handle("/rollback", http.HandlerFunc(h.Rollback))
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
	}

	if err := client(); err != nil {
		log.Fatalf("Error happened: %v", err)
	}
}

func client() error {
	addr1 := "http://localhost"
	addr2 := "http://localhost:8080"

	key := "hello"
	value := "comrads"

	defer rollback(addr1)
	defer rollback(addr2)

	if err := begin(addr1); err != nil {
		return fmt.Errorf("calling begin on server1: %v", err)
	}

	if err := begin(addr2); err != nil {
		return fmt.Errorf("calling begin on server2: %v", err)
	}

	if err := set(addr1, key, value); err != nil {
		return fmt.Errorf("calling set on server1: %v", err)
	}

	if err := set(addr2, key, value); err != nil {
		return fmt.Errorf("calling set on server2: %v", err)
	}

	log.Printf("Waiting 30s before commit")
	time.Sleep(30 * time.Second)

	if err := set(addr1, "test", "test"); err != nil {
		return fmt.Errorf("calling test set on server1: %v", err)
	}

	if err := set(addr2, "test", "test"); err != nil {
		return fmt.Errorf("calling test set on server2: %v", err)
	}

	if err := commit(addr1); err != nil {
		return fmt.Errorf("calling begin on server1: %v", err)
	}

	if err := commit(addr2); err != nil {
		return fmt.Errorf("calling begin on server2: %v", err)
	}

	return nil
}

func begin(addr string) error {
	log.Printf("Begin %q", addr)

	resp, err := http.Get(addr + "/begin")
	if err != nil {
		return fmt.Errorf("making http query: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(respBody)), errTransactionAlreadyStarted)
	} else if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected response: status code %d, contents: %v", resp.StatusCode, string(respBody))
}

func commit(addr string) error {
	log.Printf("Commit %q", addr)

	resp, err := http.Get(addr + "/commit")
	if err != nil {
		return fmt.Errorf("making http query: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusBadRequest {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(respBody)), errMustBeInTransaction)
	} else if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected response: status code %d, contents: %v", resp.StatusCode, string(respBody))
}

func rollback(addr string) error {
	log.Printf("Rollback %q", addr)

	resp, err := http.Get(addr + "/rollback")
	if err != nil {
		return fmt.Errorf("making http query: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusBadRequest {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(respBody)), errMustBeInTransaction)
	} else if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected response: status code %d, contents: %v", resp.StatusCode, string(respBody))
}

func set(addr string, key string, value string) error {
	log.Printf("Set %q = %q on host %q", key, value, addr)

	u := url.Values{
		"key":   []string{key},
		"value": []string{value},
	}

	resp, err := http.Get(addr + "/set?" + u.Encode())
	if err != nil {
		return fmt.Errorf("making http query: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotAcceptable {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(respBody)), errMustBeInTransaction)
	} else if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected response: status code %d, contents: %v", resp.StatusCode, string(respBody))
}

type webHandler struct {
	sync.RWMutex

	// our "permanent" state
	data map[string]string

	// our in-transaction state
	inTransaction bool
	newData       map[string]string
}

func (h *webHandler) Begin(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	log.Printf("BEGIN")

	if h.inTransaction {
		http.Error(w, "Already in transaction", http.StatusConflict)
		return
	}

	h.inTransaction = true
	h.newData = make(map[string]string)
	w.Write([]byte("Transaction started"))
}

func (h *webHandler) Set(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	req.ParseForm()
	key := req.Form.Get("key")
	value := req.Form.Get("value")

	log.Printf("SET %q = %q", key, value)

	if !h.inTransaction {
		http.Error(w, "Must be in a transaction", http.StatusNotAcceptable)
		return
	}

	if key == "" {
		http.Error(w, "Must specify a key", http.StatusBadRequest)
		return
	}

	h.newData[key] = value
	w.Write([]byte("SET OK"))
}

func (h *webHandler) Commit(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	log.Printf("COMMIT")

	if !h.inTransaction {
		http.Error(w, "Must be in a transaction", http.StatusNotAcceptable)
		return
	}

	for key, value := range h.newData {
		h.data[key] = value
	}

	h.inTransaction = false
	w.Write([]byte("COMMIT SUCCESSFUL"))
}

func (h *webHandler) Rollback(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	log.Printf("ROLLBACK")

	if !h.inTransaction {
		http.Error(w, "Must be in a transaction", http.StatusNotAcceptable)
		return
	}

	h.inTransaction = false
	w.Write([]byte("ROLLBACK SUCCESSFUL"))
}

func (h *webHandler) List(w http.ResponseWriter, req *http.Request) {
	h.Lock()
	defer h.Unlock()

	for key, value := range h.data {
		w.Write([]byte(key + "=" + value + "\n"))
	}
}
