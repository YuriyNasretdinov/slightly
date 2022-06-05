package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var server = flag.Bool("server", false, "Run as a server")
var name = flag.String("name", "hello_comrads", "Name of the lock")

var errLocked = errors.New("lock is already active")

func main() {
	flag.Parse()

	if *server {
		h := &webHandler{
			activeLocks: make(map[string]lockInfo),
		}

		http.Handle("/", http.HandlerFunc(h.ListLocks))
		http.Handle("/acquire-lock", http.HandlerFunc(h.AcquireLock))
		http.Handle("/release-lock", http.HandlerFunc(h.ReleaseLock))
		log.Fatal(http.ListenAndServe(":80", nil))
	}

	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("%s:%d", hostname, os.Getpid())

	if err := acquireLock(*name, owner, 30*time.Second); err != nil {
		log.Fatal(err)
	}
	defer releaseLock(*name, owner)

	ch := make(chan os.Signal, 5)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-ch
		releaseLock(*name, owner)
		os.Exit(0)
	}()

	log.Printf("Sleeping for one minute")
	time.Sleep(time.Minute)
}

func acquireLock(name string, owner string, timeout time.Duration) error {
	go func() {
		time.Sleep(timeout - time.Second)
		log.Printf("Timeout expired for lock %q (%s)", name, timeout)
		os.Exit(2)
	}()

	log.Printf("Acquiring lock %q with owner %q and timeout %s", name, owner, timeout)

	u := url.Values{
		"name":    []string{name},
		"owner":   []string{owner},
		"timeout": []string{strconv.Itoa(int(timeout / time.Second))},
	}

	resp, err := http.Get("http://localhost/acquire-lock?" + u.Encode())
	if err != nil {
		return fmt.Errorf("making http query: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(respBody)), errLocked)
	} else if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected response: status code %d, contents: %v", resp.StatusCode, string(respBody))
}

func releaseLock(name string, owner string) error {
	log.Printf("Releasing lock %q with owner %q", name, owner)

	u := url.Values{
		"name":  []string{name},
		"owner": []string{owner},
	}

	resp, err := http.Get("http://localhost/release-lock?" + u.Encode())
	if err != nil {
		return fmt.Errorf("making http query: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected response: status code %d, contents: %v", resp.StatusCode, string(respBody))
}

type lockInfo struct {
	owner string
	end   time.Time
}

type webHandler struct {
	sync.RWMutex
	activeLocks map[string]lockInfo
}

func (h *webHandler) AcquireLock(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	name := req.Form.Get("name")
	if name == "" {
		http.Error(w, "lock name is required", http.StatusBadRequest)
		return
	}

	owner := req.Form.Get("owner")
	if owner == "" {
		http.Error(w, "lock owner field is required", http.StatusBadRequest)
		return
	}

	timeout := req.Form.Get("timeout")
	if timeout == "" {
		http.Error(w, "timeout (in seconds) is required", http.StatusBadRequest)
		return
	}

	timeoutSec, err := strconv.ParseInt(timeout, 10, 64)
	if err != nil || timeoutSec <= 0 {
		http.Error(w, "timeout must be a positive integer", http.StatusBadRequest)
		return
	}

	h.Lock()
	defer h.Unlock()

	now := time.Now()
	li, ok := h.activeLocks[name]

	// allow the owner of the lock to acquire the lock once again even if it hasn't expired yet
	if ok && li.owner != owner && li.end.After(now) {
		http.Error(w, fmt.Sprintf("lock is already active (%s left)", li.end.Sub(now)), http.StatusConflict)
		return
	}

	h.activeLocks[name] = lockInfo{
		owner: owner,
		end:   time.Now().Add(time.Duration(timeoutSec) * time.Second),
	}
	w.Write([]byte("Success"))
}

func (h *webHandler) ListLocks(w http.ResponseWriter, req *http.Request) {
	h.RLock()
	defer h.RUnlock()

	w.Write([]byte("Active locks:\n"))
	locks := make([]string, 0, len(h.activeLocks))

	for name, _ := range h.activeLocks {
		locks = append(locks, name)
	}

	sort.Strings(locks)

	for _, name := range locks {
		fmt.Fprintf(w, "%s: owned by %q, expires in %s\n", name, h.activeLocks[name].owner, h.activeLocks[name].end.Sub(time.Now()))
	}
}

func (h *webHandler) ReleaseLock(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	name := req.Form.Get("name")
	if name == "" {
		http.Error(w, "lock name is required", http.StatusBadRequest)
		return
	}

	owner := req.Form.Get("owner")
	if owner == "" {
		http.Error(w, "lock owner field is required", http.StatusBadRequest)
		return
	}

	h.Lock()
	defer h.Unlock()

	if li, ok := h.activeLocks[name]; ok && li.owner != owner {
		http.Error(w, fmt.Sprintf("lock has another owner %q", li.owner), http.StatusBadRequest)
		return
	}

	delete(h.activeLocks, name)
	w.Write([]byte("Success"))
}
