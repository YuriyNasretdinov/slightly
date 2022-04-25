package main

import (
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	_ "embed"
)

const fileName = "/loopfs/messages"
const counterFileName = "/loopfs/counter"
const maxAtomicAppendSize = 512 // bytes, according to POSIX

//go:embed index.html
var indexHTML string

var indexTPL = template.Must(template.New("index.html").Parse(indexHTML))

type IndexData struct {
	Count    int
	Messages []string
}

type webHandler struct {
	sync.RWMutex
}

func (h *webHandler) HelloWorld(w http.ResponseWriter, req *http.Request) {
	h.RLock()
	defer h.RUnlock()

	contents, err := ioutil.ReadFile(fileName)
	if err != nil && !os.IsNotExist(err) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	count, err := h.ReadCounterValue()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	messages := strings.Split(strings.TrimSpace(string(contents)), "\n")

	indexTPL.Execute(w, &IndexData{
		Count:    count,
		Messages: messages,
	})
}

func (h *webHandler) ReadCounterValue() (count int, err error) {
	counterContents, err := ioutil.ReadFile(counterFileName)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	count, _ = strconv.Atoi(strings.TrimSpace(string(counterContents)))
	return count, nil
}

func (h *webHandler) writeMessage(message string) error {
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {

		return err
	}
	defer f.Close()

	if _, err := f.WriteString(message + "\n"); err != nil {
		return err
	}

	return nil
}

func (h *webHandler) incrementCounter() error {
	count, err := h.ReadCounterValue()
	if err != nil {
		return err
	}

	contents := fmt.Sprintf("%d\n", count+1)

	if err := ioutil.WriteFile(counterFileName+".tmp", []byte(contents), 0666); err != nil {
		return err
	}

	return os.Rename(counterFileName+".tmp", counterFileName)
}

func (h *webHandler) PostMessage(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	message := strings.TrimSpace(req.Form.Get("message"))
	if message == "" {
		http.Error(w, "No message", http.StatusBadRequest)
		return
	} else if strings.Contains(message, "\n") {
		http.Error(w, "Newline not allowed", http.StatusBadRequest)
		return
	} else if len(message)+1 > maxAtomicAppendSize {
		http.Error(w, "Message too long", http.StatusBadRequest)
		return
	}

	h.Lock()
	defer h.Unlock()

	if err := h.incrementCounter(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := h.writeMessage(message); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, req, "/", http.StatusFound)
}

func main() {
	h := &webHandler{}

	http.Handle("/", http.HandlerFunc(h.HelloWorld))
	http.Handle("/post-message", http.HandlerFunc(h.PostMessage))
	log.Fatal(http.ListenAndServe(":80", nil))
}
