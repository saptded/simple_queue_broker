package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type queueResolver struct {
	queues syncMap
	locker sync.Mutex
}

func NewQueueResolver() *queueResolver {
	return &queueResolver{queues: syncMap{internal: make(map[string]*syncQueue)}, locker: sync.Mutex{}}
}

type syncMap struct {
	internal map[string]*syncQueue
	sync.Mutex
}

func (m *syncMap) loadOrCreateNew(key string) (*syncQueue, bool) {
	defer m.Unlock()
	m.Lock()
	val, ok := m.internal[key]
	if !ok {
		val = &syncQueue{queue: make([]string, 0), syncMutex: sync.RWMutex{}, stopMutex: sync.Mutex{}}
		m.internal[key] = val
	}

	return val, ok
}

func (m *syncMap) load(key string) (*syncQueue, bool) {
	defer m.Unlock()
	m.Lock()
	val, ok := m.internal[key]

	return val, ok
}

type syncQueue struct {
	queue     []string
	stopMutex sync.Mutex
	syncMutex sync.RWMutex
}

func (q *syncQueue) push(elem string) {
	defer q.syncMutex.Unlock()
	q.syncMutex.Lock()
	q.queue = append(q.queue, elem)
}

func (q *syncQueue) pop() string {
	var elem string
	if q.queue != nil && len(q.queue) > 0 {
		q.syncMutex.RLock()
		elem = q.queue[0]
		q.queue = q.queue[1:]
		q.syncMutex.RUnlock()
	}

	return elem
}

func (q *syncQueue) popWithTimeout(timeout int) string {
	if timeout == 0 {
		return q.pop()
	}

	msg := make(chan string)
	finishChan := make(chan bool)

	go func(msg chan string, finish chan bool, queue *syncQueue) {
		defer q.stopMutex.Unlock()
		q.stopMutex.Lock()
		for elem := ""; elem == ""; {
			select {
			case <-finish:
				return
			default:
				elem = queue.pop()
				if elem != "" {
					msg <- elem
				}
			}
		}
	}(msg, finishChan, q)

	var message string
	for finish := false; !finish; {
		select {
		case message = <-msg:
			return message
		case <-time.After(time.Second * time.Duration(timeout)):
			finish = true
			finishChan <- true
		}
	}

	return message
}

func (q *queueResolver) Push(w http.ResponseWriter, r *http.Request) {
	queueTitle := r.URL.Path[1:]
	queue, _ := q.queues.loadOrCreateNew(queueTitle)

	queueValue := r.URL.Query().Get("v")
	if queueValue == "" {
		w.WriteHeader(http.StatusNotFound)
	} else {
		queue.push(queueValue)
		w.WriteHeader(http.StatusOK)
	}
}

func (q *queueResolver) Pop(w http.ResponseWriter, r *http.Request) {
	queueTitle := r.URL.Path[1:]
	queue, ok := q.queues.load(queueTitle)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var (
		timeoutInt int
		err        error
	)
	if timeout := r.URL.Query().Get("timeout"); timeout != "" {
		timeoutInt, err = strconv.Atoi(r.URL.Query().Get("timeout"))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	elem := queue.popWithTimeout(timeoutInt)
	if elem == "" {
		w.WriteHeader(http.StatusNotFound)
	}

	_, err = w.Write([]byte(elem))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

type Methods map[string]http.HandlerFunc

func RouteMethods(methods Methods) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resolver, ok := methods[r.Method]
		if !ok {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		resolver(w, r)
	})
}

func main() {
	port := flag.String("p", "8000", "server port")
	flag.Parse()

	server := http.NewServeMux()
	queue := NewQueueResolver()

	server.Handle("/", RouteMethods(Methods{
		http.MethodGet: queue.Pop,
		http.MethodPut: queue.Push,
	}))

	log.Fatal(http.ListenAndServe(":"+*port, server))
}
