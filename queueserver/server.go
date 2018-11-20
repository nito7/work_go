package main

import (
	"fmt"
	"strconv"
	"net/http"
	"time"
	"net/url"
	"strings"
)

const (
	workerNum = 10
	queueSize = 100
)

var (
	logProc *LogProc
	logWorker *LogWorker
)

type LogProc struct {
	Workers [workerNum]*LogWorker
}

func NewLogProc() *LogProc {
	proc := &LogProc{}
	for i := 0; i < workerNum; i++ {
		proc.Workers[i] = NewLogWorker("Queue" + strconv.Itoa(i))
	}
	return proc
}

func (proc *LogProc) Run() {
	for i := 0; i < workerNum; i++ {
		proc.Workers[i].Run()
	}
}

func getWorker(proc *LogProc) *LogWorker {
	var worker *LogWorker
	min := queueSize
	for i := 0; i < workerNum; i++ {
		qlen := len(proc.Workers[i].Queue)
		fmt.Printf("len(%s) = %d\n", proc.Workers[i].Name, qlen)
		if qlen < min {
			worker = proc.Workers[i]
			min = len(worker.Queue)
		}
	}
	return worker
}

type LogWorker struct {
	Queue chan map[int]string
	Name  string
}

func NewLogWorker(name string) *LogWorker {
	return &LogWorker {
		Queue: make(chan map[int]string, queueSize),
		Name: name,
	}
}

func (w *LogWorker) Run() {
	go func(w *LogWorker) {
		for {
			select {
			case m := <-w.Queue:
				time.Sleep(5 * time.Second)
				fmt.Printf("UA(for %s): %s\n", w.Name, m[0])
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}(w)
}

func escape(raw string) string {
	escapeString := url.QueryEscape(raw)
	return strings.Replace(escapeString, "+", "%20", -1)
}

func handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "{}")

	str := escape(r.Header.Get("User-Agent"))
	data := make(map[int]string)
	data[0] = str

	worker := getWorker(logProc)
	if worker == nil {
		fmt.Println("no worker is free")
		return
	}
	worker.Queue <- data
	return
}

func main() {
	logProc = NewLogProc()
	logProc.Run()
	http.HandleFunc("/event", handle)
	http.ListenAndServe(":8080", nil)
}
