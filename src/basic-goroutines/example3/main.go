package main

import (
	"fmt"
	"runtime"
	"sync"
)

var wg = sync.WaitGroup{}
var counter = 0
var m = sync.RWMutex{} // Many can read, but for only one guy can access the data at the same time

func main() {
	// runtime.GOMAXPROCS(1) // making it single thread
	// runtime.GOMAXPROCS(100)
	for i := 0; i < 10; i++ {
		wg.Add(2)
		m.RLock()
		go sayHello()
		m.Lock()
		go increment()
	}
	wg.Wait()
	fmt.Printf("Final counter: #%v\n", counter)
	fmt.Printf("Threads: %v\n", runtime.GOMAXPROCS(-1))
}

func sayHello() {
	//m.RLock()
	fmt.Printf("Hello #%v\n", counter)
	m.RUnlock()
	wg.Done()
}

func increment() {
	//m.Lock()
	counter++
	m.Unlock()
	wg.Done()
}

// output:
/*
Hello #0
Hello #1
Hello #2
Hello #3
Hello #4
Hello #5
Hello #6
Hello #7
Hello #8
Hello #9
Final counter: #10

But this case has no benefit of concurrency
*/
