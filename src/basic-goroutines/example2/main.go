package main

import (
	"fmt"
	"runtime"
	"sync"
)

var wg = sync.WaitGroup{}
var counter = 0

func main() {
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go sayHello()
		go increment()
	}
	wg.Wait()
	fmt.Printf("Final counter: #%v\n", counter)
	fmt.Printf("Threads: %v\n", runtime.GOMAXPROCS(-1))
}

func sayHello() {
	fmt.Printf("Hello #%v\n", counter)
	wg.Done()
}

func increment() {
	counter++
	wg.Done()
}

// output:
/*
Hello #10
Hello #1
Hello #5
Hello #6
Hello #7
Hello #1
Hello #4
Hello #7
Hello #1
Hello #3
Final counter: #10
*/
