// https://www.youtube.com/watch?v=icbFEmh7Ym0

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var wg = sync.WaitGroup{}

func main() { // main goroutine
	var msg = "Init"
	wg.Add(2)
	go sayHello()

	go func() {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
		fmt.Println("Output shared msg: " + msg) // example of a potential race condition
		// either "Changed" or "Changed by a goroutine" will be print!
		wg.Done()
	}()
	go func() {
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		fmt.Println("Changing msg on a goroutine...")
		msg = "Changed by a goroutine!!" // example of a potential race condition
		wg.Done()
	}()

	// msg = "Changed by main!"
	// time.Sleep(100 * time.Millisecond)  // a bad way to wait for main to finish

	wg.Wait()
}

func sayHello() {
	fmt.Println("Hello")
}

/*
Use command to check race condition!

go run -race main.go

Hello
==================
WARNING: DATA RACE
Read at 0x00c0000aa030 by goroutine 7:
  main.main.func1()
      /Users/fuyang/Workspace/6.824/src/basic-goroutines/example1/main.go:21 +0x7b

Previous write at 0x00c0000aa030 by main goroutine:
  main.main()
      /Users/fuyang/Workspace/6.824/src/basic-goroutines/example1/main.go:32 +0xf8

Goroutine 7 (running) created at:
  main.main()
      /Users/fuyang/Workspace/6.824/src/basic-goroutines/example1/main.go:19 +0xc8
==================
Output shared msg: Changed by main!
Changing msg on a goroutine...
Found 1 data race(s)
exit status 66

*/
