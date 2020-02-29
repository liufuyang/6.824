// https://www.youtube.com/watch?v=SgifAwaxMQU

package main

import (
	"fmt"
	"sync"
	"time"
)

var wg = sync.WaitGroup{}

func main() {
	ch := make(chan int, 50) // here we use buffered channel

	wg.Add(3)

	go func() {
		for i := range ch { // receive from channel
			fmt.Println(i)
		}
		// the same as the below loop:
		// for {
		// 	if i, ok := <-ch; ok {
		// 		fmt.Println(i)
		// 	} else {
		// 		break
		// 	}
		// }
		wg.Done()
	}()
	go func() {
		time.Sleep(100 * time.Millisecond)
		i := 42
		ch <- i // send a copyinto channel
		i = 24

		ch <- 36
		ch <- 86

		close(ch)
		wg.Done()
	}()
	go func() {
		time.Sleep(100 * time.Millisecond)
		// ch <- 44
		// This will cause "panic: send on closed channel" sometimes!!
		wg.Done()
	}()
	wg.Wait()
}
