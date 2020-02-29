// https://www.youtube.com/watch?v=SgifAwaxMQU

package main

import (
	"fmt"
	"sync"
)

var wg = sync.WaitGroup{}

func main() {
	ch := make(chan int)

	wg.Add(3)

	go func() {
		i := <-ch // receive from channel
		fmt.Println(i)
		wg.Done()
	}()
	go func() {
		i := 42
		ch <- i // send a copyinto channel
		i = 24
		wg.Done()
	}()
	go func() {
		// ch <- 24 // send into channel again will cause Error.
		// Because after one pushes data in channel, it will wait until the data being used.
		// However our user above is already Done() and no one will pick the new data up, causing deadlock
		// See example 3 and 4 on how to solve this issue
		wg.Done()
	}()

	wg.Wait()
}
