// https://www.youtube.com/watch?v=SgifAwaxMQU

package main

import (
	"fmt"
	"sync"
)

var wg = sync.WaitGroup{}

func main() {
	ch := make(chan int)

	wg.Add(2)

	go func(ch <-chan int) { // ch now is a receive only channel
		i := <-ch // receive from channel
		fmt.Println(i)
		// ch <- 27 // you can not do this now, as ch is receiving only channel
		wg.Done()
	}(ch)
	go func(ch chan<- int) { // ch now is a sending only channel
		i := 42
		ch <- i // send a copyinto channel
		i = 24
		wg.Done()
	}(ch)

	wg.Wait()
}
