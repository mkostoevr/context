package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"
	"github.com/tarantool/go-tarantool"
)

var Addr = flag.String(
	"addr", "127.0.0.1:3301",
	"address of the tarantool instance to benchmark")
var Conc = flag.Int("conc", 1000, "number of concurrent clients")
var Reqs = flag.Int("reqs", 1000, "number of requests to send per client")
var BenchFunc = flag.String("bench_func", "bench_func", "function to call")
var BenchFuncArgSize = flag.Int(
	"bench_func_arg_size", 5,
	"size of an argument passed to bench_func")
var BenchFuncArgCount = flag.Int(
	"bench_func_arg_count", 10,
	"number of arguments passed to bench_func")

func main() {
	flag.Parse()

	conn, err := tarantool.Connect(*Addr, tarantool.Opts {
		User: "guest",
	})
	if err != nil {
		log.Fatal("Connect: ", err)
	}
	defer conn.Close()

	bench_func_arg_value := ""
	for i := 0; i < *BenchFuncArgSize; i++ {
		bench_func_arg_value += "a"
	}
	bench_func_args := make([]string, *BenchFuncArgCount)
	for i := range bench_func_args {
		bench_func_args[i] = bench_func_arg_value
	}

	start_time := time.Now()

	var wg sync.WaitGroup
	wg.Add(*Conc)
	for i := 0; i < *Conc; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < *Reqs; i++ {
				_, err := conn.Call(*BenchFunc, bench_func_args)
				if err != nil {
					log.Fatal("Call: ", err)
				}
			}
		}()
	}
	wg.Wait()

	duration := float64(time.Now().Sub(start_time)) / float64(time.Second)
	fmt.Printf("REQUESTS: %d\n", *Reqs * *Conc)
	fmt.Printf("ELAPSED: %.2f\n", duration)
	fmt.Printf("RPS: %.0f\n", float64(*Reqs) * float64(*Conc) / duration)
}
