package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
)

func main() {

	srcAddr := flag.String("src", "", "Address of source redis (non-clustered): Example: redis-nonclusterdev.example.com:6379")
	dstAddr := flag.String("dst", "", "Address of destination redis (clustered): Example: redis-clusterdev.example.com:6379")
	verify := flag.Bool("verify", false, "Verify keys after migration")
	flag.Parse()

	if *srcAddr == "" || *dstAddr == "" {
		log.Fatal("src and dst addrs cannot be empty")
	}

	m, err := NewMigrator(*srcAddr, *dstAddr)
	if err != nil {
		log.Fatalf("newMigrator(%s, %s) failed: %s", srcAddr, dstAddr, err.Error())
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go m.Start(wg, *verify)

	go func() {
		sigCh := make(chan os.Signal)
		signal.Notify(sigCh, os.Interrupt)
		<-sigCh
		fmt.Println("Received interrupt. Shutting down...")
		m.Stop()
	}()

	wg.Wait()

	fmt.Printf("\nDone; successCount = %d; failureCount = %d;\n", m.successCount, m.failureCount)

	if err := m.dumpFailed("failed.keys"); err != nil {
		log.Fatalf("Failed to write failed keys to file: %s", err.Error())
	} else {
		if m.failureCount > 0 {
			fmt.Println("Failed keys written to failed.keys file")
		}
	}
}
