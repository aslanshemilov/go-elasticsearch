// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8/_examples/bulk/kafka/consumer"
	"github.com/elastic/go-elasticsearch/v8/_examples/bulk/kafka/producer"
)

var (
	brokerURL string

	topicName  = "stocks"
	topicParts = 4
	msgRate    = 100 // Messages per second

	indexName    = "stocks"
	flushBytes   = 0
	numConsumers = 4
)

func init() {
	if v := os.Getenv("KAFKA_URL"); v != "" {
		brokerURL = v
	} else {
		brokerURL = "localhost:9092"
	}
}

func main() {
	log.SetFlags(0)

	var (
		wg  sync.WaitGroup
		ctx = context.Background()

		producers []*producer.Producer
		consumers []*consumer.Consumer
	)

	done := make(chan os.Signal)
	signal.Notify(done, os.Interrupt)
	go func() { <-done; log.Println(""); os.Exit(0) }()

	producers = append(producers,
		&producer.Producer{
			BrokerURL:   brokerURL,
			TopicName:   topicName,
			TopicParts:  topicParts,
			MessageRate: msgRate})

	for i := 1; i <= numConsumers; i++ {
		consumers = append(consumers,
			&consumer.Consumer{
				BrokerURL:  brokerURL,
				TopicName:  topicName,
				IndexName:  indexName,
				FlushBytes: flushBytes})
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		fmt.Println("Initializing...")
		for {
			select {
			case <-ticker.C:
				fmt.Print("\033[2J\033[K")
				fmt.Print("\033[0;0H")
				for i, p := range producers {
					fmt.Printf("\033[%d;0H [Producer %d]   ", i, i+1)
					p.Report()
				}
				for i, c := range consumers {
					fmt.Printf("\033[%d;0H [Consumer %d]   ", len(producers)+i+1, i+1)
					c.Report()
				}
			}
		}
	}()

	if err := producers[0].CreateTopic(ctx); err != nil {
		log.Fatalf("ERROR: Producer: %s", err)
	}

	for _, c := range consumers {
		wg.Add(1)
		go func(c *consumer.Consumer) {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				log.Fatalf("ERROR: Consumer: %s", err)
			}
		}(c)
	}

	time.Sleep(5 * time.Second) // Leave some room for consumers to connect
	for _, p := range producers {
		wg.Add(1)
		go func(p *producer.Producer) {
			defer wg.Done()
			if err := p.Run(ctx); err != nil {
				log.Fatalf("ERROR: Producer: %s", err)
			}
		}(p)
	}

	wg.Wait()

	for i, p := range producers {
		fmt.Printf("[Producer %d]   ", i+1)
		p.Report()
	}
	for i, c := range consumers {
		fmt.Printf("[Consumer %d]   ", i+1)
		c.Report()
	}
}
