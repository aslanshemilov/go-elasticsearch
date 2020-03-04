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
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"

	"github.com/elastic/go-elasticsearch/v8/_examples/bulk/kafka/consumer"
	"github.com/elastic/go-elasticsearch/v8/_examples/bulk/kafka/producer"
)

var (
	brokerURL string

	topicName  = "stocks"
	topicParts = 4
	msgRate    = 100 // Messages per second

	indexName    = "stocks"
	numConsumers = 4
	flushBytes   = 0 // Default
	numWorkers   = 0 // Default
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
		indexers  []esutil.BulkIndexer
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

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		RetryOnStatus: []int{502, 503, 504, 429}, // Add 429 to the list of retryable statuses
		RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		MaxRetries:    5,
	})
	if err != nil {
		log.Fatalf("Error: NewClient(): %s", err)
	}

	idx, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      indexName,
		Client:     es,
		NumWorkers: numWorkers,
		FlushBytes: int(flushBytes),
	})
	if err != nil {
		log.Fatalf("ERROR: NewBulkIndexer(): %s", err)
	}
	indexers = append(indexers, idx)

	for i := 1; i <= numConsumers; i++ {
		consumers = append(consumers,
			&consumer.Consumer{
				BrokerURL: brokerURL,
				TopicName: topicName,
				Indexer:   indexers[0]})
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		fmt.Println("Initializing...")
		for {
			select {
			case <-ticker.C:
				// for i, p := range producers {
				// 	fmt.Printf("\033[%d;0H [Producer %d]   ", i, i+1)
				// 	p.Report()
				// }
				// for i, c := range consumers {
				// 	fmt.Printf("\033[%d;0H [Consumer %d]   ", len(producers)+i+1, i+1)
				// 	c.Report()
				// }
				fmt.Print(report(producers, consumers, indexers))
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

	fmt.Print(report(producers, consumers, indexers))
}

func report(
	producers []*producer.Producer,
	consumers []*consumer.Consumer,
	indexers []esutil.BulkIndexer,
) string {
	var b strings.Builder

	fmt.Print("\033[2J\033[K")
	fmt.Print("\033[0;0H")

	for i, p := range producers {
		fmt.Fprintf(&b, "\033[%d;0H [Producer %d]   ", i+1, i+1)
		s := p.Stats()
		fmt.Fprintf(&b,
			"duration=%-*s |   msg/sec=%-*s  |   sent=%-*s    |   bytes=%-*s |   errors=%-*s",
			10, s.Duration.Truncate(time.Second),
			10, humanize.FtoaWithDigits(s.Throughput, 0),
			10, humanize.Comma(int64(s.TotalMessages)),
			10, humanize.Bytes(uint64(s.TotalBytes)),
			10, humanize.Comma(int64(s.TotalErrors)))
	}

	for i, c := range consumers {
		fmt.Fprintf(&b, "\033[%d;0H [Consumer %d]   ", len(producers)+i+1, i+1)
		s := c.Stats()
		fmt.Fprintf(&b,
			"lagging=%-*s  |   received=%-*s |   errors=%-*s",
			10, humanize.Comma(s.TotalLag),
			10, humanize.Comma(s.TotalMessages),
			10, humanize.Comma(s.TotalErrors))
	}

	for i, x := range indexers {
		fmt.Fprintf(&b, "\033[%d;0H [Indexer  %d]   ", len(producers)+len(consumers)+i+1, i+1)
		s := x.Stats()
		fmt.Fprintf(&b,
			"added=%-*s    |   flushed=%-*s  |   failed=%-*s",
			10, humanize.Comma(int64(s.NumAdded)),
			10, humanize.Comma(int64(s.NumFlushed)),
			0, humanize.Comma(int64(s.NumFailed)))
	}

	return b.String()
}
