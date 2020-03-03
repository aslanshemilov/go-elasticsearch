// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

package producer

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/segmentio/kafka-go"
)

var (
	sides    = []string{"BUY", "SELL"}
	symbols  = []string{"ZBZX", "ZJZZT", "ZTEST", "ZVV", "ZVZZT", "ZWZZT", "ZXZZT"}
	accounts = []string{"ABC123", "LMN456", "XYZ789"}
)

func init() {
	rand.Seed(time.Now().UnixNano())
	kafka.DefaultClientID = "go-elasticsearch-kafka-demo"
}

type Producer struct {
	BrokerURL   string
	TopicName   string
	TopicParts  int
	MessageRate int

	writer *kafka.Writer

	startTime     time.Time
	totalMessages int64
	totalErrors   int64
	totalBytes    int64
}

func (p *Producer) Run(ctx context.Context) error {
	var messages []kafka.Message
	p.startTime = time.Now()

	p.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{p.BrokerURL},
		Topic:   p.TopicName,
	})

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			for i := 1; i <= p.MessageRate; i++ {
				var buf bytes.Buffer
				fmt.Fprintf(&buf,
					`{"symbol":"%s", "price":%d, "side":"%s", "quantity":%d, "account":"%s"}`,
					symbols[rand.Intn(len(symbols))],
					rand.Intn(1000)+5,
					sides[rand.Intn(len(sides))],
					rand.Intn(5000)+1,
					accounts[rand.Intn(len(accounts))],
				)
				messages = append(messages, kafka.Message{Value: buf.Bytes()})
			}
			if err := p.writer.WriteMessages(ctx, messages...); err != nil {
				messages = messages[:0]
				return err
			}
			messages = messages[:0]
		}
	}

	p.writer.Close()
	ticker.Stop()

	return nil
}

func (p *Producer) CreateTopic(ctx context.Context) error {
	conn, err := net.Dial("tcp", p.BrokerURL)
	if err != nil {
		return err
	}

	return kafka.NewConn(conn, "", 0).CreateTopics(
		kafka.TopicConfig{
			Topic:             p.TopicName,
			NumPartitions:     p.TopicParts,
			ReplicationFactor: 1,
		})
}

func (p *Producer) Report() {
	if p.writer == nil {
		return
	}

	duration := time.Since(p.startTime)

	stats := p.writer.Stats()
	p.totalMessages += stats.Messages
	p.totalErrors += stats.Errors
	p.totalBytes += stats.Bytes

	var rate string
	if p.totalMessages > 0 {
		rate = humanize.FtoaWithDigits(float64(p.totalMessages)/duration.Truncate(time.Second).Seconds(), 0) + "/sec"
	} else {
		rate = "0/sec"
	}

	fmt.Printf(
		"duration=%-*s |   rate=%-*s     |   sent=%-*s    |   bytes=%-*s |   errors=%-*s",
		10, duration.Truncate(time.Second),
		10, rate,
		10, humanize.Comma(int64(p.totalMessages)),
		10, humanize.Bytes(uint64(p.totalBytes)),
		10, humanize.Comma(int64(p.totalErrors)))
}
