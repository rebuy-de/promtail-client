package promtail

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/afiskon/promtail-client/logproto"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/golang/snappy"
)

type protoLogEntry struct {
	entry *logproto.Entry
	level LogLevel
}

type Client struct {
	config    *ClientConfig
	quit      chan struct{}
	entries   chan protoLogEntry
	waitGroup sync.WaitGroup
	client    httpClient
}

func NewClientProto(conf ClientConfig) (*Client, error) {
	client := Client{
		config:  &conf,
		quit:    make(chan struct{}),
		entries: make(chan protoLogEntry, LOG_ENTRIES_CHAN_SIZE),
		client:  httpClient{},
	}

	client.waitGroup.Add(1)
	go client.run()

	return &client, nil
}

func (c *Client) Debug(data map[string]interface{}) {
	c.log(DEBUG, "debug", data)
}

func (c *Client) Info(data map[string]interface{}) {
	c.log(INFO, "info", data)
}

func (c *Client) Warn(data map[string]interface{}) {
	c.log(WARN, "warning", data)
}

func (c *Client) Error(data map[string]interface{}) {
	c.log(ERROR, "error", data)
}

func (c *Client) log(level LogLevel, levelText string, data map[string]interface{}) {
	if (level >= c.config.SendLevel) || (level >= c.config.PrintLevel) {

		data["level"] = levelText

		buf, err := json.MarshalIndent(data, "", "    ")
		if err != nil {
			log.Printf("unable to marshal log data: %s\n", err)
			return
		}

		now := time.Now().UnixNano()
		c.entries <- protoLogEntry{
			entry: &logproto.Entry{
				Timestamp: &timestamp.Timestamp{
					Seconds: now / int64(time.Second),
					Nanos:   int32(now % int64(time.Second)),
				},
				Line: string(buf),
			},
			level: level,
		}
	}
}

func (c *Client) Shutdown() {
	close(c.quit)
	c.waitGroup.Wait()
}

func (c *Client) run() {
	var batch []*logproto.Entry
	batchSize := 0
	maxWait := time.NewTimer(c.config.BatchWait)

	defer func() {
		if batchSize > 0 {
			c.send(batch)
		}

		c.waitGroup.Done()
	}()

	for {
		select {
		case <-c.quit:
			return
		case entry := <-c.entries:
			if entry.level >= c.config.PrintLevel {
				log.Print(entry.entry.Line)
			}

			if entry.level >= c.config.SendLevel {
				batch = append(batch, entry.entry)
				batchSize++
				if batchSize >= c.config.BatchEntriesNumber {
					c.send(batch)
					batch = []*logproto.Entry{}
					batchSize = 0
					maxWait.Reset(c.config.BatchWait)
				}
			}
		case <-maxWait.C:
			if batchSize > 0 {
				c.send(batch)
				batch = []*logproto.Entry{}
				batchSize = 0
			}
			maxWait.Reset(c.config.BatchWait)
		}
	}
}

func (c *Client) send(entries []*logproto.Entry) {
	var streams []*logproto.Stream
	streams = append(streams, &logproto.Stream{
		Labels:  c.config.Labels,
		Entries: entries,
	})

	req := logproto.PushRequest{
		Streams: streams,
	}

	buf, err := proto.Marshal(&req)
	if err != nil {
		log.Printf("promtail.ClientProto: unable to marshal: %s\n", err)
		return
	}

	buf = snappy.Encode(nil, buf)

	resp, body, err := c.client.sendJsonReq("POST", c.config.PushURL, "application/x-protobuf", buf)
	if err != nil {
		log.Printf("promtail.ClientProto: unable to send an HTTP request: %s\n", err)
		return
	}

	if resp.StatusCode != 204 {
		log.Printf("promtail.ClientProto: Unexpected HTTP status code: %d, message: %s\n", resp.StatusCode, body)
		return
	}
}
