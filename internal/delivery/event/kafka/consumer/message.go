package consumer

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Topic     string
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
}

func newMessage(rec *kgo.Record) Message {
	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}

	return Message{
		Topic:     rec.Topic,
		Value:     rec.Value,
		Headers:   headers,
		Partition: rec.Partition,
		Offset:    rec.Offset,
	}
}
