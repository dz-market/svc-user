package consumer

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Topic     string
	Value     []byte
	Headers   []Header
	Partition int32
	Offset    int64
}

type Header struct {
	Key   string
	Value []byte
}

func newMessage(rec *kgo.Record) Message {
	headers := make([]Header, 0, len(rec.Headers))
	for _, h := range rec.Headers {
		headers = append(
			headers, Header{
				Key:   h.Key,
				Value: h.Value,
			},
		)
	}

	return Message{
		Topic:     rec.Topic,
		Value:     rec.Value,
		Headers:   headers,
		Partition: rec.Partition,
		Offset:    rec.Offset,
	}
}
