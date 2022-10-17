package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/renatodaltiba/imersaofsfc2-simulator/application/route"
	"github.com/renatodaltiba/imersaofsfc2-simulator/infra/kafka"
)

func Produce(msg *ckafka.Message) error {
	producer := kafka.NewKafkaProducer()
	route := route.NewRoute()
	err := json.Unmarshal(msg.Value, &route)
	if err != nil {
		return err
	}

	route.LoadPositions()
	positions, err := route.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}

	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KAFKA_PRODUCE_TOPIC"), producer)
		time.Sleep(time.Millisecond * 500)
	}

	return nil
}
