package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strconv"
	"time"
)

//异步消息模式
func syncProducer() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 5 * time.Second
	p, err := sarama.NewAsyncProducer([]string{"192.168.1.23:9092"}, config)
	if err != nil {
		log.Printf("sarama.NewSyncProducer err, message=%s \n", err)
		return
	}
	defer p.Close()

	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					fmt.Println(err)
				}
			case <-success:
			}
		}
	}(p)
	for i := 0; i < 10000; i++ {
		time.Sleep(time.Second)
		v := strconv.Itoa(i)
		fmt.Println(v)
		msg := &sarama.ProducerMessage{
			Topic: "mashuai",
			Value: sarama.ByteEncoder(v),
		}
		p.Input() <- msg
	}

}

func main() {
	syncProducer()
	fmt.Println("success")
}
