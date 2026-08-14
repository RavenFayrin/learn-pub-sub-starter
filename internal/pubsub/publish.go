package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	dat, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        dat,
	})
}

type SimpleQueueType int

const (
    SimpleQueueDurable SimpleQueueType = iota
    SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	chPublish, err := conn.Channel()
	if err != nil{
		return nil, amqp.Queue{}, err
	}
	var queue amqp.Queue

	if queueType == SimpleQueueDurable {
		queue, err = chPublish.QueueDeclare(queueName, true, false, false, false, nil)
		if err != nil{
			return nil, amqp.Queue{}, err
		}
	} else {
		queue, err = chPublish.QueueDeclare(queueName, false, true, true, false, nil)
		if err != nil{
			return nil, amqp.Queue{}, err
		}
	}
	err = chPublish.QueueBind(queueName, key, exchange, false, nil)
	if err != nil{
		return nil, amqp.Queue{}, err
	}
	return chPublish, queue, nil	
}