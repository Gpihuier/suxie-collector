package queue

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MQPublisher struct {
	producer   *Producer
	routingKey string
}

func NewMQPublisher(producer *Producer, routingKey string) *MQPublisher {
	return &MQPublisher{producer: producer, routingKey: routingKey}
}

func (p *MQPublisher) Publish(ctx context.Context, msg CollectMessage) error {
	if p.producer == nil {
		return fmt.Errorf("nil producer")
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal collect message: %w", err)
	}
	headers := amqp.Table{
		"tenant_id": msg.TenantID,
		"platform":  msg.Platform,
		"job_name":  msg.JobName,
	}
	return p.producer.Publish(ctx, body, p.routingKey, headers)
}
