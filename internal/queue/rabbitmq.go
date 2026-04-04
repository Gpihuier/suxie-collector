package queue

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"suxie.com/suxie-collector/internal/config"
)

type Producer struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	exchange   string
	routingKey string
	mandatory  bool
}

func NewProducer(cfg config.RabbitMQConfig) (*Producer, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		cfg.Exchange,
		cfg.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	return &Producer{
		conn:       conn,
		ch:         ch,
		exchange:   cfg.Exchange,
		routingKey: cfg.RoutingKey,
		mandatory:  cfg.Mandatory,
	}, nil
}

func (p *Producer) Publish(ctx context.Context, body []byte, routingKey string, headers amqp.Table) error {
	if routingKey == "" {
		routingKey = p.routingKey
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := amqp.Publishing{
		Headers:      headers,
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}

	if err := p.ch.PublishWithContext(ctx, p.exchange, routingKey, p.mandatory, false, msg); err != nil {
		return fmt.Errorf("publish rabbitmq message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	if p == nil {
		return nil
	}
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
