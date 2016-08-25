package conn

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"errors"
)

type RmqMock struct {
	RpcResult interface{}
	IsConsume bool
	IsPublish bool
}

func (rmq *RmqMock) Ping() (err error) {
	return
}

func (rmq *RmqMock) Reconnect() (err error) {
	return
}

func (rmq *RmqMock) Channel() (ch ChannelInt, err error) {
	ch = new(ChannelMock)
	return
}

func (rmq *RmqMock) Rpc(queueName string, uuid string, body []byte) (res []byte, err error) {
	if e, ok := rmq.RpcResult.(error); ok {
		res = nil
		err = e
	} else {
		res, _ = json.Marshal(rmq.RpcResult)
		err = nil
	}
	return
}

func (rmq *RmqMock) ConsumeRpc(string, func([]byte) (RmqMessage, error)) (err error) {
	return
}

func (rmq *RmqMock) Consume(string, func([]byte) error) (err error) {
	if rmq.IsConsume {
		return
	}
	return errors.New("")
}

func (rmq *RmqMock) Publish(queueName string, data []byte, headers amqp.Table) (err error) {
	if rmq.IsPublish {
		return
	}
	return errors.New("")
}

type ChannelMock struct {

}

func (ch *ChannelMock) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (q amqp.Queue, err error) {
	return
}

func (ch *ChannelMock) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (d <-chan amqp.Delivery, err error) {
	return
}

func (ch *ChannelMock) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (err error) {
	return
}

func (ch *ChannelMock) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) (err error) {
	return
}

func (ch *ChannelMock) Close() (err error) {
	return
}

type RmqMessageMock struct {}

func (m *RmqMessageMock) GetJson() (res []byte) {
	return
}