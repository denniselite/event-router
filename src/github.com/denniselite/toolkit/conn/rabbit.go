package conn

import (
	"github.com/streadway/amqp"
	"errors"
	"encoding/json"
	"time"
	"log"
	. "github.com/denniselite/toolkitlite/toolkit/errors"
	"os"
	"strconv"
	"runtime"
)

const (
	EXCHANGE_EVENTS_ROUTER_COMMON_PENDING = "events_router.common.pending"

	EXCHANGE_DIRECT_TYPE = "direct"
)

type RmqMessage interface {
	GetJson() []byte
}

type RmqInt interface {
	Ping() error
	Reconnect() error
	Channel() (ChannelInt, error)
	Rpc(queueName string, uuid string, body []byte) ([]byte, error)
	ConsumeRpc(queueName string, worker func(body []byte) (RmqMessage, error)) error
	Consume(queueName string, worker func(body []byte) (error)) error
	Publish(queueName string, body []byte, headers amqp.Table) error
	ExchangePublish(exchangeName string, exchangeType string, routingKey string, data []byte) (err error)
}

type ChannelInt interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Close() error
}

type Rmq struct {
	Url           string
	Conn          *amqp.Connection
	ErrorFilePath string
}

// NewRmq создает объект Rmq с подключением к Rabbit
func NewRmq(connStr string) (rmq *Rmq, err error) {
	rmq = new(Rmq)
	rmq.Url = connStr
	rmq.Conn, err = amqp.Dial(connStr)
	return
}

func (rmq *Rmq) Ping() error {
	if rmq.Conn == nil {
		return NewError(nil, RabbitException)
	}
	return nil
}

// Reconnect выполняет повторное соеднинение
func (rmq *Rmq) Reconnect() (err error) {
	rmq.Conn, err = amqp.Dial(rmq.Url)
	return
}

// Channel создает канал для обмена сообщениями с попыткой одного реконнекта
func (rmq *Rmq) Channel() (ch ChannelInt, err error) {
	ch, err = rmq.Conn.Channel()
	// Try to reconnect to open channel
	if err != nil {
		err = rmq.Reconnect()
		if err != nil {
			return
		}
		ch, err = rmq.Conn.Channel()
	}
	return
}

// Rpc публикация сообщения в очередь с ожиданием ответа в ответную очередь по уникальному ключу
func (rmq *Rmq) Rpc(queueName string, uuid string, body []byte) (res []byte, err error) {
	if queueName == "" {
		err = errors.New("Empty queue name")
		return
	}
	if uuid == "" {
		err = errors.New("Empty uuid")
		return
	}
	if len(body) == 0 {
		err = errors.New("Empty body")
		return
	}

	ch, err := rmq.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return
	}

	responseQueue, err := ch.QueueDeclare(uuid, false, true, true, false, nil)
	if err != nil {
		return
	}

	msgs, err := ch.Consume(responseQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return
	}

	err = ch.Publish("", queue.Name, false, false, amqp.Publishing{
		ContentType:   "application/json",
		ReplyTo:       responseQueue.Name,
		Body:          body,
	})
	if err != nil {
		return
	}

	// После двух секунд ожиданий возвращаем ошибку по таймауту
	go func() {
		time.Sleep(time.Second * 5)
		ch.Publish("", responseQueue.Name, false, false, amqp.Publishing{
			ContentType:   "application/json",
			Body:          NewError(nil, RequestTimeout).GetJson(),
		})
	}()

	// Ожидаем ответное сообщение
	d := <-msgs
	var js interface{}
	if json.Unmarshal(d.Body, &js) == nil {
		if jsMap, ok := js.(map[string]interface{}); ok {
			if val, ok := jsMap["error"]; ok {
				err = errors.New(val.(string))
			} else {
				res = d.Body
			}
		} else {
			res = d.Body
		}
		d.Ack(false)
	} else {
		err = errors.New("Invalid response from queue")
	}

	return
}

func (rmq *Rmq) ConsumeRpc(queueName string, worker func(body []byte) (RmqMessage, error)) (err error) {
	if queueName == "" {
		err = errors.New("Empty queue name")
		return
	}

	ch, err := rmq.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return
	}

	var forever <-chan struct{}

	for i := 0; i < runtime.NumCPU(); i++ {
		go func(work <-chan amqp.Delivery) {
			for d := range work {
				func() {
					defer func() {
						if ctx := recover(); ctx != nil {
							log.Printf("Error: %v \n Message: %v", ctx, string(d.Body))
							if err, ok := ctx.(error); ok {
								msg, err := json.Marshal(map[string]string{"error" : err.Error()})
								if err == nil {
									ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
										ContentType: "application/json",
										Body: msg,
									})
								}
							}
							d.Ack(false)
						}
					}()

					res, err := worker(d.Body)
					var body []byte
					if err != nil {
						log.Println(err.Error())
						log.Println(err)
						body, err = json.Marshal(map[string]string{"error" : err.Error()})
						log.Println(string(body))
					} else {
						body = res.GetJson()
					}
					ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
						ContentType: "application/json",
						Body: body,
					})
					d.Ack(false)
				}()
			}
		}(msgs)
	}
	<-forever

	return
}

func (rmq *Rmq) Consume(queueName string, worker func(body []byte) (error)) (err error) {
	if queueName == "" {
		err = errors.New("Empty queue name")
		return
	}

	ch, err := rmq.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return
	}

	var forever <-chan struct{}
	for i := 0; i < runtime.NumCPU(); i++ {
		go func(work <-chan amqp.Delivery) {
			for d := range work {
				func() {
					err := worker(d.Body)

					if err != nil {
						d.Reject(true)
					} else {
						d.Ack(false)
					}
				}()
			}
		}(msgs)
	}
	<-forever

	return
}

func (rmq *Rmq) ExchangePublish(exchangeName string, exchangeType string, routingKey string, data []byte) (err error) {
	if exchangeName == "" {
		err = errors.New("Empty exchange name")
		return
	}

	if routingKey == "" {
		err = errors.New("Empty routing key")
		return
	}

	ch, err := rmq.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,  // name
		exchangeType,  // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)

	if err != nil {
		return
	}

	err = ch.Publish(
		exchangeName, // exchange
		routingKey,   // routing key
		false, 		  // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		})

	return
}

func (rmq *Rmq) Publish(queueName string, data []byte, headers amqp.Table) (err error) {
	defer func() {
		if r := recover(); r != nil {
			filename := rmq.ErrorFilePath + strconv.Itoa(int(time.Now().Unix()))
			var f *os.File
			var e error
			if _, exists := os.Stat(filename); exists != nil {
				f, e = os.Create(filename)
			} else {
				f, e = os.OpenFile(filename, os.O_APPEND | os.O_WRONLY, os.ModeAppend)
			}
			if e == nil {
				defer f.Close()
			}

			_, e = f.Write([]byte(queueName + "\n"))
			_, e = f.Write(data)
			if e != nil {
				log.Println("Cant write to file " + filename + " " + string(data))
			}
			err = NewError(nil, RabbitException)
			return
		}
	}()
	if queueName == "" {
		err = errors.New("Empty queue name")
		return
	}

	ch, err := rmq.Channel()
	Oops(err)
	defer ch.Close()

	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	Oops(err)

	Oops(ch.Publish("", queueName, false, false, amqp.Publishing{
		Headers: headers,
		ContentType: "application/json",
		Body: data,
		DeliveryMode: 2,
	}))

	return
}