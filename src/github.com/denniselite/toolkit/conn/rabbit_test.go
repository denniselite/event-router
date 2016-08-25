package conn

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"
	"errors"
	. "github.com/denniselite/toolkitlite/toolkit/errors"
)

var rmqConnString = "amqp://guest:guest@127.0.0.1:5672"

func TestRabbit(t *testing.T) {
	Convey("Test New function", t, func() {
		Convey("Invalid connection string", func() {
			connStr := ""
			rmq, err := NewRmq(connStr)
			So(rmq.Url, ShouldEqual, connStr)
			So(func() { _ = *rmq.Conn }, ShouldPanic)
			So(err, ShouldNotBeNil)

			connStr = "invalid"
			rmq, err = NewRmq(connStr)
			So(rmq.Url, ShouldEqual, connStr)
			So(func() { _ = *rmq.Conn }, ShouldPanic)
			So(err, ShouldNotBeNil)
		})
	})

	Convey("Test Reconnect function", t, func() {
		Convey("Invalid connection string", func() {
			connStr := ""
			rmq := &Rmq{Url: connStr}
			err := rmq.Reconnect()
			So(rmq.Url, ShouldEqual, connStr)
			So(func() { _ = *rmq.Conn }, ShouldPanic)
			So(err, ShouldNotBeNil)
		})
	})

	Convey("Test Channel function", t, func() {
		Convey("Has active connection", func() {
			rmq := new(RmqMock)
			_, err := rmq.Channel()
			So(err, ShouldBeNil)
			//So(*ch, ShouldHaveSameTypeAs, amqp.Channel{})
		})

		Convey("Has no active connection", func() {
			rmq := new(RmqMock)
			_, err := rmq.Channel()
			So(err, ShouldBeNil)
		})

		Convey("Has no active connection and reconnect fails", func() {
			rmq := new(RmqMock)
			_, err := rmq.Channel()
			So(err, ShouldNotBeNil)
		})
	})

	Convey("Test Rpc func", t, func() {
		queueName := "testQueue"
		uuid := "uuid"
		body := []byte("{\"key\":\"value\"}")

		c, _ := amqp.Dial(rmqConnString)
		ch, _ := c.Channel()
		ch.QueueDelete(queueName, false, false, false)
		ch.QueueDelete(uuid, false, false, false)

		Convey("Передача пустых значений и правильного json в body", func() {
			rmq, _ := NewRmq(rmqConnString)
			defer rmq.Conn.Close()

			_, err := rmq.Rpc("", uuid, body)
			So(err, ShouldResemble, errors.New("Empty queue name"))

			_, err = rmq.Rpc(queueName, "", body)
			So(err, ShouldResemble, errors.New("Empty uuid"))

			_, err = rmq.Rpc(queueName, uuid, []byte{})
			So(err, ShouldResemble, errors.New("Empty body"))
		})

		Convey("Ошибка при создании канала", func() {
			rmq, _ := NewRmq(rmqConnString)
			rmq.Conn.Close()
			rmq.Url = ""
			_, err := rmq.Rpc(queueName, uuid, body)
			So(err, ShouldNotBeNil)
		})

		Convey("Ошибка при создании очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			ch, _ := rmq.Conn.Channel()

			// Создаем очередь с отличными параметрами, чтобы вызвать ошибку
			ch.QueueDeclare(queueName, false, false, false, false, nil)
			_, err := rmq.Rpc(queueName, uuid, body)
			So(err, ShouldNotBeNil)
			ch.QueueDelete(queueName, false, false, false)

			// Создаем очередь с отличными параметрами, чтобы вызвать ошибку
			ch.QueueDeclare(uuid, false, false, false, false, nil)
			_, err = rmq.Rpc(queueName, uuid, body)
			So(err, ShouldNotBeNil)
			ch.QueueDelete(uuid, false, false, false)
			//  При этом очередь по названию queueName должна была быть создана
			_, err = ch.QueueDelete(queueName, false, false, false)
			So(err, ShouldBeNil)
		})

		Convey("Обработка ответа из ответной очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			ch, _ := rmq.Conn.Channel()

			Convey("При долгом ожидании ответа", func() {
				_, err := rmq.Rpc(queueName, uuid, body)
				So(err, ShouldResemble, NewError(nil, RequestTimeout))

				ch.QueueDelete(queueName, false, false, false)
				ch.QueueDelete(uuid, false, false, false)
			})

			Convey("При невалидном ответе", func() {
				value := []byte("invalid")

				go requestHandler(queueName, ch, value)

				_, err := rmq.Rpc(queueName, uuid, body)
				So(err, ShouldResemble, errors.New("Invalid response from queue"))

				ch.QueueDelete(queueName, false, false, false)
				ch.QueueDelete(uuid, false, false, false)
			})

			Convey("При успешном ответе", func() {
				value := []byte("{\"key\":\"value\"}")
				go requestHandler(queueName, ch, value)
				res, err := rmq.Rpc(queueName, uuid, body)
				So(err, ShouldBeNil)
				So(res, ShouldResemble, value)

				ch.QueueDelete(queueName, false, false, false)
				ch.QueueDelete(uuid, false, false, false)
			})

			Convey("При ответе ошибки", func() {
				value := []byte("{\"errorCode\":1000, \"errorMessage\":\"errmsg\"}")
				go requestHandler(queueName, ch, value)
				_, err := rmq.Rpc(queueName, uuid, body)
				So(err, ShouldResemble, NewError(nil, 1000))

				ch.QueueDelete(queueName, false, false, false)
				ch.QueueDelete(uuid, false, false, false)
			})

		})
	})

	Convey("Test ConsumeRpc", t, func() {
		queueName := "testQueue"
		emptyWorker := func(body []byte) (res RmqMessage, err error) { return new(RmqMessageMock), nil }

		Convey("Передача пустого имени очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			defer rmq.Conn.Close()

			err := rmq.ConsumeRpc("", emptyWorker)
			So(err, ShouldResemble, errors.New("Empty queue name"))
		})

		Convey("Ошибка при создании канала", func() {
			rmq, _ := NewRmq(rmqConnString)
			rmq.Conn.Close()
			rmq.Url = ""
			err := rmq.ConsumeRpc(queueName, emptyWorker)
			So(err, ShouldNotBeNil)
		})

		Convey("Ошибка при создании очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			ch, _ := rmq.Conn.Channel()

			// Создаем очередь с отличными параметрами, чтобы вызвать ошибку
			ch.QueueDeclare(queueName, false, false, false, false, nil)
			err := rmq.ConsumeRpc(queueName, emptyWorker)
			So(err, ShouldNotBeNil)
			ch.QueueDelete(queueName, false, false, false)
		})
	})

	Convey("Test Consume", t, func() {
		queueName := "testQueue"
		emptyWorker := func(body []byte) (err error) { return }

		Convey("Передача пустого имени очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			defer rmq.Conn.Close()

			err := rmq.Consume("", emptyWorker)
			So(err, ShouldResemble, errors.New("Empty queue name"))
		})

		Convey("Ошибка при создании канала", func() {
			rmq, _ := NewRmq(rmqConnString)
			rmq.Conn.Close()
			rmq.Url = ""
			err := rmq.Consume(queueName, emptyWorker)
			So(err, ShouldNotBeNil)
		})

		Convey("Ошибка при создании очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			ch, _ := rmq.Conn.Channel()

			// Создаем очередь с отличными параметрами, чтобы вызвать ошибку
			ch.QueueDeclare(queueName, false, false, false, false, nil)
			err := rmq.Consume(queueName, emptyWorker)
			So(err, ShouldNotBeNil)
			ch.QueueDelete(queueName, false, false, false)
		})
	})

	Convey("Test Publish", t, func() {
		queueName := "testQueue"

		Convey("Передача пустого имени очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			defer rmq.Conn.Close()

			err := rmq.Publish("", []byte{})
			So(err, ShouldResemble, errors.New("Empty queue name"))
		})

		Convey("Ошибка при создании канала", func() {
			rmq, _ := NewRmq(rmqConnString)
			rmq.Conn.Close()
			rmq.Url = ""
			err := rmq.Publish(queueName, []byte{})
			So(err, ShouldNotBeNil)
		})

		Convey("Ошибка при создании очереди", func() {
			rmq, _ := NewRmq(rmqConnString)
			ch, _ := rmq.Conn.Channel()

			// Создаем очередь с отличными параметрами, чтобы вызвать ошибку
			ch.QueueDeclare(queueName, false, false, false, false, nil)
			err := rmq.Publish(queueName, []byte{})
			So(err, ShouldNotBeNil)
			ch.QueueDelete(queueName, false, false, false)
		})
	})
}

func requestHandler(queueName string, ch *amqp.Channel, value []byte) {
	q, err := ch.QueueDeclare(queueName, true, false, false,false, nil)
	if err != nil {
		panic(err)
	}
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	for d := range msgs {
		err = ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body: value,
		})
		if err != nil {
			panic(err)
		}
		d.Ack(false)
		return
	}
}