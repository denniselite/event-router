package main

import (
    "github.com/kataras/iris"
    "gopkg.in/validator.v2"
    "github.com/denniselite/toolkit/conn"
    . "github.com/denniselite/toolkit/errors"
    "github.com/denniselite/events_router/structs"
    "encoding/json"
)

func (er *EventsRouter) ProcessMessage(ctx *iris.Context) {
    // Уникальный идентификатор запроса
    uuid := ctx.GetString("uuid")

    // Пытаемся распарсить содержимое запроса в структуру PsRequest
    var signature structs.InputMessage
    if err := ctx.ReadJSON(&signature); err != nil {
        ctx.JSON(HttpApiError(NewError(err, BadRequest)))
        return
    }

    ctx.Log("%s Signature: %#v\n", uuid, signature)
    // Выполняем валидацию стрктуры запроса
    if err := validator.Validate(signature); err != nil {
        ctx.JSON(HttpApiError(err))
        return
    }

    //Проверяем данные в запросе, должны представлять собой правильный JSON
    var inputData interface{}
    if err := json.Unmarshal([]byte(signature.Data), &inputData); err != nil {
        ctx.JSON(HttpApiError(err))
        return
    }

    // Выполняем отправку сообщения в exchange Rabbit'а с routing key
    err := er.Rmq.ExchangePublish(conn.EXCHANGE_EVENTS_ROUTER_COMMON_PENDING, conn.EXCHANGE_DIRECT_TYPE, signature.Route, []byte(signature.Data))
    if err != nil {
        ctx.JSON(HttpApiError(err))
        return
    }

    outResponse := structs.Response{Error:nil, Message:"ok"}
    // Отдаем результат
    ctx.JSON(iris.StatusAccepted, outResponse)
}
