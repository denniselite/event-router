package main

import (
    "github.com/denniselite/toolkit/conn"
    "github.com/jinzhu/gorm"
    . "github.com/denniselite/toolkit/errors"
    "github.com/kataras/iris"
    "github.com/satori/go.uuid"
    "github.com/kataras/iris/config"
    "github.com/iris-contrib/middleware/logger"
    "fmt"
)

type EventsRouter struct {
    ListenPort           int
    UseSandbox           bool
    ErrorFilePath        string
    Db                   *gorm.DB
    DbMysql              *gorm.DB
    Rmq                  conn.RmqInt
}

func(er *EventsRouter) Run(db *gorm.DB, dbMySQL *gorm.DB, rmq conn.RmqInt) {
    er.Rmq = rmq
    er.Db = db
    er.DbMysql = dbMySQL

    router := er.NewRouter()
    router.Listen(fmt.Sprintf(":%d",er.ListenPort))
}

func (er *EventsRouter) LogPayload (ctx *iris.Context) {
    uuid := uuid.NewV4().String()
    ctx.Set("uuid", uuid)

    ctx.Next()

    ctx.Log("%s Request: %s\n%s Response: %s\n", uuid, ctx.Request.Body(), uuid, ctx.Response.Body())
}

func (er *EventsRouter) NewRouter() *iris.Framework {
    router := iris.New(config.Iris { DisableBanner: true })

    router.UseFunc((*er).LogPayload)
    router.Use(logger.New(iris.Logger))

    router.Post("/process-message", er.ProcessMessage)
    router.Get("/ping", func (ctx *iris.Context) {
        ctx.Text(iris.StatusOK, "pong")
    })

    return router
}