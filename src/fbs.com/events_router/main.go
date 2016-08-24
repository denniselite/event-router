package main

import (
    "flag"
    "log"
    "io/ioutil"
    "fmt"
    "fbs.com/toolkit/conn"
    . "fbs.com/toolkit/errors"
    "github.com/streadway/amqp"
    "github.com/jinzhu/gorm"
    "gopkg.in/yaml.v2"
)

type Config struct {
    Listen       int
    Pgsql        struct {
                     Host     string
                     Port     int
                     Username string
                     Password string
                     Database string
                 }
    MySQL        struct {
                     Host     string
                     Port     int
                     Username string
                     Password string
                     Database string
                 }
    Rabbit       struct {
                     Host     string
                     Port     int
                     Username string
                     Password string
                     Queue    string
                 }
}

func main() {
    var configFile string

    // register flags for config file
    flag.StringVar(&configFile, "config", "", "config filename")
    flag.StringVar(&configFile, "c", "", "config filename (shorthand)")

    flag.Parse()

    if configFile == "" {
        log.Fatalln("config file required")
    }

    // load config from file
    data, err := ioutil.ReadFile(configFile)
    cfg := new(Config)
    err = yaml.Unmarshal(data, &cfg)

    if err != nil {
        log.Fatalln(err)
    }

    rmq, db, dbMySQL, err := cfg.connect()
    Oops(err)

    er := new(EventsRouter)
    er.ListenPort = cfg.Listen
    cfg.run(er, db, dbMySQL, rmq)
}

func checker(c chan *amqp.Error, rmq *conn.Rmq) {
    rmq.Conn.NotifyClose(c)
}

func (cfg *Config) run(er *EventsRouter, db *gorm.DB, dbMySQL *gorm.DB, rmq conn.RmqInt) {
    defer func() {
        if r := recover(); r != nil {
            log.Println(r)
            cfg.restart(er)
        }
    }()
    er.Run(db, dbMySQL, rmq)

    c := make(chan *amqp.Error)
    go checker(c, rmq.(*conn.Rmq))

    rmqStatus := <-c
    if (rmqStatus != nil) {
        cfg.restart(er)
    }
}

func (cfg *Config) restart(er *EventsRouter) {
    for {
        rmq, db, dbMySQL, err := cfg.connect()
        if err == nil {
            cfg.run(er, db, dbMySQL, rmq)
            return
        }
    }
}

func (cfg *Config) connect() (rmq conn.RmqInt, db *gorm.DB, dbMySQL *gorm.DB, err error) {

    //get postgres connection
    pgConnectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        cfg.Pgsql.Host,
        cfg.Pgsql.Port,
        cfg.Pgsql.Username,
        cfg.Pgsql.Password,
        cfg.Pgsql.Database,
    )

    db, err = conn.Connect(pgConnectionString)

    if err != nil {
        return
    }

    log.Printf("Postgres connected at %s:%d\n", cfg.Pgsql.Host, cfg.Pgsql.Port)

    //get mysql connection
    mysqlConnectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
        cfg.MySQL.Username,
        cfg.MySQL.Password,
        cfg.MySQL.Host,
        cfg.MySQL.Port,
        cfg.MySQL.Database,
    )
    dbMySQL, err = conn.ConnectMySQL(mysqlConnectionString)
    if err != nil {
        return
    }

    log.Printf("MySQL connected at %s:%d\n", cfg.MySQL.Host, cfg.MySQL.Port)

    // get rabbit connection
    rabbitConnectionString := fmt.Sprintf("amqp://%s:%s@%s:%d",
        cfg.Rabbit.Username,
        cfg.Rabbit.Password,
        cfg.Rabbit.Host,
        cfg.Rabbit.Port,
    )
    rmq, err = conn.NewRmq(rabbitConnectionString)
    if err != nil {
        return
    }

    log.Printf("RabbitMQ connected at %s:%d\n", cfg.Rabbit.Host, cfg.Rabbit.Port)

    return
}