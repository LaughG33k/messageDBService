package app

import (
	"context"
	"fmt"
	"time"

	"github.com/LaughG33k/messageDBService/iternal/client/mongo"
	"github.com/LaughG33k/messageDBService/pkg"
)

func Run() {

	log, err := pkg.InitLogrus("/Users/user/Desktop/messengerMicroservice/messageDBworkerService/logs.json")

	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	mcl, err := mongo.NewMongoClient(
		ctx,
		mongo.MongoClientConfig{
			Host:                   "127.0.0.1",
			Port:                   "27017",
			Db:                     "messages",
			BulkWriteTimeSleep:     1 * time.Second,
			HealthCheakWaitingTime: 15 * time.Second,
			ReconectWaitingTime:    1 * time.Minute,
			OperationTimeout:       30 * time.Second,

			RecconectAttempts: 10,
			MaxPoolSize:       250,

			RetryWrites: true,
			RetryReads:  true,
		},
	)

	if err != nil {
		log.Panic(err)
		return
	}

	fmt.Println(mcl)

	for {

	}

}
