package integration_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LaughG33k/messageDBService/iternal/client/mongo"

	"golang.org/x/net/context"
)

var ctx context.Context = context.Background()

func initClient() (*mongo.MongoClient, error) {

	tm, canc := context.WithTimeout(ctx, 5*time.Second)

	defer canc()

	client, err := mongo.NewMongoClient(tm, mongo.MongoClientConfig{
		Host:                   "127.0.0.1",
		Port:                   "27017",
		Db:                     "messages",
		Collection:             "messages",
		BulkWriteTimeSleep:     1 * time.Second,
		HealthCheakWaitingTime: 10 * time.Second,
		ReconectWaitingTime:    5 * time.Second,
		OperationTimeout:       30 * time.Second,

		RecconectAttempts: 1,
		MaxPoolSize:       250,

		RetryWrites: false,
		RetryReads:  true,
	})

	if err != nil {
		return nil, err
	}

	return client, nil

}

func TestInitUser(t *testing.T) {

	client, err := initClient()

	if err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		Name string
		User []string
	}

	const testAmount int = 10
	const users int = 1000

	testsCase := [testAmount]testCase{}

	for x := 0; x < len(testsCase); x++ {

		testsCase[x] = testCase{}

		for i := 0; i < users; i++ {

			res := fmt.Sprintf("user%d%d", x, i)

			testsCase[x].Name = res
			testsCase[x].User = append(testsCase[x].User, res)

		}

	}

	for _, test := range testsCase {

		tc := test

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			wg := &sync.WaitGroup{}
			wg.Add(len(tc.User))

			for _, v := range tc.User {
				name := v
				go func() {

					tm, canc := context.WithTimeout(ctx, 10*time.Second)
					defer canc()
					defer wg.Done()

					t.Log("start adding a user")
					if err := client.InitUser(tm, name); err != nil {
						t.Log(err)
						t.Log("end adding a user")
						return
					}
					t.Log("end adding a user")
				}()
			}

			wg.Wait()

			t.Log("users have been created")

		})

	}

}
