package integration_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/LaughG33k/messageDBService/iternal/client/mongo"
	"github.com/LaughG33k/messageDBService/iternal/model"
	chatrepository "github.com/LaughG33k/messageDBService/iternal/repository/mongo/chatRepository"

	"golang.org/x/net/context"
)

var ctx context.Context = context.Background()

func initRepository() (*chatrepository.ChatRepository, error) {

	tm, canc := context.WithTimeout(ctx, 5*time.Second)

	defer canc()

	client, err := mongo.NewMongoClient(tm, mongo.MongoClientConfig{
		Host:                   "127.0.0.1",
		Port:                   "27017",
		Db:                     "messages",
		Collections:            []string{"messages"},
		BulkWriteTimeSleep:     1 * time.Second,
		HealthCheakWaitingTime: 10 * time.Second,
		ReconectWaitingTime:    5 * time.Second,
		OperationTimeout:       30 * time.Second,

		RecconectAttempts: 1,
		MaxPoolSize:       250,

		RetryWrites: true,
		RetryReads:  true,
	})

	if err != nil {
		return nil, err
	}

	repository := chatrepository.NewRepository(client, "messages")

	return repository, nil

}

func TestSaveMessages(t *testing.T) {

	client, err := initRepository()

	if err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		Name     string
		Messages []model.MessageForSave
	}

	const testAmmount int = 3
	const messages int = 1000

	var testsCase [testAmmount]testCase

	for index := range testsCase {

		testsCase[index].Name = fmt.Sprintf("TestSaveMessages num %d", index)

		testsCase[index].Messages = make([]model.MessageForSave, 0, messages)

		for i := 0; i < messages; i++ {

			message := model.MessageForSave{
				SenderUuid:   fmt.Sprintf("user%d", rand.Intn(messages)),
				ReceiverUuid: fmt.Sprintf("user%d", rand.Intn(messages)),
				Text:         randString(rand.Intn(100)),
				MessageId:    randString(10),
				Time:         time.Now().Unix(),
			}

			testsCase[index].Messages = append(testsCase[index].Messages, message)

		}

	}

	for _, ts := range testsCase {

		test := ts

		t.Run(test.Name, func(t *testing.T) {

			t.Parallel()

			t.Log("Start add messages")

			wg := &sync.WaitGroup{}
			wg.Add(len(test.Messages))

			for _, val := range test.Messages {

				v := val

				go func() {
					t.Logf("%s saves message to recipient %s", v.SenderUuid, v.ReceiverUuid)

					timeout, canc := context.WithTimeout(ctx, 15*time.Second)
					defer canc()
					defer wg.Done()

					if err := client.SaveMessage(timeout, v); err != nil {
						t.Logf("%s was unable to send a message to %s", v.SenderUuid, v.ReceiverUuid)
						t.Error(err)
						return
					}
					t.Logf("%s managed to send a message to %s", v.SenderUuid, v.ReceiverUuid)

				}()

			}

			wg.Wait()

		})

	}

}

func TestInitUser(t *testing.T) {

	client, err := initRepository()

	if err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		Name string
		User []string
	}

	const testAmount int = 1
	const users int = 10000

	testsCase := [testAmount]testCase{}

	for x := 0; x < len(testsCase); x++ {

		testsCase[x] = testCase{}

		for i := 0; i < users; i++ {

			res := fmt.Sprintf("user%d", i)

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
						t.Error(err)
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

func randString(lenght int) string {

	sybmsols := []byte("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890")
	res := ""

	for i := 0; i < lenght; i++ {
		res += string(sybmsols[rand.Intn(len(sybmsols))])
	}

	return res
}
