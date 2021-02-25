package main

import (
	"context"
	"data_porting_service/functions"
	"data_porting_service/models"
	"data_porting_service/utils"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	cr "github.com/brkelkar/common_utils/configreader"
	"github.com/brkelkar/common_utils/logger"
)

type BukectStruct struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Bucket  string    `json:"bucket"`
	Updated time.Time `json:"updated"`
	Size    string    `json:"size"`
}

var (
	dateFormatMap map[string]string
	err           error
	cfg           cr.Config
	gcsFileAttr   utils.GcsFile
	//gcsObj      gc.GcsBucketClient
)

func main() {
	projectID := "awacs-dev"
	subID := "awacsstock"
	cfg.ReadFile("config.yml")
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		logger.Error("pubsub.NewClient: %v", err)
	}
	defer client.Close()
	maxGoroutines := 2
	sub := client.Subscription(subID)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create a channel to handle messages to as they come in.
	cm := make(chan *pubsub.Message)
	cnt := 1
	defer close(cm)
	guard := make(chan struct{}, maxGoroutines)
	// Handle individual messages in a goroutine.
	go func() {
		for msg := range cm {
			guard <- struct{}{} // would block if guard channel is already filled
			fmt.Println(cnt)
			go func(ctx context.Context, msg pubsub.Message) {
				time.Sleep(10 * time.Millisecond)
				worker(ctx, msg)
				msg.Ack()
				<-guard
			}(ctx, *msg)
			cnt++
		}

	}()

	// Receive blocks until the context is cancelled or an error occurs.
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		cm <- msg
	})
	if err != nil {
		logger.Error("pubsub.NewClient: %v", err)
		log.Fatal("Error while recieving Message")
	}

}

func worker(ctx context.Context, msg pubsub.Message) {
	var bucketDetails BukectStruct
	json.Unmarshal(msg.Data, &bucketDetails)
	var e models.GCSEvent
	e.Bucket = bucketDetails.Bucket
	//fmt.Println(bucketDetails.Name)
	e.Name = bucketDetails.Name
	e.Updated = bucketDetails.Updated
	e.Size = bucketDetails.Size
	g := gcsFileAttr.HandleGCSEvent(ctx, e)
	if !g.GcsClient.GetLastStatus() {
		log.Print("Error while reading file")
		g.GcsClient.MoveObject(g.FileName, "error_Files/"+g.FileName, "balaawacstest")
		return
	}

	var stockObj functions.StockAttr
	if len(msg.Data) > 0 {
		stockObj.StockCloudFunction(g, cfg)
	}
	//fmt.Println(bucketDetails.Bucket)
}
