package main

import (
	"context"
	"data_porting_service/functions"
	"data_porting_service/models"
	"data_porting_service/utils"
	"encoding/json"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	cr "github.com/brkelkar/common_utils/configreader"
	log "github.com/brkelkar/common_utils/logger"
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
	awacsSubNames []string
	projectID     string
	maxGoroutines int64
)

func init() {
	awacsSubNames = []string{"awacsstock-sub", "awacscustomermaster-sub", "awacsoutstanding-sub", "awacsproductmaster-sub", "awacsinvoice-sub"}
	projectID = "awacs-dev"
	maxGoroutines = 10
	cfg.ReadGcsFile("gs://awacs_config/cloud_function_config.yml")
}

func main() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Error("Error while recieving Message: %v", err)
	}
	defer client.Close()

	var awacsSubscriptions []*pubsub.Subscription

	for _, name := range awacsSubNames {
		awacsSubscriptions = append(awacsSubscriptions, client.Subscription(name))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create a channel to handle messages to as they come in.
	cm := make(chan *pubsub.Message)

	defer close(cm)
	guard := make(chan struct{}, maxGoroutines)

	for _, sub := range awacsSubscriptions {
		go func(sub *pubsub.Subscription) {
			// Receive blocks until the context is cancelled or an error occurs.
			err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				cm <- msg
			})
		}(sub)
	}

	for msg := range cm {
		guard <- struct{}{} // would block if guard channel is already filled
		go func(ctx context.Context, msg pubsub.Message) {
			worker(ctx, msg)
			msg.Ack()
			<-guard
		}(ctx, *msg)
	}
}

func worker(ctx context.Context, msg pubsub.Message) {
	var bucketDetails BukectStruct
	json.Unmarshal(msg.Data, &bucketDetails)
	var e models.GCSEvent
	e.Bucket = bucketDetails.Bucket
	e.Name = bucketDetails.Name
	e.Updated = bucketDetails.Updated
	e.Size = bucketDetails.Size
	var mu sync.Mutex
	mu.Lock()
	g := *gcsFileAttr.HandleGCSEvent(ctx, e)
	mu.Unlock()

	switch bucketDetails.Bucket {
	case "awacsstock":
		var stockObj functions.StockAttr
		stockObj.StockCloudFunction(g, cfg)
	case "awacsinvoice":
		var invoiceAttr functions.InvoiceAttr
		invoiceAttr.InvoiceCloudFunction(g, cfg)
	case "awacscustomermaster":
		var customerMasterAttar functions.CustomerMasterAttar
		customerMasterAttar.CustomerMasterCloudFunction(g, cfg)
	case "awacsproductmaster":
		var ProductmasterAttar functions.ProductMasterAttar
		ProductmasterAttar.ProductMasterCloudFunction(g, cfg)
	case "awacsoutstanding":
		var OutstandingAttar functions.OutstandingAttar
		OutstandingAttar.OutstandingCloudFunction(g, cfg)
	}
}
