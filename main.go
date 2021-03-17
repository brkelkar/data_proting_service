package main

import (
	"context"
	"data_porting_service/functions"
	"data_porting_service/models"
	"data_porting_service/utils"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	cr "github.com/brkelkar/common_utils/configreader"
	log "github.com/brkelkar/common_utils/logger"
)

//BukectStruct parse data from pubsub
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
	fiveMB        int64
)

func init() {
	awacsSubNames = []string{"awacsstock-sub", "awacscustomermaster-sub", "awacsoutstanding-sub", "awacsproductmaster-sub", "awacsinvoice-sub"}
	projectID = "awacs-dev"
	maxGoroutines = 15
	envVerables := []string{"SERVER_PORT", "SERVER_HOST", "AWACS_DB", "AWACS_SMART_DB", "AWACS_SMART_STOCKIST_DB",
		"DB_PORT", "DB_HOST", "DB_USERNAME", "DB_PASSWORD"}
	cfg.ReadGcsFile("gs://awacs_config/cloud_function_config.yml")
	m := cfg.ReadEnv(envVerables)

	//Over write config file variables if enviroment variable is set
	cfg.MapEnv(m)

	fiveMB = 5 * 1024 * 1024
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
	log.Info("Starting go routines")
	for _, sub := range awacsSubscriptions {
		go func(sub *pubsub.Subscription) {
			// Receive blocks until the context is cancelled or an error occurs.
			err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				cm <- msg
			})
			if err != nil {
				log.Error("Subscription error := ", err)
			}
		}(sub)
	}
	log.Info("Starting go Message reader")
	for msg := range cm {
		guard <- struct{}{} // would block if guard channel is already filled
		go func(ctx context.Context, msg pubsub.Message) {
			time.Sleep(5 * time.Millisecond)
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

	bucketSize, err := strconv.ParseInt(bucketDetails.Size, 10, 64)
	if err == nil && bucketSize > fiveMB {
		msg.Ack()
	}
	var mu sync.Mutex
	mu.Lock()
	g := *gcsFileAttr.HandleGCSEvent(ctx, e)
	if g.GcsClient.GetLastStatus() == false {
		return
	}

	mu.Unlock()

	switch bucketDetails.Bucket {
	case "awacssmartstock":
		var stockObj functions.StockAttr
		stockObj.StockCloudFunction(g, cfg)
	case "awacssmartinvoice":
		if bucketSize <= 511 {
			g.GcsClient.MoveObject(g.FileName, g.FileName, "awacsportedinvoice")
			g.LogFileDetails(true)
			return
		}
		var invoiceAttr functions.InvoiceAttr
		invoiceAttr.InvoiceCloudFunction(g, cfg)
	case "awacssmartcustomermaster":
		var customerMasterAttar functions.CustomerMasterAttar
		customerMasterAttar.CustomerMasterCloudFunction(g, cfg)
	case "awacssmartproductmaster":
		var ProductmasterAttar functions.ProductMasterAttar
		ProductmasterAttar.ProductMasterCloudFunction(g, cfg)
	case "awacssmartoutstanding":
		var OutstandingAttar functions.OutstandingAttar
		OutstandingAttar.OutstandingCloudFunction(g, cfg)
	}
}
