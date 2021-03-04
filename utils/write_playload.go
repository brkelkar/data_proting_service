package utils

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

//WriteToSyncService sends records to syncUpload service
func WriteToSyncService(URLPath string, payloadVal []interface{}, batchSize int) (err error) {
	totalRecordCount := len(payloadVal)
	if totalRecordCount <= batchSize {
		CallToSyncService(URLPath, payloadVal)
	} else {
		remainingRecords := totalRecordCount
		updateRecordLastIndex := batchSize
		startIndex := 0

		for {
			if remainingRecords < 1 {
				break
			}
			log.Printf("Batch Size Index:=  %v := %v\n", startIndex, updateRecordLastIndex)
			updateBatch := payloadVal[startIndex:updateRecordLastIndex]

			CallToSyncService(URLPath, updateBatch)

			remainingRecords = remainingRecords - batchSize
			startIndex = updateRecordLastIndex
			if remainingRecords < batchSize {
				updateRecordLastIndex = updateRecordLastIndex + remainingRecords
			} else {
				updateRecordLastIndex = updateRecordLastIndex + batchSize
			}
		}
	}
	return nil
}

//CallToSyncService sends records to syncUpload service
func CallToSyncService(URLPath string, payloadVal []interface{}) (err error) {
	payload, _ := json.Marshal(payloadVal)
	for i := 1; i <= 5; i++ {
		resp, err := http.Post(URLPath, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Println(err)
			log.Printf("Retry: %v\n", i)
			time.Sleep(10 * time.Second)			
			continue
		}

		if resp.Status != "200 OK" {
			log.Println("Failed to write by Sync service status := " + resp.Status)
			resp.Body.Close()
			log.Printf("Retry: %v\n", i)
			time.Sleep(10 * time.Second)			
		} else {
			resp.Body.Close()
			return nil
		}
	}

	return nil
}
