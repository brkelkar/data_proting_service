package utils

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
	"errors"
)

//WriteToSyncService sends records to syncUpload service
func WriteToSyncService(URLPath string, payloadVal []interface{}, batchSize int) (err error) {
	totalRecordCount := len(payloadVal)
	if totalRecordCount <= batchSize {
		err=CallToSyncService(URLPath, payloadVal)
		if err != nil {
			return err
		}
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

			err = CallToSyncService(URLPath, updateBatch)
			if err != nil {
				return err
			}
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
	var resp *http.Response
	for i := 1; i <= 7; i++ {
		resp, err = http.Post(URLPath, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("Failed to Connect by Sync service status := %v, Retry %v \n", err, i)
			time.Sleep(25 * time.Second)
			continue
		}

		if resp.Status != "200 OK" {
			log.Printf("Failed to write by Sync service status := %v, for URI=%v, Retry=%v\n",resp.Status,URLPath,i)
			err= errors.New(resp.Status)
			resp.Body.Close()
			time.Sleep(25 * time.Second)
			continue
		} else {
			resp.Body.Close()
			return nil
		}
	}

	return err
}
