package utils

import (
	"bytes"
	"log"
	"net/http"
	"time"
)

//WriteToSyncService sends records to syncUpload service
func WriteToSyncService(URLPath string, payload []byte) (err error) {
	for i := 1; i <= 5; i++ {
		resp, err := http.Post(URLPath, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Println(err)
			resp.Body.Close()
			time.Sleep(60 * time.Second)
			continue
		}

		if resp.Status != "200 OK" {
			log.Println("Failed to write by Sync service status := " + resp.Status)
			resp.Body.Close()
			time.Sleep(60 * time.Second)
		} else if resp.Status == "200 OK" {
			resp.Body.Close()
			return nil
		}
	}
	return nil
}
