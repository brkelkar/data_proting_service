package utils

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/http"
)

//WriteToSyncService sends records to syncUpload service
func WriteToSyncService(URLPath string, payload []byte) (err error) {
	fmt.Println(URLPath)
	resp, err := http.Post(URLPath, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	if resp.Status != "200 OK" {
		return errors.New("Failed to write by Sync service status := " + resp.Status)
	}
	return nil
}
