package functions

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"data_porting_service/models"
	"data_porting_service/utils"

	cr "github.com/brkelkar/common_utils/configreader"
)

//OutstandingAttar as model
type OutstandingAttar struct {
	cAttar CommonAttr
}

func (o *OutstandingAttar) initOutstanding(cfg cr.Config) {
	o.cAttar.colMap = make(map[string]int)
	o.cAttar.colName = []string{"CUSTOMERCODE", "DOCUMENTNUMBER", "DOCUMENTDATE", "AMOUNT", "ADJUSTEDAMOUNT", "PENDINGAMOUNT", "DUEDATE"}

	for _, val := range o.cAttar.colName {
		o.cAttar.colMap[val] = -1
	}

	apiPath = "/api/outstanding"
	URLPath = utils.GetHostURL(cfg) + apiPath
}

//OutstandingCloudFunction used to load outstanding file to database
func (o *OutstandingAttar) OutstandingCloudFunction(g utils.GcsFile, cfg cr.Config) (err error) {
	startTime := time.Now()
	log.Printf("Starting outstanding file upload for :%v/%v ", g.FilePath, g.FileName)
	o.initOutstanding(cfg)
	g.FileType = "O"

	var reader *bufio.Reader
	reader = bufio.NewReader(g.GcsClient.GetReader())
	flag := 1
	var Outstanding []models.Outstanding
	var OutstandingPayload []interface{}

	for {
		//fileRow, err := reader.Read()
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			g.ErrorMsg = "Error while reading file"
			g.LogFileDetails(false)
			return err
		}
		var tempOutstanding models.Outstanding
		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, "|")
		for i, val := range lineSlice {
			if flag == 1 {
				o.cAttar.colMap[strings.ToUpper(val)] = i
			} else {
				switch i {
				case -1:
					break
				case o.cAttar.colMap["CUSTOMERCODE"]:
					tempOutstanding.CustomerCode = val
				case o.cAttar.colMap["DOCUMENTNUMBER"]:
					tempOutstanding.DocumentNumber = val
				case o.cAttar.colMap["DOCUMENTDATE"]:
					tempOutstanding.DocumentDate = val
				case o.cAttar.colMap["AMOUNT"]:
					tempOutstanding.Amount, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["ADJUSTEDAMOUNT"]:
					tempOutstanding.AdjustedAmount, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["PENDINGAMOUNT"]:
					tempOutstanding.PendingAmount, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["DUEDATE"]:
					tempOutstanding.DueDate = val
				}
			}
		}
		tempOutstanding.UserId = g.DistributorCode
		if flag == 0 {
			Outstanding = append(Outstanding, tempOutstanding)
		}
		flag = 0

		if err == io.EOF {
			break
		}
	}

	outstandingMap := make(map[string]models.CustomerOutstanding)
	for _, val := range Outstanding {
		key := val.UserId + val.CustomerCode
		if _, ok := outstandingMap[key]; !ok {
			var tout models.CustomerOutstanding
			tout.OutstandingJson = GetJsonstring(val) //`"[{"CustomerCode":"` + val.CustomerCode + `","DocumentNumber":"` + val.DocumentNumber + `","DocumentDate":"` + val.DocumentDate + `","Amount":"` + fmt.Sprintf("%f", val.Amount) + `","PendingAmount":"` + fmt.Sprintf("%f", val.PendingAmount) + `","AdjustedAmount":"` + fmt.Sprintf("%f", val.AdjustedAmount) + `","DueDate":"` + val.DueDate + `"}"`
			tout.Outstanding = val.PendingAmount
			tout.UserId = val.UserId
			tout.CustomerCode = val.CustomerCode
			tout.LastUpdated = time.Now()

			outstandingMap[key] = tout
		} else {
			t, _ := outstandingMap[key]
			t.OutstandingJson = t.OutstandingJson + "," + GetJsonstring(val) //`",{"CustomerCode":"` + val.CustomerCode + `","DocumentNumber":"` + val.DocumentNumber + `","DocumentDate":"` + val.DocumentDate + `","Amount":"` + fmt.Sprintf("%f", val.Amount) + `","PendingAmount":"` + fmt.Sprintf("%f", val.PendingAmount) + `","AdjustedAmount":"` + fmt.Sprintf("%f", val.AdjustedAmount) + `","DueDate":"` + val.DueDate + `"}]"`
			t.Outstanding = t.Outstanding + val.PendingAmount
			t.UserId = val.UserId
			t.CustomerCode = val.CustomerCode
			t.LastUpdated = time.Now()

			outstandingMap[key] = t
		}
	}

	var customerOutstanding []models.CustomerOutstanding
	for _, val := range outstandingMap {
		val.OutstandingJson = "[" + val.OutstandingJson + "]"
		customerOutstanding = append(customerOutstanding, val)
	}

	for _, val := range customerOutstanding {
		OutstandingPayload = append(OutstandingPayload, val)
	}

	recordCount := len(OutstandingPayload)
	if recordCount > 0 {
		err = utils.WriteToSyncService(URLPath, OutstandingPayload,20000)
		if err != nil {
			log.Print(err)
			g.GcsClient.MoveObject(g.FileName, "error_Files/"+g.FileName, "balatestawacs")
			log.Println("Porting Error :" + g.FileName)
			g.LogFileDetails(false)
			return err
		}
	}
	var mu sync.Mutex
	mu.Lock()
	// If either of the loading is successful move file to ported
	g.GcsClient.MoveObject(g.FileName, "ported/"+g.FileName, "balatestawacs")
	log.Println("Porting Done :" + g.FileName)
	mu.Unlock()
	g.TimeDiffrence = int64(time.Now().Sub(startTime) / 1000000)
	g.Records = recordCount
	g.LogFileDetails(true)
	return
}

//GetJsonstring concat json string
func GetJsonstring(outstanding models.Outstanding) (jsonString string) {
	jsonString = `{"CustomerCode":"` + outstanding.CustomerCode + `","DocumentNumber":"` +
		outstanding.DocumentNumber + `","DocumentDate":"` + outstanding.DocumentDate + `","Amount":"` +
		fmt.Sprintf("%f", outstanding.Amount) + `","PendingAmount":"` + fmt.Sprintf("%f", outstanding.PendingAmount) +
		`","AdjustedAmount":"` + fmt.Sprintf("%f", outstanding.AdjustedAmount) + `","DueDate":"` + outstanding.DueDate + `"}`
	return
}
