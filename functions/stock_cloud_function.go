package functions

import (
	"bufio"
	"data_porting_service/models"
	"data_porting_service/utils"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	cr "github.com/brkelkar/common_utils/configreader"
)

//StockAttr used for update Stock file in database
type StockAttr struct {
	cAttr CommonAttr
}

func (s *StockAttr) stockInit(cfg cr.Config) {
	s.cAttr.colMap = make(map[string]int)
	s.cAttr.colName = []string{"USERID", "PRODUCTCODE", "CLOSING"}

	for _, val := range s.cAttr.colName {
		s.cAttr.colMap[val] = -1
	}
	apiPath = "/api/stocks"
	URLPath = utils.GetHostURL(cfg) + apiPath
}

//StockCloudFunction used to load stock file to database
func (s *StockAttr) StockCloudFunction(g utils.GcsFile, cfg cr.Config) (err error) {
	startTime := time.Now()
	log.Printf("Starting stock file upload for :%v", g.FilePath)
	s.stockInit(cfg)
	g.FileType = "S"
	r := g.GcsClient.GetReader()
	if g.GcsClient.GetLastStatus() == false {
		return
	}
	
	var reader *bufio.Reader
	reader = bufio.NewReader(r)
	if reader == nil {
		return
	}
	flag := 1
	var stock []interface{}
	productMap := make(map[string]models.Stocks)

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			fmt.Println(err)
		}
		var tempStock models.Stocks
		var strproductCode string
		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, "|")
		for i, val := range lineSlice {
			if flag == 1 {
				s.cAttr.colMap[strings.ToUpper(val)] = i
			} else {
				switch i {
				case -1:
					break
				case s.cAttr.colMap["PRODUCTCODE"]:
					strproductCode = val
					tempStock.ProductCode = val
				case s.cAttr.colMap["CLOSING"]:
					if s, err := strconv.ParseFloat(val, 64); err == nil {
						tempStock.Closing = s
					}
				}
				tempStock.UserId = g.DistributorCode
			}
		}

		if flag == 0 {
			val, ok := productMap[strproductCode]
			if ok == true {
				val.Closing = val.Closing + tempStock.Closing
				productMap[strproductCode] = val
			} else {
				productMap[strproductCode] = tempStock
			}
		}
		flag = 0

		if err == io.EOF {
			break
		}
	}

	for _, val := range productMap {
		stock = append(stock, val)
	}
	recordCount := len(stock)
	if recordCount > 0 {
		err = utils.WriteToSyncService(URLPath, stock, 20000)
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

	return nil
}
