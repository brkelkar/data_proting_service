package functions

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"data_porting_service/models"
	"data_porting_service/utils"

	cr "github.com/brkelkar/common_utils/configreader"
)

//CustomerMasterAttar as model
type CustomerMasterAttar struct {
	cAttar CommonAttr
}

func (o *CustomerMasterAttar) initCustomerMaster(cfg cr.Config) {
	o.cAttar.colMap = make(map[string]int)
	o.cAttar.colName = []string{"CODE", "COMPANIONCODE", "NAME", "ADDRESS1", "ADDRESS2", "ADDRESS3", "CITY",
		"STATE", "AREA", "PINCODE", "KEYPERSON", "CELL", "PHONE", "EMAIL", "DRUGLIC1", "DRUGLIC2", "DRUGLIC3",
		"DRUGLIC4", "DRUGLIC5", "DRUGLIC6", "GSTIN", "PAN", "SALESMANCODE", "ISLOCKED", "ISLOCKEDBILLING", "ALLOWDELIVERY"}

	for _, val := range o.cAttar.colName {
		o.cAttar.colMap[val] = -1
	}

	apiPath = "/api/customermaster"
	URLPath = utils.GetHostURL(cfg) + apiPath
}

//CustomerMasterCloudFunction used to load outstanding file to database
func (o *CustomerMasterAttar) CustomerMasterCloudFunction(g utils.GcsFile, cfg cr.Config) (err error) {
	startTime := time.Now()
	log.Printf("Starting customer master file upload for :%v/%v ", g.FilePath, g.FileName)
	o.initCustomerMaster(cfg)
	g.FileType = "C"
	var reader *bufio.Reader
	reader = bufio.NewReader(g.GcsClient.GetReader())
	flag := 1
	var Customermaster []models.CustomerMaster
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			g.ErrorMsg = "Error while reading file"
			g.LogFileDetails(false)
			return err
		}
		var tempCustomermaster models.CustomerMaster
		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, "|")
		for i, val := range lineSlice {
			if flag == 1 {
				o.cAttar.colMap[strings.ToUpper(val)] = i
			} else {
				switch i {
				case -1:
					break
				case o.cAttar.colMap["CODE"]:
					tempCustomermaster.Code = val
				case o.cAttar.colMap["COMPANIONCODE"]:
					tempCustomermaster.CompanionCode = val
				case o.cAttar.colMap["NAME"]:
					tempCustomermaster.Name = val
				case o.cAttar.colMap["ADDRESS1"]:
					tempCustomermaster.Address1 = val
				case o.cAttar.colMap["ADDRESS2"]:
					tempCustomermaster.Address2 = val
				case o.cAttar.colMap["ADDRESS3"]:
					tempCustomermaster.Address3 = val
				case o.cAttar.colMap["CITY"]:
					tempCustomermaster.City = val
				case o.cAttar.colMap["STATE"]:
					tempCustomermaster.State = val
				case o.cAttar.colMap["AREA"]:
					tempCustomermaster.Area = val
				case o.cAttar.colMap["PINCODE"]:
					tempCustomermaster.Pincode = val
				case o.cAttar.colMap["KEYPERSON"]:
					tempCustomermaster.KeyPerson = val
				case o.cAttar.colMap["CELL"]:
					tempCustomermaster.Cell = val
				case o.cAttar.colMap["PHONE"]:
					tempCustomermaster.Phone = val
				case o.cAttar.colMap["EMAIL"]:
					tempCustomermaster.Email = val
				case o.cAttar.colMap["DRUGLIC1"]:
					tempCustomermaster.DrugLic1 = val
				case o.cAttar.colMap["DRUGLIC2"]:
					tempCustomermaster.DrugLic2 = val
				case o.cAttar.colMap["DRUGLIC3"]:
					tempCustomermaster.DrugLic3 = val
				case o.cAttar.colMap["DRUGLIC4"]:
					tempCustomermaster.DrugLic4 = val
				case o.cAttar.colMap["DRUGLIC5"]:
					tempCustomermaster.DrugLic5 = val
				case o.cAttar.colMap["DRUGLIC6"]:
					tempCustomermaster.DrugLic6 = val
				case o.cAttar.colMap["GSTIN"]:
					tempCustomermaster.GSTIN = val
				case o.cAttar.colMap["PAN"]:
					tempCustomermaster.PAN = val
				case o.cAttar.colMap["SALESMANCODE"]:
					tempCustomermaster.SalesmanCode = val
				case o.cAttar.colMap["ISLOCKED"]:
					tempCustomermaster.IsLocked = val
				case o.cAttar.colMap["ISLOCKEDBILLING"]:
					tempCustomermaster.IsLockedBilling = val
				case o.cAttar.colMap["ALLOWDELIVERY"]:
					tempCustomermaster.AllowDelivery = val
				}
			}
		}
		tempCustomermaster.UserId = g.DistributorCode
		if flag == 0 {
			Customermaster = append(Customermaster, tempCustomermaster)
		}
		flag = 0

		if err == io.EOF {
			break
		}
	}
	recordCount := len(Customermaster)
	if recordCount > 0 {
		jsonValue, _ := json.Marshal(Customermaster)
		err = utils.WriteToSyncService(URLPath, jsonValue)
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
	g.GcsClient.MoveObject(g.FileName, "ported/customermaster/"+g.FileName, "balatestawacs")
	log.Println("Porting Done :" + g.FileName)
	mu.Unlock()
	g.TimeDiffrence = int64(time.Now().Sub(startTime) / 1000000)
	g.Records = recordCount
	g.LogFileDetails(true)
	return
}
