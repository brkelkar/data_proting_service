package functions

import (
	"bufio"
	"data_porting_service/models"
	"data_porting_service/utils"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	cr "github.com/brkelkar/common_utils/configreader"
)

//ProductMasterAttar as model
type ProductMasterAttar struct {
	cAttar CommonAttr
}

func (o *ProductMasterAttar) initProductMaster(cfg cr.Config) {
	o.cAttar.colMap = make(map[string]int)
	o.cAttar.colName = []string{"USERID", "UPC", "PRODUCTCODE", "CODE", "FAVCODE",
		"PRODUCTNAME", "NAME", "BOXPACK", "CASEPACK", "PRODUCTPACK", "PACK", "COMPANYNAME", "COMPANYCODE",
		"COMPANYCODE", "COMPANYNAME", "COMPANY", "DIVISIONCODE", "DIVISION", "DIVISIONNAME",
		"PRODUCTCATEGORY", "CATEGORY", "PTS", "PTR", "MRP", "HSN",
		"CONTENT", "ISACTIVE", "LASTUPDATEDTIME", "CLOSING", "MINDISCOUNT", "MAXDISCOUNT", "ISLOCKED"}

	for _, val := range o.cAttar.colName {
		o.cAttar.colMap[val] = -1
	}

	apiPath = "/api/productmaster"
	URLPath = utils.GetHostURL(cfg) + apiPath
}

//ProductMasterCloudFunction used to load outstanding file to database
func (o *ProductMasterAttar) ProductMasterCloudFunction(g utils.GcsFile, cfg cr.Config) (err error) {
	startTime := time.Now()
	log.Printf("Starting product master file upload for :%v/%v ", g.FilePath, g.FileName)
	o.initProductMaster(cfg)
	g.FileType = "P"
	r := g.GcsClient.GetReader()
	if !g.GcsClient.GetLastStatus() {
		return
	}
	reader := bufio.NewReader(r)
	if reader == nil {
		return
	}
	flag := 1
	var Productmaster []interface{}

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			g.ErrorMsg = "Error while reading file"
			g.LogFileDetails(false)
			return err
		}
		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, "|")
		var tempProductmaster models.ProductMaster
		for i, val := range lineSlice {
			if flag == 1 {
				o.cAttar.colMap[strings.ToUpper(val)] = i
			} else {
				switch i {
				case -1:
					break
				case o.cAttar.colMap["USERID"]:
					tempProductmaster.UserId = val
				case o.cAttar.colMap["UPC"]:
					tempProductmaster.UPC = val
				case o.cAttar.colMap["CODE"], o.cAttar.colMap["PRODUCTCODE"]:
					tempProductmaster.Code = val
				case o.cAttar.colMap["FAVCODE"]:
					tempProductmaster.FavCode = val
				case o.cAttar.colMap["NAME"], o.cAttar.colMap["PRODUCTNAME"]:
					tempProductmaster.Name = val
				case o.cAttar.colMap["BOXPACK"]:
					tempProductmaster.BoxPack = val
				case o.cAttar.colMap["CASEPACK"]:
					tempProductmaster.CasePack = val
				case o.cAttar.colMap["PACK"], o.cAttar.colMap["PRODUCTPACK"]:
					tempProductmaster.Pack = val
				case o.cAttar.colMap["COMPANYCODE"]:
					if len(val) > 21 {
						val = val[0:21]
					}
					tempProductmaster.CompanyCode = val
				case o.cAttar.colMap["COMPANY"], o.cAttar.colMap["COMPANYNAME"]:
					tempProductmaster.CompanyName = val
				case o.cAttar.colMap["DIVISIONCODE"]:
					tempProductmaster.DivisionCode = val
				case o.cAttar.colMap["DIVISION"], o.cAttar.colMap["DIVISIONNAME"]:
					tempProductmaster.DivisionName = val
				case o.cAttar.colMap["CATEGORY"], o.cAttar.colMap["PRODUCTCATEGORY"]:
					tempProductmaster.Category = val
				case o.cAttar.colMap["PTS"]:
					tempProductmaster.PTS, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["PTR"]:
					tempProductmaster.PTR, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["MRP"]:
					tempProductmaster.MRP, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["HSN"]:
					tempProductmaster.HSN = val
				case o.cAttar.colMap["CONTENT"]:
					tempProductmaster.Content = val
				case o.cAttar.colMap["ISACTIVE"]:
					tempProductmaster.IsActive, _ = strconv.ParseBool(val)
				case o.cAttar.colMap["LASTUPDATEDTIME"]:
					tempProductmaster.LastUpdatedTime, _ = utils.ConvertDate(val)
				case o.cAttar.colMap["CLOSING"]:
					tempProductmaster.Closing, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["MINDISCOUNT"]:
					tempProductmaster.MinDiscount, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["MAXDISCOUNT"]:
					tempProductmaster.MaxDiscount, _ = strconv.ParseFloat(val, 64)
				case o.cAttar.colMap["ISLOCKED"]:
					tempProductmaster.IsLocked, _ = strconv.ParseBool(val)
				}
			}
		}
		tempProductmaster.UserId = g.DistributorCode
		if flag == 0 {
			Productmaster = append(Productmaster, tempProductmaster)
		}
		flag = 0

		if err == io.EOF {
			break
		}
	}
	recordCount := len(Productmaster)

	if recordCount > 0 {
		//jsonValue, _ := json.Marshal(Productmaster)
		err = utils.WriteToSyncService(URLPath, Productmaster, 20000)
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
	g.GcsClient.MoveObject(g.FileName, "ported/productmaster/"+g.FileName, "balatestawacs")
	log.Println("Porting Done :" + g.FileName)
	g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
	mu.Unlock()
	g.Records = recordCount
	g.LogFileDetails(true)
	return
}
