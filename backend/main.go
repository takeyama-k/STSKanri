package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/xuri/excelize/v2"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

var es_sts_idx string
var lockFolderPath string
var stsFilePath string
var sts75FilePath string
var igsFolderPath string
var stsLinkFileName string
var stsLockFileName string
var sts75ListLinkFileName string
var sts75ListLockFileName string
var igsLinkFileName string
var igsLockFileName string
var igsFileName string
var igsBLNOFileName string
var gatewayPath string
var gatewayHtml string
var deleteIndiciesfrom string
var Tp http.Transport

type Timeline struct {
	Time  time.Time `json:"time"`
	Index int       `json:"index"`
}

type TimelineResponce struct {
	Timeline []Timeline `json:"timeline"`
}

type StatusResponse struct {
	Status []Status `json:"status"`
}

type Status struct {
	Index             int       `json:"index"`
	Awbno             string    `json:"awbno"`
	BaseTime          time.Time `json:"base_time"`
	TimespanInMinutes int       `json:"time_span"`
	Q                 int       `json:"q"`
	StatusCode        string    `json:"status_code"`
	SectionCode       string    `json:"section_code"`
	CompanyCode       string    `json:"company_code"`
	CompanyName       string    `json:"company_name"`
	LastUserName      string    `json:"last_updated_user"`
	LastUserId        string    `json:"last_updated_id"`
	IsStocked         bool      `json:"is_stocked"`
	IgsStatus         string    `json:"igs_status"`
}

type AwbStatus struct {
	Awbno        string    `json:"awbno"`
	UpdateTime   time.Time `json:"update_time"`
	StatusCode   string    `json:"status_code"`
	SectionCode  string    `json:"section_code"`
	CompanyCode  string    `json:"company_code"`
	CompanyName  string    `json:"company_name"`
	LastUserName string    `json:"last_updated_user"`
	LastUserId   string    `json:"last_updated_id"`
}

type AWBResponce struct {
	TtlAwbs int      `json:"ttl"`
	Awbno   []string `json:"awbnos"`
}

type STSResult struct {
	Result []AwbStatus
	Error  error
}

type UserResponce struct {
	User []string `json:"users"`
}

type StslistResponce struct {
	StatusCode []string `json:"statuscodes"`
}

type Metrics struct {
	SakuAccumMins float64 `json:"sakuttl"`
	SakuAccumCnts int64   `json:"sakucnt"`
	ShinAccumMins float64 `json:"shinttl"`
	ShinAccumCnts int64   `json:"shincnt"`
}

type DeadorAlive struct {
	LastStsUpdated float64 `json:"laststsupdated"`
	LastIgsUpdated float64 `json:"lastigsupdated"`
	DeadorAlive    string  `json:"status"`
}

func main() {
	Tp = http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 100,
	}
	if err := Init(); err != nil {
		log.Fatalf("%v", err)
	}
	STS := make(map[string]AwbStatus)
	SakuBlackList := make(map[string]bool)
	ShinBlackList := make(map[string]bool)
	Metrics := Metrics{SakuAccumMins: 0, SakuAccumCnts: 0, ShinAccumMins: 0, ShinAccumCnts: 0}
	DeadorAlive := DeadorAlive{LastStsUpdated: float64(time.Now().Local().UnixMilli()), LastIgsUpdated: float64(time.Now().Local().UnixMilli()), DeadorAlive: `Fine`}
	resStss, err := readFiles()
	if err != nil {
		log.Fatalf("%s", err)
	}
	e := echo.New()
	e.Use(middleware.CORS())
	e.Static("/", "public/")
	e.GET("/api/status", statusApi)
	e.GET("/api/awb", apiFactory(awbApi, &STS))
	e.GET("/api/user", apiFactory(userApi, &STS))
	e.GET("/api/stslist", apiFactory(stslistApi, &STS))
	e.GET("/api/timeline", timeLineApi)
	e.GET("/api/metrics", metApiFactory(metricsApi, &Metrics))
	e.GET("/api/deadoralive", deadApiFactory(deadoraliveApi, &DeadorAlive))

	go func() {
		for {
			ressts := <-resStss
			if ressts.Result != nil {
				DeadorAlive.LastStsUpdated = float64(time.Now().UnixMilli())
				for k := range STS {
					delete(STS, k)
				}
				prevawb := ""
				if ressts.Result != nil && len(ressts.Result) > 0 {
					prevawb = ressts.Result[0].Awbno
					prevstatus := ressts.Result[0]
					for _, status := range ressts.Result {
						if prevawb != status.Awbno {
							STS[prevawb] = prevstatus
						}
						prevawb = status.Awbno
						prevstatus = status
					}
					STS[prevawb] = prevstatus
				}
			} else {
				if ressts.Error != nil {
					log.Fatalf("%s", ressts.Error)
				} else {
					DeadorAlive.LastIgsUpdated = float64(time.Now().UnixMilli())
				}
			}

			//calculate metrics
			survayAwbs := make([]string, 0, 100)
			for _, status := range ressts.Result {
				if SakuBlackList[status.Awbno] {
					continue
				}
				if status.StatusCode < "70" {
					continue
				} else {
					survayAwbs = append(survayAwbs, status.Awbno)
				}
			}
			durs, err := getDurations(survayAwbs, "50", "70")
			if err != nil {
				log.Printf("%s", err)
			}
			for _, dur := range durs {
				if dur != 0 {
					Metrics.SakuAccumCnts++
					Metrics.SakuAccumMins += dur
				}
			}
			for _, awb := range survayAwbs {
				SakuBlackList[awb] = true
			}

			survayAwbs = make([]string, 0, 100)
			for _, status := range ressts.Result {
				if ShinBlackList[status.Awbno] {
					continue
				}
				if status.StatusCode < "72" {
					continue
				} else {
					survayAwbs = append(survayAwbs, status.Awbno)
				}
			}
			durs, err = getDurations(survayAwbs, "70", "72")
			if err != nil {
				log.Printf("%s", err)
			}
			for _, dur := range durs {
				if dur != 0 {
					Metrics.ShinAccumCnts++
					Metrics.ShinAccumMins += dur
				}
			}
			for _, awb := range survayAwbs {
				ShinBlackList[awb] = true
			}
		}
	}()
	e.Logger.Debug(e.Start(":8080"))
}

func getDurations(awbs []string, gte, lt string) ([]float64, error) {
	from := time.Now().UnixMilli() / (24 * 60 * 60 * 1000) * (24 * 60 * 60 * 1000)
	to := time.Now().UnixMilli()
	var r map[string]interface{}
	muniteUnit := int64(60 * 1000)
	result := make([]float64, 0, 100)

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Transport: &Tp,
	}

	es7, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	for _, awb := range awbs {
		qbody := strings.NewReader(`{"sort": [{"update_time": {"order": "asc"}}],"query":{"bool":{"must": [{"term":{"awb_no": {"value": "` + awb + `"}}},{"range":{"update_time": {"gte":` + strconv.FormatInt(from, 10) + `,"lt":` + strconv.FormatInt(to, 10) + `}}}]}}}`)
		size := 1000
		req := esapi.SearchRequest{
			Index: []string{es_sts_idx},
			Body:  qbody,
			Size:  &size,
		}
		res, err := req.Do(context.Background(), es7.Transport)
		if err != nil {
			log.Printf("%v", err)
		}
		defer func() {
			io.Copy(ioutil.Discard, res.Body)
			res.Body.Close()
		}()
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		}
		hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
		isOK := false
		contidx := 0
		for idx, hit := range hits {
			data := hit.(map[string]interface{})["_source"]
			sts_code, _ := data.(map[string]interface{})["sts_code"].(string)
			if sts_code < gte {
				isOK = true
				contidx = idx + 1
				break
			}
		}
		if !isOK {
			result = append(result, 0)
			continue
		}
		isOK = false
		startidx := 0
		for idx := contidx; idx < len(hits); idx++ {
			data := hits[idx].(map[string]interface{})["_source"]
			sts_code, _ := data.(map[string]interface{})["sts_code"].(string)
			if sts_code >= gte {
				isOK = true
				startidx = idx
				contidx = idx + 1
				break
			}
		}
		if !isOK {
			result = append(result, 0)
			continue
		}
		isOK = false
		endidx := 0
		for idx := contidx; idx < len(hits); idx++ {
			data := hits[idx].(map[string]interface{})["_source"]
			sts_code, _ := data.(map[string]interface{})["sts_code"].(string)
			if sts_code >= lt {
				isOK = true
				endidx = idx
				break
			}
		}
		if !isOK {
			result = append(result, 0)
			continue
		}
		start := hits[startidx].(map[string]interface{})["_source"]
		starttime := start.(map[string]interface{})["update_time"].(float64)
		end := hits[endidx].(map[string]interface{})["_source"]
		endtime := end.(map[string]interface{})["update_time"].(float64)
		dur := endtime - starttime
		result = append(result, dur/float64(muniteUnit))
	}
	return result, nil
}

func deadoraliveApi(c echo.Context, dead *DeadorAlive) error {
	var duration float64
	duration = float64(time.Now().UnixMilli()) - dead.LastIgsUpdated
	if float64(time.Now().UnixMilli())-dead.LastStsUpdated > duration {
		duration = float64(time.Now().UnixMilli()) - dead.LastStsUpdated
	}
	tempDead := DeadorAlive{LastStsUpdated: dead.LastStsUpdated, LastIgsUpdated: dead.LastIgsUpdated}
	if duration > float64(10*60*1000) {
		tempDead.DeadorAlive = `Dead`
	} else {
		tempDead.DeadorAlive = `Fine`
	}
	return c.JSON(http.StatusOK, tempDead)
}

func metricsApi(c echo.Context, met *Metrics) error {
	return c.JSON(http.StatusOK, *met)
}
func deadApiFactory(fn func(echo.Context, *DeadorAlive) error, dead *DeadorAlive) echo.HandlerFunc {
	return func(c echo.Context) error {
		fn(c, dead)
		return nil
	}
}

func metApiFactory(fn func(echo.Context, *Metrics) error, met *Metrics) echo.HandlerFunc {
	return func(c echo.Context) error {
		fn(c, met)
		return nil
	}
}

func apiFactory(fn func(echo.Context, *map[string]AwbStatus) error, awbs *map[string]AwbStatus) echo.HandlerFunc {
	return func(c echo.Context) error {
		fn(c, awbs)
		return nil
	}
}

func stslistApi(c echo.Context, awbs *map[string]AwbStatus) error {
	stsTable := make(map[string]bool)
	for _, value := range *awbs {
		stsTable[value.StatusCode] = true
	}

	result := StslistResponce{StatusCode: make([]string, 0, 100)}
	for s, _ := range stsTable {
		result.StatusCode = append(result.StatusCode, s)
	}
	sort.SliceStable(result.StatusCode, func(i, j int) bool { return result.StatusCode[i] < result.StatusCode[j] })
	return c.JSON(http.StatusOK, result)
}

func userApi(c echo.Context, awbs *map[string]AwbStatus) error {
	userTable := make(map[string]bool)
	for _, value := range *awbs {
		if value.LastUserName != "" {
			if c.QueryParam("status") == "" {
				userTable[value.LastUserName] = true
			} else if value.StatusCode == c.QueryParam("status") {
				userTable[value.LastUserName] = true
			}
		}
	}
	result := UserResponce{User: make([]string, 0, 100)}
	for u, _ := range userTable {
		result.User = append(result.User, u)
	}
	return c.JSON(http.StatusOK, result)
}

func awbApi(c echo.Context, awbs *map[string]AwbStatus) error {
	result := AWBResponce{Awbno: make([]string, 0, 100), TtlAwbs: 0}
	values := make([]AwbStatus, 0, 100)
	for _, value := range *awbs {
		values = append(values, value)
	}
	sort.SliceStable(values, func(i, j int) bool { return values[i].Awbno < values[j].Awbno })
	if c.QueryParam("sort") != "" {
		switch c.QueryParam("sort") {
		case "awbno":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool { return values[i].Awbno < values[j].Awbno })
			} else {
				sort.SliceStable(values, func(i, j int) bool { return values[i].Awbno > values[j].Awbno })
			}
		case "update_user_id":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool { return values[i].LastUserId < values[j].LastUserId })
			} else {
				sort.SliceStable(values, func(i, j int) bool { return values[i].LastUserId > values[j].LastUserId })
			}
		case "update_user_name":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool { return values[i].LastUserName < values[j].LastUserName })
			} else {
				sort.SliceStable(values, func(i, j int) bool { return values[i].LastUserName > values[j].LastUserName })
			}
		case "last_updated":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool {
					if values[i].UpdateTime.Before(values[j].UpdateTime) {
						return true
					} else {
						return false
					}
				})
			} else {
				sort.SliceStable(values, func(i, j int) bool {
					if values[i].UpdateTime.After(values[j].UpdateTime) {
						return true
					} else {
						return false
					}
				})
			}
		case "status":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool { return values[i].StatusCode < values[j].StatusCode })
			} else {
				sort.SliceStable(values, func(i, j int) bool { return values[i].StatusCode > values[j].StatusCode })
			}
		case "company_name":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool { return values[i].CompanyName < values[j].CompanyName })
			} else {
				sort.SliceStable(values, func(i, j int) bool { return values[i].CompanyName > values[j].CompanyName })
			}
		case "company_code":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool { return values[i].CompanyCode < values[j].CompanyCode })
			} else {
				sort.SliceStable(values, func(i, j int) bool { return values[i].CompanyCode > values[j].CompanyCode })
			}
		case "section_code":
			if c.QueryParam("isdesc") != "true" {
				sort.SliceStable(values, func(i, j int) bool { return values[i].SectionCode < values[j].SectionCode })
			} else {
				sort.SliceStable(values, func(i, j int) bool { return values[i].SectionCode > values[j].SectionCode })
			}
		}
	}

	f := make([]AwbStatus, 0, 100)
	if c.QueryParam("user") != "" && c.QueryParam("sts") == "" {
		for _, v := range values {
			if v.LastUserName == c.QueryParam("user") {
				f = append(f, v)
			}
		}
		values = f
	} else if c.QueryParam("user") == "" && c.QueryParam("sts") != "" {
		for _, v := range values {
			if v.StatusCode == c.QueryParam("sts") {
				f = append(f, v)
			}
		}
		values = f
	} else if c.QueryParam("user") != "" && c.QueryParam("sts") != "" {
		for _, v := range values {
			if v.StatusCode == c.QueryParam("sts") && v.LastUserName == c.QueryParam("user") {
				f = append(f, v)
			}
		}
		values = f
	}

	if c.QueryParam("isupdate") == "true" {
		var laststss []Status
		blacklist := make(map[string]bool)
		if totime, err := strconv.ParseInt(c.QueryParam("lastupdated"), 10, 64); err == nil {
			lasttime := (totime - (10 * 60 * 1000)) / (10 * 60 * 1000) * (10 * 60 * 1000)
			laststss, err = getLatestAwbs(lasttime, totime, 10)
			if err != nil {
				log.Printf("Error getting Latest Awbs: %s", err)
			}

		}
		for _, awb := range laststss {
			if c.QueryParam("user") != "" && c.QueryParam("sts") == "" {
				if awb.LastUserName == c.QueryParam("user") {
					blacklist[awb.Awbno] = true
				} else {
					blacklist[awb.Awbno] = false
				}
			} else if c.QueryParam("user") == "" && c.QueryParam("sts") != "" {
				if awb.StatusCode == c.QueryParam("sts") {
					blacklist[awb.Awbno] = true
				} else {
					blacklist[awb.Awbno] = false
				}
			} else if c.QueryParam("user") != "" && c.QueryParam("sts") != "" {
				if awb.StatusCode == c.QueryParam("sts") && awb.LastUserName == c.QueryParam("user") {
					blacklist[awb.Awbno] = true
				} else {
					blacklist[awb.Awbno] = false
				}
			} else {
				blacklist[awb.Awbno] = true
			}
		}
		tempval := make([]AwbStatus, 0, 100)
		for _, v := range values {
			if blacklist[v.Awbno] {
				continue
			}
			tempval = append(tempval, v)
		}
		values = tempval
	}

	cntr := 0
	for _, value := range values {
		result.Awbno = append(result.Awbno, value.Awbno)
		cntr++
	}
	result.TtlAwbs = cntr
	if c.QueryParam("page") != "" && c.QueryParam("par") != "" {
		page, err := strconv.Atoi(c.QueryParam("page"))
		if err != nil {
			return c.JSON(http.StatusOK, result)
		}
		par, err := strconv.Atoi(c.QueryParam("par"))
		if err != nil {
			return c.JSON(http.StatusOK, result)
		}
		from := page * par
		to := (page + 1) * par
		if from > len(result.Awbno)-1 {
			return c.JSON(http.StatusBadRequest, AWBResponce{Awbno: nil, TtlAwbs: 0})
		} else if to > len(result.Awbno) {
			to = len(result.Awbno)
		}
		result.Awbno = result.Awbno[from:to]
	}
	return c.JSON(http.StatusOK, result)
}

func timeLineApi(c echo.Context) error {
	from := c.QueryParam("from")
	to := c.QueryParam("to")
	span_str := c.QueryParam("timespan")
	hourUnit := int64(3600000)
	span := 10
	if span, err := strconv.Atoi(span_str); err != nil || span < 1 {
		return c.JSON(http.StatusBadRequest, nil)
	}
	timeSpanUnit := int64(span * 60 * 1000)
	from_i64, err := strconv.ParseInt(from, 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, nil)
	}
	to_i64, err := strconv.ParseInt(to, 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, nil)
	}
	isLatest := false
	if c.QueryParam("islatest") == "true" {
		isLatest = true
	}
	startTime := ((from_i64) / hourUnit) * hourUnit
	endtime := ((to_i64 + hourUnit - 1) / hourUnit) * hourUnit // 繰り上げ割り算
	if isLatest {
		startTime = ((from_i64) / timeSpanUnit) * timeSpanUnit
		endtime = ((to_i64 + timeSpanUnit - 1) / timeSpanUnit) * timeSpanUnit
	}
	interval := endtime - startTime
	quarters := (hourUnit-1)/int64(timeSpanUnit) + 1 // 繰り上げ割り算
	hours := interval / hourUnit
	resi := interval - hours*hourUnit
	ttlSecs := quarters*hours + (resi+timeSpanUnit-1)/timeSpanUnit

	result := TimelineResponce{Timeline: make([]Timeline, 0, 100)}
	c_time := startTime
	idx := 0
	for ttlSecs > 0 {
		result.Timeline = append(result.Timeline, Timeline{Time: time.Unix(c_time/1000, 0), Index: idx})
		c_time += timeSpanUnit
		idx++
		ttlSecs--
	}
	return c.JSON(http.StatusOK, result)
}

func statusApi(c echo.Context) error {
	awbno := c.QueryParam("key")
	from := c.QueryParam("from")
	to := c.QueryParam("to")
	from_i64, err := strconv.ParseInt(from, 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, nil)
	}
	to_i64, err := strconv.ParseInt(to, 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, nil)
	}
	if awbno == "" {
		return c.JSON(http.StatusBadRequest, nil)
	}

	isLatest := false
	isUpdate := false
	if c.QueryParam("islatest") == "true" {
		isLatest = true
	} else if c.QueryParam("isupdate") == "true" {
		isUpdate = true
	}
	awbstatus, err := getAwbStatuses(from_i64, to_i64, awbno, 10, isLatest, isUpdate)
	if err != nil {
		log.Printf("%s", err)
	}
	return c.JSON(http.StatusOK, StatusResponse{Status: awbstatus})
}

func Init() error {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Transport: &Tp,
	}

	es7, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return err
	}
	deleteFrom, err := strconv.Atoi(deleteIndiciesfrom)
	if err != nil {
		deleteFrom = 3
	}
	for minus := 1; minus < deleteFrom+1; minus++ {
		name := `sts_index_` + time.Now().AddDate(0, 0, minus*-1).Format("20060102")
		req := esapi.IndicesExistsRequest{
			Index: []string{name},
		}

		res, err := req.Do(context.Background(), es7.Transport)
		if err != nil {
			return err
		}
		defer func() {
			io.Copy(ioutil.Discard, res.Body)
			if res != nil {
				res.Body.Close()
			}
		}()
		if res.StatusCode == 200 {
			req := esapi.IndicesDeleteRequest{
				Index: []string{name},
			}

			res, err := req.Do(context.Background(), es7.Transport)
			if err != nil {
				return err
			}
			defer func() {
				io.Copy(ioutil.Discard, res.Body)
				if res != nil {
					res.Body.Close()
				}
			}()
		}
	}

	es_sts_idx = `sts_index_` + time.Now().Format("20060102")
	//stsデータidx(トラン)の存在確認⇒なければ作成
	req := esapi.IndicesExistsRequest{
		Index: []string{es_sts_idx},
	}

	res, err := req.Do(context.Background(), es7.Transport)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		if res != nil {
			res.Body.Close()
		}
	}()
	statuscode := res.StatusCode
	if statuscode == 404 {
		qbody := strings.NewReader(`{"mappings":{"properties":{"awb_no":{"type":"keyword","doc_values":true},"update_time":{"type":"date","doc_values":true},"sts_code":{"type":"keyword","doc_values":true},"last_updated_user":{"type":"keyword","doc_values":true},"last_updated_user_id":{"type":"keyword","doc_values":true},"company_name":{"type":"keyword","doc_values":true},"company_code":{"type":"keyword","doc_values":true},"section_code":{"type":"keyword","doc_values":true},"is_stocked":{"type":"boolean","doc_values":true},"igs_status":{"type":"keyword","doc_values":true}}}}`)
		req := esapi.IndicesCreateRequest{
			Index: es_sts_idx,
			Body:  qbody,
		}
		res, err := req.Do(context.Background(), es7.Transport)
		if err != nil {
			return err
		} else if res.StatusCode == 400 {
			log.Fatalf("%v", res)
			return errors.New("インデックス作成に失敗しました" + es_sts_idx)
		}
		defer func() {
			io.Copy(ioutil.Discard, res.Body)
			if res != nil {
				res.Body.Close()
			}
		}()
	}
	if err := GetSettings(); err != nil {
		return err
	}
	putGatewayHtml()
	return nil
}
func getLatestAwbs(from int64, to int64, timespan int) ([]Status, error) {
	var r map[string]interface{}
	hourUnit := int64(3600000)
	timeSpanUnit := int64(timespan * 60 * 1000)

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Transport: &Tp,
	}

	es7, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	qbody := strings.NewReader(`{"aggs": {"f": {"filter": {"range": {"update_time": {"gte":` + strconv.FormatInt(from, 10) + `,"lt":` + strconv.FormatInt(to, 10) + `}}},"aggs": {"awbs": {"terms": {"size": 10000, "field": "awb_no"},"aggs": {"latest": {"top_hits": {"size": 1,"sort": [{"update_time":{"order":"desc"}}]}}}}}}}}`)
	size := 10000
	req := esapi.SearchRequest{
		Index: []string{es_sts_idx},
		Body:  qbody,
		Size:  &size,
	}
	res, err := req.Do(context.Background(), es7.Transport)
	if err != nil {
		log.Printf("%v", err)
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		if res != nil {
			res.Body.Close()
		}
	}()
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	}
	result := make([]Status, 0, 100)
	f := r["aggregations"].(map[string]interface{})["f"].(map[string]interface{})
	aggs := f["awbs"].(map[string]interface{})["buckets"].([]interface{})
	for idx, a := range aggs {
		bucket := a.(map[string]interface{})
		awb := bucket["key"].(string)
		data := bucket["latest"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})
		if len(data) < 1 {
			continue
		}
		source := data[0].(map[string]interface{})["_source"].(map[string]interface{})

		sts_code, _ := source["sts_code"].(string)
		is_stocked, _ := source["is_stocked"].(bool)
		update_time, _ := source["update_time"].(float64)
		update_time_i64 := int64(update_time)
		last_user, _ := source["last_updated_user"].(string)
		last_user_id, _ := source["last_updated_user_id"].(string)
		com_code, _ := source["company_code"].(string)
		com_name, _ := source["company_name"].(string)
		sec_code, _ := source["section_code"].(string)
		igs_status, _ := source["igs_status"].(string)
		basetime := (update_time_i64 / hourUnit) * hourUnit
		q := int((update_time_i64 - basetime) / timeSpanUnit)
		result = append(result, Status{
			Index:             idx,
			Awbno:             awb,
			StatusCode:        sts_code,
			BaseTime:          time.Unix(basetime/1000, 0),
			Q:                 q,
			TimespanInMinutes: timespan,
			SectionCode:       sec_code,
			CompanyCode:       com_code,
			CompanyName:       com_name,
			LastUserName:      last_user,
			LastUserId:        last_user_id,
			IsStocked:         is_stocked,
			IgsStatus:         igs_status,
		})
	}
	return result, nil
}

func getAwbStatuses(from int64, to int64, awbno string, timespan int, isLatest bool, isUpdate bool) ([]Status, error) {
	//from to UnixTime(millsec)
	//timespan min
	var r map[string]interface{}
	hourUnit := int64(3600000)
	timeSpanUnit := int64(timespan * 60 * 1000)

	result := make([]Status, 0, 100)

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Transport: &Tp,
	}

	es7, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	qbody := strings.NewReader(`{"sort": [{"update_time": {"order": "asc"}}],"query":{"bool":{"must": [{"term":{"awb_no": {"value": "` + awbno + `"}}},{"range":{"update_time": {"gte":` + strconv.FormatInt(from, 10) + `,"lte":` + strconv.FormatInt(to, 10) + `}}}]}}}`)
	size := 1000
	req := esapi.SearchRequest{
		Index: []string{es_sts_idx},
		Body:  qbody,
		Size:  &size,
	}
	res, err := req.Do(context.Background(), es7.Transport)
	if err != nil {
		log.Printf("%v", err)
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	}
	statusIdx := 0
	hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
	if len(hits) > 0 {
		data := hits[0].(map[string]interface{})["_source"]
		p_sts_code, _ := data.(map[string]interface{})["sts_code"].(string)
		p_is_stocked, _ := data.(map[string]interface{})["is_stocked"].(bool)
		p_update_time, _ := data.(map[string]interface{})["update_time"].(float64)
		p_update_time_i64 := int64(p_update_time)
		p_last_user, _ := data.(map[string]interface{})["last_updated_user"].(string)
		p_last_user_id, _ := data.(map[string]interface{})["last_updated_user_id"].(string)
		p_com_code, _ := data.(map[string]interface{})["company_code"].(string)
		p_com_name, _ := data.(map[string]interface{})["company_name"].(string)
		p_sec_code, _ := data.(map[string]interface{})["section_code"].(string)
		p_igs_status, _ := data.(map[string]interface{})["igs_status"].(string)
		p_basetime := (p_update_time_i64 / hourUnit) * hourUnit
		p_q := int((p_update_time_i64 - p_basetime) / timeSpanUnit)

		//開始時刻のbasetimeとq
		//最初のデータが存在する時刻までの「詰め物」の個数を計算する。
		c_basetime := (from / hourUnit) * hourUnit //繰り下げ割り算
		c_q := 0
		if isLatest {
			o_basetime := c_basetime
			c_basetime = (from / timeSpanUnit) * timeSpanUnit //繰り下げ割り算
			c_q = int((c_basetime - o_basetime) / timeSpanUnit)
		} else if isUpdate {
			c_basetime = (from / hourUnit) * hourUnit
			//c_q = 0
		}
		hour_diff := int((p_basetime - c_basetime) / hourUnit)
		hour_quarters := int((hourUnit-1)/timeSpanUnit + 1)
		whitespaces := hour_quarters*hour_diff + p_q - c_q
		for whitespaces > 0 {
			result = append(result, Status{
				Index:             statusIdx,
				Awbno:             awbno,
				StatusCode:        "NA",
				BaseTime:          time.Unix(c_basetime/1000, 0),
				Q:                 int(c_q),
				TimespanInMinutes: timespan,
				SectionCode:       "",
				CompanyCode:       "",
				CompanyName:       "",
				LastUserName:      "",
				LastUserId:        "",
				IsStocked:         false,
				IgsStatus:         "",
			})
			if (c_q + 1) < hour_quarters {
				c_q++
			} else {
				c_q = 0
				c_basetime += hourUnit
			}
			statusIdx++
			whitespaces--
		}
		//c_q = p_q && c_basetime = p_basetime
		for _, hit := range hits {
			data := hit.(map[string]interface{})["_source"]
			sts_code, _ := data.(map[string]interface{})["sts_code"].(string)
			is_stocked, _ := data.(map[string]interface{})["is_stocked"].(bool)
			update_time, _ := data.(map[string]interface{})["update_time"].(float64)
			update_time_i64 := int64(update_time)
			last_user, _ := data.(map[string]interface{})["last_updated_user"].(string)
			last_user_id, _ := data.(map[string]interface{})["last_updated_user_id"].(string)
			com_code, _ := data.(map[string]interface{})["company_code"].(string)
			com_name, _ := data.(map[string]interface{})["company_name"].(string)
			sec_code, _ := data.(map[string]interface{})["section_code"].(string)
			igs_status, _ := data.(map[string]interface{})["igs_status"].(string)
			c_basetime = (update_time_i64 / hourUnit) * hourUnit
			c_q = int((update_time_i64 - c_basetime) / timeSpanUnit)

			if c_basetime != p_basetime || c_q != p_q {
				hour_diff = int((c_basetime - p_basetime) / hourUnit)
				whitespaces = hour_quarters*hour_diff + c_q - p_q - 1
				for whitespaces > 0 {
					result = append(result, Status{
						Index:             statusIdx,
						Awbno:             awbno,
						StatusCode:        "NA",
						BaseTime:          time.Unix(p_basetime/1000, 0),
						Q:                 p_q,
						TimespanInMinutes: timespan,
						SectionCode:       "",
						CompanyCode:       "",
						CompanyName:       "",
						LastUserName:      "",
						LastUserId:        "",
						IsStocked:         false,
						IgsStatus:         "",
					})
					statusIdx++
					whitespaces--
					if (p_q + 1) < hour_quarters {
						p_q++
					} else {
						p_q = 0
						p_basetime += hourUnit
					}
				}
				result = append(result, Status{
					Index:             statusIdx,
					Awbno:             awbno,
					StatusCode:        p_sts_code,
					BaseTime:          time.Unix(p_basetime/1000, 0),
					Q:                 int(p_q),
					TimespanInMinutes: timespan,
					SectionCode:       p_sec_code,
					CompanyCode:       p_com_code,
					CompanyName:       p_com_name,
					LastUserName:      p_last_user,
					LastUserId:        p_last_user_id,
					IsStocked:         p_is_stocked,
					IgsStatus:         p_igs_status,
				})
				statusIdx++
			}
			p_sts_code = sts_code
			p_is_stocked = is_stocked
			p_update_time = update_time
			p_update_time_i64 = update_time_i64
			p_last_user = last_user
			p_last_user_id = last_user_id
			p_com_code = com_code
			p_com_name = com_name
			p_sec_code = sec_code
			p_igs_status = igs_status
			p_basetime = c_basetime
			p_q = c_q
		}
		result = append(result, Status{
			Index:             statusIdx,
			Awbno:             awbno,
			StatusCode:        p_sts_code,
			BaseTime:          time.Unix(p_basetime/1000, 0),
			Q:                 int(p_q),
			TimespanInMinutes: timespan,
			SectionCode:       p_sec_code,
			CompanyCode:       p_com_code,
			CompanyName:       p_com_name,
			LastUserName:      p_last_user,
			LastUserId:        p_last_user_id,
			IsStocked:         p_is_stocked,
			IgsStatus:         p_igs_status,
		})
		statusIdx++
		if (c_q + 1) < hour_quarters {
			c_q++
		} else {
			c_q = 0
			c_basetime += hourUnit
		}
		to_basetime := ((to + hourUnit - 1) / hourUnit) * hourUnit
		hour_diff = int((to_basetime - c_basetime) / hourUnit)
		whitespaces = hour_quarters*hour_diff - c_q
		if isLatest || isUpdate {
			to_basetime = (to / timeSpanUnit) * timeSpanUnit
			hour_diff = int((to_basetime - c_basetime) / hourUnit)
			to_q := int((to - to_basetime) / timeSpanUnit)
			whitespaces = hour_quarters*hour_diff + to_q - c_q
		}
		for whitespaces > 0 {
			result = append(result, Status{
				Index:             statusIdx,
				Awbno:             awbno,
				StatusCode:        "NA",
				BaseTime:          time.Unix(c_basetime/1000, 0),
				Q:                 int(c_q),
				TimespanInMinutes: timespan,
				SectionCode:       "",
				CompanyCode:       "",
				CompanyName:       "",
				LastUserName:      "",
				LastUserId:        "",
				IsStocked:         false,
				IgsStatus:         "",
			})
			statusIdx++
			whitespaces--
			if (c_q + 1) < hour_quarters {
				c_q++
			} else {
				c_q = 0
				c_basetime += hourUnit
			}
		}
		return result, nil
	} else {
		return nil, nil
	}
}

func GetSettings() error {
	b, err := ioutil.ReadFile("path.ini")
	if err != nil {
		log.Fatalf("%v", err)
	}
	lines := strings.Split(string(b), "\n")
	settings := make(map[string]string)
	for _, line := range lines {
		kv := strings.Split(line, "=")
		if len(kv) < 2 {
			continue
		}
		settings[kv[0]] = kv[1]
	}
	if settings["LockFolderPath"] == "" {
		return errors.New("LockFolderPathが設定されていません")
	}
	lockFolderPath = settings["LockFolderPath"]
	if settings["STSFilePath"] == "" {
		return errors.New("STSFilePathが設定されていません")
	}
	stsFilePath = settings["STSFilePath"]
	if settings["STS75FilePath"] == "" {
		return errors.New("STS75FilePathが設定されていません")
	}
	sts75FilePath = settings["STS75FilePath"]
	if settings["IGSFolderPath"] == "" {
		return errors.New("IGSFolderPathが設定されていません")
	}
	igsFolderPath = settings["IGSFolderPath"]
	if settings["STSLinkFileName"] == "" {
		return errors.New("STSLinkFileNameが設定されていません")
	}
	stsLinkFileName = settings["STSLinkFileName"]
	if settings["STSLockFileName"] == "" {
		return errors.New("STSLockFileNameが設定されていません")
	}
	stsLockFileName = settings["STSLockFileName"]
	if settings["75ListLinkFileName"] == "" {
		return errors.New("75ListLinkFileNameが設定されていません")
	}
	sts75ListLinkFileName = settings["75ListLinkFileName"]
	if settings["75ListLockFileName"] == "" {
		return errors.New("75ListLockFileNameが設定されていません")
	}
	sts75ListLockFileName = settings["75ListLockFileName"]
	if settings["IGSLinkFileName"] == "" {
		return errors.New("IGSLinkFileNameが設定されていません")
	}
	igsLinkFileName = settings["IGSLinkFileName"]
	if settings["IGSLockFileName"] == "" {
		return errors.New("IGSLockFileNameが設定されていません")
	}
	igsLockFileName = settings["IGSLockFileName"]
	if settings["IGSFileName"] == "" {
		return errors.New("IGSFileNameが設定されていません")
	}
	igsFileName = settings["IGSFileName"]
	if settings["IGSBLNOFilename"] == "" {
		return errors.New("IGSBLNOFilenameが設定されていません")
	}
	igsBLNOFileName = settings["IGSBLNOFilename"]
	if settings["GatewayPath"] == "" {
		return errors.New("GatewayPathが設定されていません")
	}
	gatewayPath = settings["GatewayPath"]
	if settings["GatewayFilename"] == "" {
		return errors.New("GatewayFilenameが設定されていません")
	}
	gatewayHtml = settings["GatewayFilename"]
	if settings["DeleteIndiciesfrom"] == "" {
		return errors.New("DeleteIndiciesfromが設定されていません")
	}
	deleteIndiciesfrom = settings["DeleteIndiciesfrom"]
	return nil
}

func readFiles() (chan STSResult, error) {
	resChan := make(chan STSResult)
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Transport: &Tp,
	}

	es7, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	stslockfile := lockFolderPath + `\` + stsLockFileName
	stsoriginfile := lockFolderPath + `\` + stsLinkFileName
	sts75lockfile := lockFolderPath + `\` + sts75ListLockFileName
	sts75originfile := lockFolderPath + `\` + sts75ListLinkFileName
	igsoriginfile := lockFolderPath + `\` + igsLinkFileName
	igslockfile := lockFolderPath + `\` + igsLockFileName
	igsMap := make(map[string]string)
	go func() {
		for {
			deadman := time.After(30 * time.Minute)
			func() {
				//setting Timers
				igsTicker := time.NewTicker(30 * time.Second)
				time.Sleep(5 * time.Second)
				stsTicker := time.NewTicker(90 * time.Second)
				defer func() {
					stsTicker.Stop()
					igsTicker.Stop()
				}()
				for {
					select {
					case <-stsTicker.C:
						readSTSfile(igsMap, resChan, stslockfile, stsoriginfile, sts75lockfile, sts75originfile, es7)
					case <-igsTicker.C:
						readIgsFile(igsMap, resChan, igslockfile, igsoriginfile)
					case <-deadman:
						log.Println("Deadman Awake")
						return
					}
				}
			}()
		}
	}()
	return resChan, nil
}

func readSTSfile(igsMap map[string]string, resChan chan STSResult, stslockfile, stsoriginfile, sts75lockfile, sts75originfile string, es7 *elasticsearch.Client) {
	maxUpdateSTS := 10
	//cfg := elasticsearch.Config{
	//	Addresses: []string{
	//		"http://localhost:9200",
	//	},
	//	Transport: &Tp,
	//}

	//es7, err := elasticsearch.NewClient(cfg)
	//if err != nil {
	//	resChan <- STSResult{Result: nil, Error: err}
	//}
	//stsTicker := time.NewTicker(30 * time.Second)
	//stslockfile := lockFolderPath + `\` + stsLockFileName
	//stsoriginfile := lockFolderPath + `\` + stsLinkFileName
	//sts75lockfile := lockFolderPath + `\` + sts75ListLockFileName
	//sts75originfile := lockFolderPath + `\` + sts75ListLinkFileName

	awbs := make([]string, 0, 100)
	brs := make([]string, 0, 100)
	stss := make([]string, 0, 100)
	users := make([]string, 0, 100)
	userids := make([]string, 0, 100)
	coms := make([]string, 0, 100)
	comcds := make([]string, 0, 100)
	secs := make([]string, 0, 100)
	log.Printf("STSファイルの読み込みを開始します")
	if err := os.Link(stsoriginfile, stslockfile); err != nil {
		cntr := 0
		for {
			err := os.Link(stsoriginfile, stslockfile)
			if err != nil {
				time.Sleep(time.Second * 1)
				cntr++
				if cntr > 30 {
					log.Println("30秒待ちましたがロックが解除されません。ロックファイルを強制削除します" + ":" + stslockfile)
					os.Remove(stslockfile)
					continue
				}
			} else {
				break
			}
		}
	}
	f, err := os.Open(stsFilePath)
	if err != nil {
		resChan <- STSResult{Result: nil, Error: err}
	}
	r := csv.NewReader(transform.NewReader(f, japanese.ShiftJIS.NewDecoder()))
	r.Read()
	record, _ := r.Read()
	prevawb := strings.TrimSpace(record[2])
	prevbr := strings.TrimSpace(record[3])
	prevsts := strings.TrimSpace(record[7])
	prevusr := strings.TrimSpace(record[12])
	prevuserid := strings.TrimSpace(record[11])
	prevcom := strings.TrimSpace(record[6])
	prevcomcd := strings.TrimSpace(record[5])
	prevsec := strings.TrimSpace(record[1])
	for {
		record, err = r.Read()
		if err == io.EOF {
			awbs = append(awbs, prevawb)
			brs = append(brs, prevbr)
			stss = append(stss, prevsts)
			users = append(users, prevusr)
			userids = append(userids, prevuserid)
			coms = append(coms, prevcom)
			comcds = append(comcds, prevcomcd)
			secs = append(secs, prevsec)
			break
		} else if record[0] == "\x1a" {
			continue
		} else if err != nil {
			resChan <- STSResult{Result: nil, Error: err}
		}
		if (prevawb + "-" + prevbr) != (strings.TrimSpace(record[2]) + "-" + strings.TrimSpace(record[3])) {
			awbs = append(awbs, prevawb)
			brs = append(brs, prevbr)
			stss = append(stss, prevsts)
			users = append(users, prevusr)
			userids = append(userids, prevuserid)
			coms = append(coms, prevcom)
			comcds = append(comcds, prevcomcd)
			secs = append(secs, prevsec)
		}
		prevawb = strings.TrimSpace(record[2])
		prevbr = strings.TrimSpace(record[3])
		prevsts = strings.TrimSpace(record[7])
		prevusr = strings.TrimSpace(record[12])
		prevuserid = strings.TrimSpace(record[11])
		prevcom = strings.TrimSpace(record[6])
		prevcomcd = strings.TrimSpace(record[5])
		prevsec = strings.TrimSpace(record[1])
	}
	f.Close()
	update_time := time.Now().UnixNano() / int64(time.Millisecond)

	bbody_str := ""
	for i := 0; i < len(awbs); i++ {
		awb := awbs[i]
		oriawb := awb
		if brs[i] != "0" {
			awb += "-" + brs[i]
		}
		sts := stss[i]
		user := users[i]
		userid := userids[i]
		com := coms[i]
		comcd := comcds[i]
		sec := secs[i]
		is_stocked := "false"
		igs_status := igsMap[oriawb]
		if igsMap[oriawb] == "" {
			igs_status = "-1"
		}
		bbody_str += `{ "create" : { "_index" : "` + es_sts_idx + `"}}` + "\n"
		bbody_str += `{"awb_no":"` + awb + `","update_time":` + strconv.FormatInt(update_time, 10) + `,"sts_code":"` + sts + `","last_updated_user":"` + user + `","last_updated_user_id":"` + userid + `","company_name":"` + com + `","company_code":"` + comcd + `","section_code":"` + sec + `","is_stocked":` + is_stocked + `,"igs_status":"` + igs_status + `"}` + "\n"
		if i%maxUpdateSTS == maxUpdateSTS-1 {
			bbody := strings.NewReader(bbody_str)
			breq := esapi.BulkRequest{
				Index: es_sts_idx,
				Body:  bbody,
			}
			func() {
				res, err := breq.Do(context.Background(), es7.Transport)
				if err != nil {
					resChan <- STSResult{Result: nil, Error: err}
				} else if res.StatusCode == 400 {
					log.Fatalf("データ登録に失敗 %v", res)
				}
				defer func() {
					io.Copy(ioutil.Discard, res.Body)
					if res != nil {
						res.Body.Close()
					}
				}()
			}()
			bbody_str = ""
		}
	}
	if bbody_str != "" {
		bbody := strings.NewReader(bbody_str)
		breq := esapi.BulkRequest{
			Index: es_sts_idx,
			Body:  bbody,
		}
		func() {
			res, err := breq.Do(context.Background(), es7.Transport)
			if err != nil {
				resChan <- STSResult{Result: nil, Error: err}
			} else if res.StatusCode == 400 {
				log.Fatalf("データ登録に失敗 %v", res)
			}
			defer func() {
				io.Copy(ioutil.Discard, res.Body)
				if res != nil {
					res.Body.Close()
				}
			}()
		}()
	}
	awbStatuss := make([]AwbStatus, 0, 100)
	for idx, _ := range awbs {
		awb := awbs[idx]
		if brs[idx] != "0" {
			awb += "-" + brs[idx]
		}
		awbStatuss = append(awbStatuss, AwbStatus{
			Awbno:        awb,
			UpdateTime:   time.Unix(update_time, 0),
			StatusCode:   stss[idx],
			SectionCode:  secs[idx],
			CompanyCode:  coms[idx],
			CompanyName:  comcds[idx],
			LastUserName: users[idx],
			LastUserId:   userids[idx],
		})
	}
	resChan <- STSResult{Result: awbStatuss, Error: err}
	os.Remove(stslockfile)
	log.Printf("STS75ファイルの書き出しを開始します")
	if err := os.Link(sts75originfile, sts75lockfile); err != nil {
		cntr := 0
		for {
			err := os.Link(sts75originfile, sts75lockfile)
			if err != nil {
				time.Sleep(time.Second * 1)
				cntr++
				if cntr > 30 {
					log.Println("30秒待ちましたがロックが解除されません。ロックファイルを強制削除します" + ":" + sts75lockfile)
					os.Remove(sts75lockfile)
					continue
				}
			} else {
				break
			}
		}
	}
	//sts75mapの初期化
	temp75Map := make(map[string]bool)
	for idx, awb := range awbs {
		if stss[idx] == "75" && (igsMap[awb] == "0" || igsMap[awb] == "") {
			temp75Map[awb] = true
		}
	}
	ef, err := excelize.OpenFile(sts75FilePath)
	if err != nil {
		resChan <- STSResult{Result: nil, Error: err}
	}
	firstShName := ef.GetSheetName(0)
	ef.NewSheet("temp")
	ef.DeleteSheet(firstShName)
	ef.NewSheet(firstShName)
	ef.DeleteSheet("temp")
	cntr := 1
	for awb, _ := range temp75Map {
		ef.SetCellValue(firstShName, "A"+strconv.Itoa(cntr), awb)
		cntr++
	}
	ef.SaveAs(sts75FilePath)
	ef.Close()
	os.Remove(sts75lockfile)

}

func readIgsFile(igsMap map[string]string, resChan chan STSResult, igslockfile, igsoriginfile string) {
	log.Printf("IGSファイルの読み込みを開始します")
	if err := os.Link(igsoriginfile, igslockfile); err != nil {
		cntr := 0
		for {
			err := os.Link(igsoriginfile, igslockfile)
			if err != nil {
				time.Sleep(time.Second * 1)
				cntr++
				if cntr > 30 {
					log.Println("30秒待ちましたがロックが解除されません。ロックファイルを強制削除します" + ":" + igslockfile)
					os.Remove(igslockfile)
					continue
				}
			} else {
				break
			}
		}
	}
	resChan <- STSResult{nil, nil}
	//sts75mapの初期化..はしない
	//igsMap = make(map[string]bool)
	//igs確認済みのものはigsMapに追加され続ける。
	blnofiles, err := findMatchedFiles(igsFolderPath, igsBLNOFileName)
	if err != nil {
		resChan <- STSResult{Result: nil, Error: err}
	}
	if len(blnofiles) < 1 {
		log.Println("BLNOファイルがありません")
		os.Remove(igslockfile)
		return
	}
	awbnos := make([]string, 0, 100)
	for _, blnofile := range blnofiles {
		b, err := ioutil.ReadFile(blnofile)
		if err != nil {
			resChan <- STSResult{Result: nil, Error: err}
		}
		lines := strings.Split(string(b), "\r\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			line = strings.TrimSpace(line)
			awbnos = append(awbnos, line)
		}
	}
	igsfiles, err := findMatchedFiles(igsFolderPath, igsFileName)
	if err != nil {
		resChan <- STSResult{Result: nil, Error: err}
	}

	if len(igsfiles) < 1 {
		log.Println("IGS結果のファイルがありません")
		os.Remove(igslockfile)
		return
	}
	igsStss := make([]string, 0, 100)
	for _, igsFile := range igsfiles {
		b, err := ioutil.ReadFile(igsFile)
		if err != nil {
			resChan <- STSResult{Result: nil, Error: err}
		}
		lines := strings.Split(string(b), "\r\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			line = strings.TrimSpace(line)
			igsStss = append(igsStss, line)
		}
	}
	for i, igsSts := range igsStss {
		if i > len(awbnos)-1 {
			continue
		}
		igsMap[awbnos[i]] = igsSts
	}
	for _, blnoFile := range blnofiles {
		os.Remove(blnoFile)
	}
	for _, igsFile := range igsfiles {
		os.Remove(igsFile)
	}
	os.Remove(igslockfile)
}

func findMatchedFiles(root, pattern string) ([]string, error) {
	findList := []string{}

	err := filepath.WalkDir(root, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return errors.New("failed filepath.WalkDir")
		}

		filename := info.Name()
		if info.IsDir() {
			return nil
		}
		if strings.Contains(filename, pattern) && strings.Index(filename, pattern) == 0 {
			findList = append(findList, path)
		}

		return nil
	})
	return findList, err
}

func getLoacalIps() ([]string, error) {
	netInterfaceAddresses, _ := net.InterfaceAddrs()
	ip := make([]string, 0, 20)
	for _, netInterfaceAddress := range netInterfaceAddresses {

		networkIp, ok := netInterfaceAddress.(*net.IPNet)

		if ok && !networkIp.IP.IsLoopback() && networkIp.IP.To4() != nil && networkIp.IP.IsPrivate() {
			ip = append(ip, networkIp.IP.String())

		}
	}
	return ip, nil
}

func putGatewayHtml() error {
	ipaddr, err := getLoacalIps()
	if err != nil {
		log.Printf("%s", err)
		return nil
	}
	if f, err := os.Stat(gatewayPath); os.IsNotExist(err) || !f.IsDir() {
		log.Println("GatewayPathに指定したディレクトリは存在しないかアクセスできません" + gatewayPath)
		return nil
	}
	if f, err := os.Stat(gatewayPath + `\` + gatewayHtml); os.IsNotExist(err) && f != nil && !f.IsDir() {
		os.Remove(gatewayPath + `\` + gatewayHtml)
	}
	lines := make([]string, 0, 20)
	lines = append(lines, `<HTML>`)
	lines = append(lines, `<BODY>`)
	for _, ip := range ipaddr {
		url := `http://` + ip + `:8080/`
		lines = append(lines, `<A href = "`+url+`">ここをクリック！</A><BR>`)
	}
	lines = append(lines, `</HTML>`)
	lines = append(lines, `</BODY>`)

	func() {
		file, err := os.Create(gatewayPath + `\` + gatewayHtml)
		if err != nil {
			log.Printf("%s", err)
			return
		}
		defer file.Close()

		for _, line := range lines {
			_, err := file.WriteString(line)
			// fmt.Fprint()の場合
			// _, err := fmt.Fprint(file, line)
			if err != nil {
				log.Printf("%s", err)
				return
			}
		}
	}()
	return nil
}
