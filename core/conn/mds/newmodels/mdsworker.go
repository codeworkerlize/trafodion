package newmodels

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/olivere/elastic/v7"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"mds/global"
	"mds/others/packet"
	"net/http"
	"os"
	"syscall"
	"time"
)

var SQL_SUCCESS int32 = 0
var SQL_SUCCESS_WITH_INFO int32 = 1
var ESCONNECTD = true

type Client struct {
	client *elastic.Client
	err    error
}
type Conf struct {
	Esurl string `yaml:"esurl"`
	Esusr string `yaml:"esusr"`
	Espwd string `yaml:"espwd"`
}

func GetConf() Conf {
	var c Conf
	TRAF_CONF := os.Getenv("TRAF_CONF")
	yamlFile, err := ioutil.ReadFile(TRAF_CONF + "/dcs/" + "mdsconf.yaml")
	global.Logger.Printf("yamlFile = %s", yamlFile)
	if err != nil {
		global.Logger.Println(err.Error())
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		global.Logger.Println(err.Error())
	}
	return c
}

type MyRetrier struct {
	backoff elastic.Backoff
}

func NewMyRetrier() *MyRetrier {
	return &MyRetrier{
		backoff: elastic.NewExponentialBackoff(10*time.Millisecond, 8*time.Second),
	}
}
func (r *MyRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {
	// Fail hard on a specific error
	if err == syscall.ECONNREFUSED {
		return 0, false, errors.New("Elasticsearch or network down")
	}

	// Stop after 5 retries
	if retry >= 5 {
		//add something
		return 0, false, nil
	}

	// Let the backoff strategy decide how long to wait and whether to stop
	wait, stop := r.backoff.Next(retry)
	return wait, stop, nil
}
func (c *Client) init() {
	conf := GetConf()
	global.Logger.Println("Esurl : ")
	global.Logger.Println(conf.Esurl)
	global.Logger.Println("Esusr : ")
	global.Logger.Println(conf.Esusr)
	var client *elastic.Client
	var err error
	//conf.Esusr = ""
	//conf.Espwd = ""
	//conf.Esurl = "http://10.13.30.71:30002"
	if conf.Esusr != "" && conf.Espwd != "" {
		client, err = elastic.NewClient(
			elastic.SetURL(conf.Esurl),
			elastic.SetBasicAuth(conf.Esusr, conf.Espwd),
			elastic.SetSniff(false),
			elastic.SetHealthcheckInterval(10*time.Second),
			elastic.SetRetrier(NewMyRetrier()),
			elastic.SetGzip(true),
			elastic.SetHeaders(http.Header{
				"X-Caller-Id": []string{"..."},
			}),
		)
	} else {
		client, err = elastic.NewClient(
			elastic.SetURL(conf.Esurl),
			elastic.SetSniff(false),
			elastic.SetHealthcheckInterval(10*time.Second),
			elastic.SetRetrier(NewMyRetrier()),
			elastic.SetGzip(true),
			elastic.SetHeaders(http.Header{
				"X-Caller-Id": []string{"..."},
			}),
		)
	}
	c.client = client
	c.err = err
}
func (c *Client) getIndex() string {
	TRAF_CLUSTER_ID := os.Getenv("TRAF_CLUSTER_ID")
	Esindex := "mxosrvrs"
	// index = mxosrvrs_2019-11-9_1_1
	esdate := time.Now().Format("2006-01-02")
	Esindex = Esindex + "_" + TRAF_CLUSTER_ID + "_" + esdate
	return Esindex
}
func (c *Client) metaData(msgDataArray []packet.Message) {
	if len(msgDataArray) == 0 {
		return
	}
	var send bool = false
	ctx := context.Background()
	esIndex := c.getIndex()
	bulkRequest := c.client.Bulk()
	for _, msgEntry := range msgDataArray {
		msgData := &msgEntry
		hostname := msgData.GetMdbody().GetHostname()
		mxosrvrname := msgData.GetMdbody().GetMxosrvrname()
		pid := msgData.GetMdbody().GetPid()
		sqlcount := msgData.GetMdbody().GetSqlcount()
		totalruntime := msgData.GetMdbody().GetTotalruntime()
		totalconnections := msgData.GetMdbody().GetTotalconnections()
		withquery := msgData.GetMdbody().GetWithquery()

		key := fmt.Sprintf("%s-%s-%d", hostname, mxosrvrname, pid)
		_, ok := global.Mxometadata.Load(key)
		if ok != true { //
			global.Mxometadata.Store(key, global.MxoMd{hostname, mxosrvrname, pid, sqlcount, totalconnections, totalruntime, 0, 0})
		}
		if withquery == 1 {
			querystarttime := msgData.GetMdbody().GetQuerybody().GetStarttime()
			sqlstring := msgData.GetMdbody().GetQuerybody().GetSqlstring()
			returncode := msgData.GetMdbody().GetQuerybody().GetRetcode()
			costtime := msgData.GetMdbody().GetQuerybody().GetCosttime()
			stmttype := msgData.GetMdbody().GetQuerybody().GetQuerytype()
			clientcomputername := msgData.GetMdbody().GetQuerybody().GetClientcomputername()
			clientappname := msgData.GetMdbody().GetQuerybody().GetClientappname()
			clientipaddress := msgData.GetMdbody().GetQuerybody().GetClientipaddress()
			clientport := msgData.GetMdbody().GetQuerybody().GetClientport()
			qid := msgData.GetMdbody().GetQuerybody().GetQid()
			txnid := msgData.GetMdbody().GetQuerybody().GetTxnID()
			username := msgData.GetMdbody().GetQuerybody().GetUsername()
			dialogueid := msgData.GetMdbody().GetQuerybody().GetDialogueid()
			if ESCONNECTD == false {
				//	global.SqlLogger.Printf("Sqlstring :%s Stmttype :%d Returncode :%d Costtime :%d\n", sqlstring, stmttype, returncode, costtime)
				return
			}

			var judge = returncode == SQL_SUCCESS || returncode == SQL_SUCCESS_WITH_INFO
			var successnsnum int32
			var failurenum int32
			var tmp, _ = global.Mxometadata.Load(key)
			var md = tmp.(global.MxoMd)
			if judge {
				successnsnum = md.Successnsnum + 1
				failurenum = md.Failurenum

			} else {
				successnsnum = md.Successnsnum
				failurenum = md.Failurenum + 1
			}
			global.Mxometadata.Store(key, global.MxoMd{hostname, mxosrvrname, pid, sqlcount, totalconnections, totalruntime, successnsnum, failurenum})
            var q = global.Query{Hostname: hostname, Mxosrvrname: mxosrvrname, Pid: pid, Ret: returncode, Sqlstring: sqlstring, Type: stmttype, Costtime: costtime, Starttime: querystarttime, Clientcomputername: clientcomputername, Clientappname: clientappname, Clientipaddress: clientipaddress, Clientport: clientport, Qid: qid, Txnid: txnid, UserName: username, DialogueId: dialogueid}

			documentJSON, err := json.Marshal(q)
			if err != nil {
				global.Logger.Printf("error while marshaling document, err: %v", err)
				continue
			}
			indexRq := elastic.NewBulkIndexRequest().
				Index(esIndex).Doc(string(documentJSON))
			bulkRequest = bulkRequest.Add(indexRq)
			send = true

		} else {
			var successnsnum int32
			var failurenum int32
			var tmp, _ = global.Mxometadata.Load(key)
			var md = tmp.(global.MxoMd)
			successnsnum = md.Successnsnum
			failurenum = md.Failurenum
			global.Mxometadata.Store(key, global.MxoMd{hostname, mxosrvrname, pid, sqlcount, totalconnections, totalruntime, successnsnum, failurenum})
		}

	}
	if send {
		bulkResponse, err := bulkRequest.Do(ctx)
		if err != nil {
			global.Logger.Printf("error while bulkrequest  err: %v", err)
		} else {

			// there are some failed requests, count it!
			failedResults := bulkResponse.Failed()
			if len(failedResults) > 0 {
				global.Logger.Printf("there are %d failed requests to Elasticsearch.", len(failedResults))
			}
		}
	}
}
func Worker() {
	global.Logger.Println("New Worker routine")
	defer global.Wg.Done()
	var client Client
	client.init()
	if client.err != nil {
		global.Logger.Fatalf("Error creating the client: %s", client.err)
	}
	global.Logger.Printf("Es client init success")
	var msgDataArray []packet.Message
	var count int = 0
	msgData := packet.Message{}
	for {
		select {
		case recdata, ok := <-global.ChListener:
			count++
			if ok {
				proto.Unmarshal(recdata.Buf, &msgData)
				msgDataArray = append(msgDataArray, msgData)
				if count >= 50 {
					//		global.Logger.Printf("count >600 send")
					count = 0
					//			t1 := time.Now()
					client.metaData(msgDataArray)
					//			elapsed := time.Since(t1)
					//		global.Logger.Println("elapsed :",elapsed)
					msgDataArray = msgDataArray[0:0]
				}
			} else {
				count = 0
				client.metaData(msgDataArray)
				msgDataArray = msgDataArray[0:0]
			}
		case <-time.After(1 * time.Second): //上面的ch如果一直没数据会阻塞，那么select也会检测其他case条件，检测到后1秒超时
			count = 0
			client.metaData(msgDataArray)
			msgDataArray = msgDataArray[0:0]
		}
	}

}
