package models

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"mds/global"
	"mds/others/packet"
	"net"
	"net/http"
	"os"
	//"strconv"
	"time"
)

//var i = 0
var SQL_SUCCESS int32 = 0
var SQL_SUCCESS_WITH_INFO int32 = 1
var ESCONNECTD = true

type Conf struct {
	Esurl string `yaml:"esurl"`
	Esusr string `yaml:"esusr"`
	Espwd string `yaml:"espwd"`
}

func (c *Conf) getConf() *Conf {
	TRAF_CONF := os.Getenv("TRAF_CONF")
	yamlFile, err := ioutil.ReadFile(TRAF_CONF + "/dcs/" + "mdsconf.yaml")
	if err != nil {
		global.Logger.Println(err.Error())
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		global.Logger.Println(err.Error())
	}

	return c
}

func ZkStateString(s *zk.Stat) string {
	return fmt.Sprintf("Czxid:%d, Mzxid: %d, Ctime: %d, Mtime: %d, Version: %d, Cversion: %d, Aversion: %d, EphemeralOwner: %d, DataLength: %d, NumChildren: %d, Pzxid: %d",
		s.Czxid, s.Mzxid, s.Ctime, s.Mtime, s.Version, s.Cversion, s.Aversion, s.EphemeralOwner, s.DataLength, s.NumChildren, s.Pzxid)
}

func ZkStateStringFormat(s *zk.Stat) string {
	return fmt.Sprintf("Czxid:%d\nMzxid: %d\nCtime: %d\nMtime: %d\nVersion: %d\nCversion: %d\nAversion: %d\nEphemeralOwner: %d\nDataLength: %d\nNumChildren: %d\nPzxid: %d\n",
		s.Czxid, s.Mzxid, s.Ctime, s.Mtime, s.Version, s.Cversion, s.Aversion, s.EphemeralOwner, s.DataLength, s.NumChildren, s.Pzxid)
}
func Zookeeper(conn *zk.Conn, msgData *packet.Message) (int32, string, []byte) {
	global.Logger.Println("zookeeper here")
	//ret := global.Response{}
	opt := msgData.GetZkbody().GetOpt()
	//buf := msgData.GetBody().GetBuf()
	data := msgData.GetZkbody().GetData()
	path := msgData.GetZkbody().GetPath()
	flag := msgData.GetZkbody().GetFlag()
	//acl := msgData.GetBody().GetAcl()
	var byteData []byte
	byteData = nil
	switch opt {
	case packet.OperationId_CREATE:
		p, err := conn.Create(path, data, flag, zk.WorldACL(zk.PermAll))
		if err != nil {
			return -1, err.Error(), nil
		}
		global.Logger.Println("created:", p)
	case packet.OperationId_DELETE:
		err := conn.Delete(path, 0)
		if err != nil {
			fmt.Println(err)
			return -1, err.Error(), nil
		}
	case packet.OperationId_EXISTS:
		exist, _, err := conn.Exists(path)
		if err != nil {
			fmt.Println(err)
			return -1, err.Error(), nil
		}
		global.Logger.Println("path[%s] exist[%t]\n", path, exist)
		global.Logger.Println("state:\n")
	case packet.OperationId_WRITE:
		s, err := conn.Set(path, data, 0)
		if err != nil {
			fmt.Println(err)
			return -1, err.Error(), nil
		}
		global.Logger.Println("update state:\n")
		global.Logger.Println("%s\n", ZkStateStringFormat(s))
	case packet.OperationId_READ:
		v, s, err := conn.Get(path)
		if err != nil {
			fmt.Println(err)
			return -1, err.Error(), nil
		}
		byteData = v
		global.Logger.Println("value of path[%s]=[%s].\n", path, v)
		global.Logger.Println("state:\n")
		global.Logger.Println("%s\n", ZkStateStringFormat(s))
	default:
		return -1, "Operation id fault", nil

	}
	return 0, "SUCESS", byteData
}
func Metadata(es *elasticsearch.Client, msgData *packet.Message) {

	hostname := msgData.GetMdbody().GetHostname()
	mxosrvrname := msgData.GetMdbody().GetMxosrvrname()
	pid := msgData.GetMdbody().GetPid()
	sqlcount := msgData.GetMdbody().GetSqlcount()
	totalruntime := msgData.GetMdbody().GetTotalruntime()
	totalconnections := msgData.GetMdbody().GetTotalconnections()
	withquery := msgData.GetMdbody().GetWithquery()
	TRAF_CLUSTER_ID := os.Getenv("TRAF_CLUSTER_ID")
	Esindex := "mxosrvrs"
	// index = mxosrvrs_2019-11-9_1_1
	esdate := time.Now().Format("2006-01-02")
	Esindex = Esindex + "_" + TRAF_CLUSTER_ID + "_" + esdate
	//global.Logger.Printf("Esindex: %s", Esindex)

	key := fmt.Sprintf("%s-%s-%d", hostname, mxosrvrname, pid)
	_, ok := global.Mxometadata.Load(key)
	if ok != true { //
		global.Mxometadata.Store(key, global.MxoMd{hostname, mxosrvrname, pid, sqlcount, totalconnections, totalruntime, 0, 0})
		global.Logger.Printf("mxosrvrname : %s pid : %d connnected", mxosrvrname, pid)
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
		if ESCONNECTD == false {
			global.SqlLogger.Printf("Sqlstring :%s Stmttype :%d Returncode :%d Costtime :%d\n", sqlstring, stmttype, returncode, costtime)
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
		var q = global.Query{Hostname: hostname, Mxosrvrname: mxosrvrname, Pid: pid, Ret: returncode, Sqlstring: sqlstring, Type: stmttype, Costtime: costtime, Starttime: querystarttime, Clientcomputername: clientcomputername, Clientappname: clientappname, Clientipaddress: clientipaddress, Clientport: clientport, Qid: qid}
		qb, _ := json.Marshal(q)
		r := bytes.NewReader(qb)
	//	i++
		req := esapi.IndexRequest{
			Index:      Esindex,
	//		DocumentID: strconv.Itoa(i + 1),
			Body:       r,
		}

		// Perform the request with the client.
		res, err := req.Do(context.Background(), es)
		if err != nil {
			global.Logger.Printf("Error getting response: %s set ESCONNECTD to false", err)
			ESCONNECTD = false
			global.SqlLogger.Printf("Sqlstring :%s Stmttype :%d Returncode :%d Costtime :%d\n", sqlstring, stmttype, returncode, costtime)
			return
		}
		defer res.Body.Close()

		if res.IsError() {

			ESCONNECTD = false
			global.Logger.Printf("[%s] Error indexing document set ESCONNECTD to false", res.Status())
		} else {
			// Deserialize the response into a map.
			var r map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				global.Logger.Printf("Error parsing the response body: %s", err)
			} else {
				// Print the response status and indexed document version.
				//	global.Logger.Printf("Es response [%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
			}
		}
	}

}
func Worker() {
	global.Logger.Println("Worker routine")
	defer global.Wg.Done()
	/*
		zkconn, _, err:= zk.Connect(global.ZkHost, time.Second*5)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer zkconn.Close()
	*/
	var cnf = Conf{}
	conf := cnf.getConf()
	global.Logger.Println("Esurl : ")
	global.Logger.Println(conf.Esurl)
	global.Logger.Println("Esusr : ")
	global.Logger.Println(conf.Esusr)
	var cfg elasticsearch.Config
	if conf.Esusr == "" || conf.Espwd == "" {
		global.Logger.Println("connect to es without usr pwd")
		cfg = elasticsearch.Config{
			Addresses: []string{
				conf.Esurl,
			},
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   10,
				ResponseHeaderTimeout: time.Second,
				DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
				TLSClientConfig: &tls.Config{
					MaxVersion:         tls.VersionTLS11,
					InsecureSkipVerify: true,
				},
			},
		}
	} else {
		global.Logger.Println("connect to es with usr pwd")
		cfg = elasticsearch.Config{
			Addresses: []string{
				conf.Esurl,
			},
			Username: conf.Esusr,
			Password: conf.Espwd,
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   10,
				ResponseHeaderTimeout: time.Second,
				DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
				TLSClientConfig: &tls.Config{
					MaxVersion:         tls.VersionTLS11,
					InsecureSkipVerify: true,
				},
			},
		}
	}
	var es, err = elasticsearch.NewClient(cfg)
	if err != nil {
		global.Logger.Fatalf("Error creating the client: %s", err)
	}
	global.Logger.Println(elasticsearch.Version)
	//global.Logger.Println(es.Info())
    
	global.Logger.Println("es connected!")
	res, err := es.Info()
	if err != nil {
		global.Logger.Fatalf("ES connecting Error getting response: %s", err)
	}
	// Check response status
	if res.IsError() {
		global.Logger.Fatalf("ES connecting Error: %s", res.String())
	}
	for {
		recdata := <-global.ChListener
		msgData := &packet.Message{}
		proto.Unmarshal(recdata.Buf, msgData)
		msgtype := msgData.GetHeader().GetType()
		/*
			var byteData []byte
			var sendMsg string
			var retCode int32
		*/
		switch msgtype {
		case packet.MsgType_ZOOKEEPER:
		/*	retCode,sendMsg,byteData = Zookeeper(zkconn,msgData)
			send := global.Response{recdata.Conn,retCode,sendMsg,byteData}
			global.ChSender<- send
		*/
		case packet.MsgType_METADATA:
			Metadata(es, msgData)
		case packet.MsgType_OTHER:
		default:
			global.Logger.Println("Unknown")
		}
	}

}
