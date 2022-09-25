package global

import (
	"log"
	"net"
	"sync"
)

const ()

var Wg sync.WaitGroup

type MxoMd struct {
	Hostname   string `json:"hostname"`
	Name         string `json:"name"`
	Pid          int32  `json:"pid"`
	Sqlcount     int32  `json:"sqlcount"`
	Totalconns   int32  `json:"totalconns"`
	Totalruntime int64  `json:"totalruntime"`
	Successnsnum int32   `json:"successnum"`
	Failurenum      int32   `json:"failurenum"`
}
type Query struct {
	Hostname    string `json:"hostname"`
	Mxosrvrname string `json:"Mxosrvrname"`
	Pid         int32  `json:"pid"`
	Ret       int32  `json:"retcode"`
	Costtime  int64  `json:"costtime"`
	Type      int32  `json:"type"`
	Sqlstring string `json:"sqlstring"`
    Starttime int64  `json:"starttime"`
	Clientcomputername  string `json:"clientcomputername"`
	Clientappname  string `json:"clientappname"`
	Clientipaddress  string `json:"clientipaddress"`
    Clientport int64  `json:"clientport"`
	Qid  string `json:"qid"`
	Txnid  int64 `json:"txnid"`
    UserName  string `json:"username"`
	DialogueId  int32  `json:"dialogueid"`
}

var Mxometadata sync.Map


type Response struct {
	Conn    net.Conn
	Retcode int32
	Errmsg  string
	Data    []byte
}
type Recieve struct {
	Conn net.Conn
	Buf  []byte
}

var ChListener = make(chan Recieve, 2560)
var ChSender = make(chan Response, 2560)
var ZkHost = []string{"127.0.0.1:2181"}
var Logger *log.Logger
var SqlLogger *log.Logger
