package models

import (
	"github.com/golang/protobuf/proto"
	"mds/global"
	"mds/others/packet"
)

func Sender() {
	global.Logger.Println("Sender routine")
	defer global.Wg.Done()
	//global.Logger.Println("sender ")
	t := packet.MsgType_ZOOKEEPER
	for  {
		client := <-global.ChSender
		p := &packet.MdsReponse{
			Header:&packet.Header{
				Type:&t,
			},
			Zkbody:& packet.ZkResponseBody{
				Retcode:proto.Int32(client.Retcode),
				Errmsg:&client.Errmsg,
				Data:[]byte(client.Errmsg),
			},
		}
		msgData, err := proto.Marshal(p)

		if err != nil {
			println(err.Error())
		}
		client.Conn.Write(msgData)
	}
}
