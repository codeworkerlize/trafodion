package others

import (
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"mds/global"
	"mds/others/packet"
	"net"
	"os"
	"strings"
)
func handleconn(conn net.Conn){
	defer conn.Close()
	defer global.Wg.Done()
	reader := bufio.NewReader(os.Stdin)
	//buf := make([]byte, 1024)
	fmt.Println("fake mxosrvr ")
	for {

		t := packet.MsgType_ZOOKEEPER
		fmt.Println("Input opertaionid  :")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		t2 := packet.OperationId(packet.OperationId_value[strings.ToUpper(input)])
		fmt.Println("Input path  :")
		path, _ := reader.ReadString('\n')
		path = strings.TrimSpace(path)
		fmt.Println("Input data   :")
		data, _ := reader.ReadString('\n')
		data = strings.TrimSpace(data)
		var flag int32 = 0
		var acl int32 = 0

		p := &packet.Message{
			Header: &packet.Header{
				Type: &t,
			},
			Zkbody : &packet.ZkBody{
				Opt:&t2,
				Path:&path,
				Data:[]byte(data),
				Flag:proto.Int32(flag),
				Acl:proto.Int32(acl),
			},
		}
		msgData, err := proto.Marshal(p)

		if err != nil {
			println(err.Error())
		}
		//println(string(msgData))
		if input == "quit" {
			return
		}
		fmt.Printf("mxosrvr send data[%s] to mds\n",p.String())
		conn.Write(msgData)
		buf := make([]byte, 1024)

			lenght, _ := conn.Read(buf)

			msg := &packet.Message{}
			err = proto.Unmarshal(buf[0:lenght], msg)
			fmt.Println("string is " + string(msg.GetZkbody().GetData()) + "    " )


	}

}
func Mxosrvrs(){
	addr := "localhost:8181"
	conn,_:= net.Dial("tcp",addr)
	handleconn(conn)


}
