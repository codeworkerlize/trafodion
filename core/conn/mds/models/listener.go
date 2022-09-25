package models

import (
	"fmt"
	"mds/global"
	"encoding/binary"
	"net"
)
func Handle_conn(conn net.Conn) {

	for {
		lenbuf := make([]byte, 8)
		n, err := conn.Read(lenbuf)
		if n == 0{
			global.Logger.Println("%s has disconnect", conn.RemoteAddr())
			break
		}
		if err != nil{
			fmt.Println(err)
			continue
		}
		len := binary.LittleEndian.Uint32(lenbuf)
		recdata := make([]byte, len)
		n, err = conn.Read(recdata)

		//0 means disconnect
		if n == 0{
			global.Logger.Println("%s has disconnect", conn.RemoteAddr())
			break
		}
		if err != nil{
			fmt.Println(err)
			continue
		}
		//global.Logger.Printf("Receive %d bytes data  from [%s]\n", n, conn.RemoteAddr())
		global.ChListener<- global.Recieve{conn,recdata}
	}
	global.Logger.Println("server conn.close")
	conn.Close()
}
func Listener() {
	global.Logger.Println("Listener routine")
	defer global.Wg.Done()
	global.Logger.Println("server ")
	addr := "0.0.0.0:8988"
	listener,err := net.Listen("tcp",addr)
	if err != nil {
		global.Logger.Fatal(err)
	}
	defer listener.Close()
	for  {
		conn,err := listener.Accept()
		if err != nil {
			global.Logger.Fatal(err)
		}
		global.Logger.Printf("mxosrvr ip:port :%s,connected \n",conn.RemoteAddr())
		go Handle_conn(conn)
	}
}
