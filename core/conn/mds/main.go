package main

import (
	"log"
	"mds/global"
	"mds/models"
	"mds/newmodels"
	"os"
)

func main() {
	TRAF_LOG := os.Getenv("TRAF_LOG")
	fileName := "mds.log"
	logFile, err := os.OpenFile(TRAF_LOG+"/"+fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	defer logFile.Close()
	if err != nil {
		global.Logger.Fatalln("open file error")
	}
	global.Logger = log.New(logFile, "[Info]", log.LstdFlags)
	global.Logger.Println("A Info message here")
	global.Logger.Println("main start here")

	sqlFileName := "mds_sql.log"
	sqlLogFile, err := os.OpenFile(TRAF_LOG+"/"+sqlFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	defer sqlLogFile.Close()
	if err != nil {
		global.SqlLogger.Fatalln("open file error")
	}
	global.SqlLogger = log.New(sqlLogFile, "", log.LstdFlags)
	/*
	   	test.Wg.Add(1)
	       go test.Producer(ch,1)
	   	test.Wg.Add(1)
	       go test.Customer(ch,1)
	*/
	global.Wg.Add(1)
	//go others.Mxosrvrs()
	//global.Wg.Add(1)
	go models.Listener()
	for i := 0; i < 10; i++ {
		global.Wg.Add(1)
		go newmodels.Worker()
	}
	global.Wg.Add(1)
	go models.Sender()
	global.Wg.Add(1)
	go models.Restserver()
	global.Wg.Wait()

}
