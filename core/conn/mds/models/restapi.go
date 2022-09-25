package models

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"mds/global"
	"net/http"
	"os"
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}
type Statistics struct {
	Sqlcount     int32 `json:"sqlcount"`
	Totalconns   int32 `json:"totalconns"`
	Successnsnum int32 `json:"successnum"`
	Failurenum   int32 `json:"failurenum"`
}

type Routes []Route
type Test struct {
	ServerId int32 `json:"serverid"`
}

func NewRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	for _, route := range routes {
		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	return router
}

var routes = Routes{
	Route{
		"TodoIndex",
		"GET",
		"/list",
		ListServer,
	},

	Route{
		"TodoIndex",
		"GET",
		"/listall",
		ListAll,
	},
	Route{
		"TodoIndex",
		"GET",
		"/metrics",
		Metrics,
	},
	Route{
		"TodoShow",
		"GET",
		"/{key}",
		ServerDetail,
	},
}

func ListServer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	var maplen int32 = 0
	global.Mxometadata.Range(func(k, v interface{}) bool {
		maplen++
		return true
	})
	if maplen != 0 {
		if err := json.NewEncoder(w).Encode(global.Mxometadata); err != nil {
			//panic(err)
			return
		}
	} else {
		fmt.Fprint(w, "nothing")
	}
}
func Metrics(w http.ResponseWriter, r *http.Request) {
	//	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	//	w.WriteHeader(http.StatusOK)
	var sqlcount int32
	var conns int32
	var successnums int32
	var failurenum int32
	var maplen int32 = 0
	global.Mxometadata.Range(func(k, v interface{}) bool {
		var md = v.(global.MxoMd)
		sqlcount += md.Sqlcount
		conns += md.Totalconns
		successnums += md.Successnsnum
		failurenum += md.Failurenum
		maplen++
		return true
	})

	if maplen != 0 {
		fmt.Fprintf(w, "esgyn_mxosrvr_total_completed_statements  %d\n", sqlcount)
		fmt.Fprintf(w, "esgyn_mxosrvr_total_successful_statements  %d\n", successnums)
		fmt.Fprintf(w, "esgyn_mxosrvr_total_failed_statements  %d\n", failurenum)
		fmt.Fprintf(w, "esgyn_mxosrvr_total_connections  %d\n", conns)

	} else {
		fmt.Fprint(w, "nothing")
	}
}
func ListAll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	var sqlcount int32
	var conns int32
	var successnums int32
	var failurenum int32
	var maplen int32 = 0
	global.Mxometadata.Range(func(k, v interface{}) bool {
		var md = v.(global.MxoMd)
		sqlcount += md.Sqlcount
		conns += md.Totalconns
		successnums += md.Successnsnum
		failurenum += md.Failurenum
		maplen++
		return true
	})
	if maplen != 0 {
		if err := json.NewEncoder(w).Encode(Statistics{sqlcount, conns, successnums, failurenum}); err != nil {
			//panic(err)
			return
		}
	} else {
		fmt.Fprint(w, "nothing")
	}
}
func ServerDetail(w http.ResponseWriter, r *http.Request) {
	global.Logger.Println("detail here")
	vars := mux.Vars(r)
	TRAF_LOG := os.Getenv("TRAF_LOG")
	mapkey := vars["key"]

	fdwrite, err := os.OpenFile(TRAF_LOG+"/"+mapkey, os.O_RDONLY, 0666)
	defer fdwrite.Close()
	if err != nil {
		global.Logger.Print(err)
		w.Write([]byte("nothing here"))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	var v []global.Query
	err = json.NewDecoder(fdwrite).Decode(&v)
	if err != nil {
		global.Logger.Print(err)
	}
	global.Logger.Print(v)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		//panic(err)
		return
	}
}

func Restserver() {
	router := NewRouter()

	global.Logger.Fatal(http.ListenAndServe(":8989", router))
}
