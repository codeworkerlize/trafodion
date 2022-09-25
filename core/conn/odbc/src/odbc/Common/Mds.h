#ifndef MDS_DEFINED
#define MDS_DEFINED
#include <iostream>
#include <queue>
#include <stdlib.h>
#include <unistd.h>
#include "packet.pb.h"
#include "CSrvrStmt.h"
#include <sys/socket.h>
#include <arpa/inet.h>

#define MDSBUFFER std::vector<char>
#define MDSPAYLOAD std::string*
#define MDSPAYLOADSET std::vector<MDSPAYLOAD>
#define MDSDAEMON CMdsDeamon

class MDSDAEMON;

using namespace std;
class CMsgQueue{
	private :
		queue<string *> q;
		//int cap;
		int size;
		MDSDAEMON *daemon_;
		pthread_mutex_t lock;
		pthread_cond_t cond;
	public:
		CMsgQueue():size(0),daemon_(0){
			pthread_mutex_init(&lock,NULL);
			pthread_cond_init(&cond,NULL);
		}
		~CMsgQueue(){
			pthread_mutex_destroy(&lock);
			pthread_cond_destroy(&cond);

		}
		void Push(string data);
		string Pop();
		bool Empty();
		bool Ready();
		int Size();

    void RegisterDaemon(MDSDAEMON *daemon) {
        daemon_ = daemon;
    }

    MDSPAYLOADSET GetPayload();
};
class CMdsDeamon{
	private :
		CMsgQueue q;
		timeval mxoStartTime;
		timeval apiStartTime;
		timeval apiEndTime;
		timeval mdsStartTime;
		timeval mdsEndTime;
		bool connected; // with socket 
		int  costtimeLimit;
		pthread_t tid;
        //packet::Message Msg;
		int deleteCount;
	public:
		void SetTimeLimit(int limit);
		void GetApiStartTs();
		void GetApiEndTs();
        void GetMdsApiStartTs();
        void GetMdsApiEndTs();
        void CalcApiCostTime();
		CMdsDeamon();
		~CMdsDeamon();
		void InitialDialMsg();
		void IntervalMsg();
		void MxoStartMsg();
		void ExecuteMsg(SRVR_STMT_HDL * pSrvrStmt,int type ,int retcode ,char * sqlstring, IDL_long_long txnID);
		bool Connected(); 
		void DeamonStart();
		void ProtoShutdownCheck();
		static void * MdsDeamon(void * arg);
public:
    volatile bool quit;

public:
    void notifyReady();

    void notifyStop();

protected:
    void NotifyEvent(int64_t e);

    static MDSBUFFER PayloadToBuffer(MDSPAYLOADSET &payload);

private:
    int notify_handler_;
};
#endif
