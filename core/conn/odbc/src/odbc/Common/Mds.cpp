#include "Mds.h"
#include <malloc.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h> 
#include <netinet/tcp.h> 
#include <sys/eventfd.h>
#include <poll.h>
#include <errno.h>

namespace mds {
    namespace details {
        class LockGuard {
        public:
            LockGuard(pthread_mutex_t *mtx) : mtx_(mtx) {
                pthread_mutex_lock(mtx_);
            }

            ~LockGuard() {
                pthread_mutex_unlock(mtx_);
            }

        private:
            pthread_mutex_t *mtx_;
        };
    }

    enum Event {
        QUEUEREADY = 1,
        STOP = 0x1fffff,
    };
}

extern SRVR_GLOBAL_Def *srvrGlobal;
extern string myProcName;
extern int myPid;
extern int sqlcount;
extern int totalruntime;
extern int totalconnections;
extern char hostname[256];
void CMdsDeamon::ProtoShutdownCheck() {
	google::protobuf::ShutdownProtobufLibrary();
}
bool CMsgQueue::Empty() {
	return size == 0 ? true:false ;
}
bool CMsgQueue::Ready() {  // if not read bolock
	pthread_mutex_lock(&lock);
	while(size == 0 ){
		pthread_cond_wait(&cond,&lock);
	}
	pthread_mutex_unlock(&lock);
	return true;
}
int CMsgQueue::Size() {
	return size;
}
void CMsgQueue::Push(string  data) {
	pthread_mutex_lock(&lock);
	string * tmp = new string(data);
	q.push(tmp);
	size++;
    // pthread_cond_broadcast(&cond);
	pthread_mutex_unlock(&lock);
    if (daemon_) {
        daemon_->notifyReady();
    }
}
string CMsgQueue::Pop() {
	string * tmp;
	pthread_mutex_lock(&lock);
    while(size == 0 ){
		pthread_cond_wait(&cond,&lock);
	}
	tmp = q.front();
	q.pop();
	size--;
	if(q.empty()) {  // incase memory leak
		queue<string*> emptyq;
		swap(emptyq,q);
		malloc_trim(0);
	}
	   
	pthread_mutex_unlock(&lock);
	string ret(*tmp);
	delete tmp;
	tmp = NULL;
	return ret;
}

MDSPAYLOADSET CMsgQueue::GetPayload() {
#define MDSPAYLOADMAX 500
    MDSPAYLOADSET res;
    mds::details::LockGuard lg(&lock);
    if (q.empty()) {
        return res;
    }
    int n = q.size();
    n = n < MDSPAYLOADMAX ? n : MDSPAYLOADMAX;
    while (n--) {
        res.push_back(q.front());
        q.pop();
    }
    return res;
#undef MDSPAYLAODMAX
}

// --- --- --- --- --- --- --- --- --- --- --- ---
// MDS DAEMON
// --- --- --- --- --- --- --- --- --- --- --- ---
CMdsDeamon::CMdsDeamon() {
    connected = false;
    deleteCount = 0;
    gettimeofday(&mxoStartTime, NULL);
    tid = 0;
    quit = false;
    notify_handler_ = -1;
}

CMdsDeamon::~CMdsDeamon() {
    quit = true;
    if (tid) {
        notifyStop();
        pthread_join(tid, 0);
        close(notify_handler_);
        notify_handler_ = -1;
    }
}

bool  CMdsDeamon::Connected() {
	return connected;
}
void CMdsDeamon::SetTimeLimit(int limit) {
	MXO_WARN("set costtimeLimit to %d ms",limit);
	costtimeLimit = limit;
}
void CMdsDeamon::GetApiStartTs() {
	if(!connected) return;
	gettimeofday(&apiStartTime,NULL);
}
void CMdsDeamon::GetApiEndTs() {
	if(!connected) return;
	gettimeofday(&apiEndTime,NULL);
}
void CMdsDeamon::GetMdsApiStartTs() {
	if(!connected) return;
	gettimeofday(&mdsStartTime,NULL);
}

void CMdsDeamon::GetMdsApiEndTs() {
	if(!connected) return;
	gettimeofday(&mdsEndTime,NULL);
}

void CMdsDeamon::CalcApiCostTime() {
	if(!connected) return;
    long long  cost = (mdsEndTime.tv_sec-mdsStartTime.tv_sec)* 1000 + (mdsEndTime.tv_usec-mdsStartTime.tv_usec)/1000;
    if(cost > 1000)
        MXO_WARN("Mds message api cost time :%lld",cost);
}

void  CMdsDeamon::ExecuteMsg(SRVR_STMT_HDL * pSrvrStmt,int type ,int retcode ,char * sqlstring, IDL_long_long txnID){

	if(!connected) return;
    GetMdsApiStartTs();
  
	string store;

	long long costtime = (apiEndTime.tv_sec-apiStartTime.tv_sec) * 1000 + (apiEndTime.tv_usec -apiStartTime.tv_usec)/ 1000;
	long long runtime = (apiEndTime.tv_sec-mxoStartTime.tv_sec) * 1000 + (apiEndTime.tv_usec -mxoStartTime.tv_usec)/ 1000;
	long long starttime = apiStartTime.tv_sec * 1000 + apiStartTime.tv_usec/ 1000;
	if(costtime < costtimeLimit) return ;
	packet::Message Msg;
	Msg.mutable_mdbody()->set_pid(myPid);
	Msg.mutable_mdbody()->set_mxosrvrname(myProcName);
	Msg.mutable_mdbody()->set_hostname(hostname);
	Msg.mutable_header()->set_type(packet::METADATA);
	Msg.mutable_mdbody()->set_sqlcount(sqlcount);
	Msg.mutable_mdbody()->set_totalruntime(runtime);
	Msg.mutable_mdbody()->set_totalconnections(totalconnections);
	Msg.mutable_mdbody()->set_withquery(1);
	Msg.mutable_mdbody()->mutable_querybody()->set_starttime(starttime);
	Msg.mutable_mdbody()->mutable_querybody()->set_retcode(retcode);
	Msg.mutable_mdbody()->mutable_querybody()->set_sqlstring(sqlstring);
	Msg.mutable_mdbody()->mutable_querybody()->set_querytype(type);
	Msg.mutable_mdbody()->mutable_querybody()->set_costtime(costtime);

	Msg.mutable_mdbody()->mutable_querybody()->set_clientcomputername(srvrGlobal->ClientComputerName);
	Msg.mutable_mdbody()->mutable_querybody()->set_clientappname(srvrGlobal->ApplicationName);
	Msg.mutable_mdbody()->mutable_querybody()->set_clientipaddress(srvrGlobal->ClientIpAddress);
	Msg.mutable_mdbody()->mutable_querybody()->set_clientport(srvrGlobal->ClientPort);
	Msg.mutable_mdbody()->mutable_querybody()->set_qid(pSrvrStmt->sqlUniqueQueryID);
	Msg.mutable_mdbody()->mutable_querybody()->set_txnid(txnID);
    Msg.mutable_mdbody()->mutable_querybody()->set_username(srvrGlobal->QSUserName);
    Msg.mutable_mdbody()->mutable_querybody()->set_dialogueid(srvrGlobal->dialogueId);
	Msg.SerializeToString(&store);
	q.Push(store);
    GetMdsApiEndTs();
    CalcApiCostTime();
	ProtoShutdownCheck();

}

void  CMdsDeamon::IntervalMsg(){
	MxoStartMsg();
}
void CMdsDeamon::InitialDialMsg() {
	if(!connected) return;

    GetMdsApiStartTs();
	string store;


	packet::Message Msg;
	Msg.mutable_mdbody()->set_pid(myPid);
	Msg.mutable_mdbody()->set_mxosrvrname(myProcName);
	Msg.mutable_mdbody()->set_hostname(hostname);
	Msg.mutable_header()->set_type(packet::METADATA);
	Msg.mutable_mdbody()->set_sqlcount(0);
	Msg.mutable_mdbody()->set_totalruntime(0);
	Msg.mutable_mdbody()->set_totalconnections(0);

	Msg.mutable_mdbody()->set_withquery(0);

	Msg.SerializeToString(&store);
	q.Push(store);
    GetMdsApiEndTs();
    CalcApiCostTime();
	ProtoShutdownCheck();

}

void   CMdsDeamon::MxoStartMsg()
{

	if(!connected) return;

    GetMdsApiStartTs();
	string store;

	timeval apiEndTime;
	gettimeofday(&apiEndTime,NULL);
	long long runtime = (apiEndTime.tv_sec-mxoStartTime.tv_sec) * 1000 + (apiEndTime.tv_usec -mxoStartTime.tv_usec)/ 1000;

	packet::Message Msg;
	Msg.mutable_mdbody()->set_pid(myPid);
	Msg.mutable_mdbody()->set_mxosrvrname(myProcName);
	Msg.mutable_mdbody()->set_hostname(hostname);
	Msg.mutable_header()->set_type(packet::METADATA);
	Msg.mutable_mdbody()->set_sqlcount(sqlcount);
	Msg.mutable_mdbody()->set_totalruntime(runtime);
	Msg.mutable_mdbody()->set_totalconnections(totalconnections);
	Msg.mutable_mdbody()->set_withquery(0);

	Msg.SerializeToString(&store);
	q.Push(store);
    GetMdsApiEndTs();
    CalcApiCostTime();

	ProtoShutdownCheck();

}

void   CMdsDeamon::DeamonStart() {
    notify_handler_ = eventfd(0, 0);
    q.RegisterDaemon(this);
	int sendthreadnum = 1;
	for(int i = 0;i<sendthreadnum;i++) {
		int ret = pthread_create(&tid, NULL, MdsDeamon, (void*) this);
		if(ret == -1 ){
			MXO_WARN("pthread_create failed,return");
			return ;
		}
	}

}

void *CMdsDeamon::MdsDeamon(void *arg) {
    CMdsDeamon *_this = (CMdsDeamon *) arg;

    int totalsize = 0;

    MXO_WARN("mds deamon server starting");
    int flag = 1;
    int nErr = 0;
    int clientSocket;
    struct sockaddr_in serverAddr;
    while (!_this->quit) {
        if ((clientSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            break;
        }
        nErr = setsockopt(clientSocket, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(flag));
        if (nErr == -1) {
            MXO_WARN("setsockopt failed");
            break;
        }

        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(8988);
        serverAddr.sin_addr.s_addr = inet_addr("0.0.0.0");

        if (connect(clientSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr)) < 0) {
            sleep(10);
            shutdown(clientSocket, SHUT_RDWR);
            close(clientSocket);
            continue;
        }
        _this->connected = true;

        MXO_WARN("mds deamon server started");
        struct pollfd pfd{};
        pfd.fd = _this->notify_handler_;
        pfd.events = POLLIN;
        for (;;) {
            int rc = poll(&pfd, 1, -1);
            if (rc == -1) {
                if (errno == EAGAIN || errno == EINTR) {
                    continue;
                }
                MXO_WARN("MDSDAEMON:poll failure");
                _this->quit = true;
                break;
            }
            int64_t e = 0;
            read(pfd.fd, &e, sizeof e);
            if (e >= mds::STOP) {
                _this->quit = true;
                break;
            } else {
                MDSPAYLOADSET payload = _this->q.GetPayload();
                if (payload.empty()) {
                    continue;
                }
                MDSBUFFER buffer = MDSDAEMON::PayloadToBuffer(payload);
                rc = send(clientSocket, &buffer[0], buffer.size(), 0);
                if (rc < 0) {
                    shutdown(clientSocket, SHUT_RDWR);
                    close(clientSocket);
                    _this->connected = false;

                    MXO_WARN("mds deamon server send msg length error retry connecting");
                    break;
                }
            }
        }
    }
    return NULL;
}

void MDSDAEMON::notifyReady() {
    NotifyEvent(mds::QUEUEREADY);
}

void MDSDAEMON::notifyStop() {
    NotifyEvent(mds::STOP);
}

void MDSDAEMON::NotifyEvent(int64_t e) {
    if (notify_handler_ != -1) {
        write(notify_handler_, &e, sizeof e);
    }
}

MDSBUFFER MDSDAEMON::PayloadToBuffer(MDSPAYLOADSET &payload) {
#define LENGTHTYPE size_t
    const int N = payload.size();
    MDSBUFFER buffer;
    char *p;
    int i, total;
    for (i = 0, total = 0; i < N; ++i) {
        total += payload[i]->size() + sizeof(LENGTHTYPE);
    }
    buffer.resize(total);
    p = &buffer[0];
    for (i = 0; i < N; ++i) {
        MDSPAYLOAD&s = payload[i];
        LENGTHTYPE sz = s->size();
        memcpy(p, &sz, sizeof(LENGTHTYPE));
        p += sizeof(LENGTHTYPE);
        memcpy(p, s->data(), sz);
        p += sz;
        delete s;
        s = 0;
    }
    return buffer;
#undef LENGTHTYPE
}
