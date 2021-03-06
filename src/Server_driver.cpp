#include <thread>
#include "Data_Server.h"
#include "Data_Transfer.h"
#include <sys/ioctl.h>
#include <unordered_set>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <netinet/tcp.h>
#include "../inc/Util.h"
#include <iostream>
#include <atomic>

using std::string;
using std::to_string;
using std::unique_lock;
using std::mutex;

mutex concurrent_counter_mu;
int concurrent_counter;
int max_concurrent_counter;

void increase_thread_priority(bool increase=true){ // true: increase the priority of the caller thread, false: priority default
    static int default_policy;
    static sched_param default_sch_params;
    static bool default_set = false;

    if(!default_set){
        default_set = true;
        assert(pthread_getschedparam(pthread_self(), &default_policy, &default_sch_params) == 0);
    }

    if(increase){
        sched_param sch_params;
        int policy = SCHED_FIFO;
        sch_params.sched_priority = 2;
        assert(pthread_setschedparam(pthread_self(), policy, &sch_params) == 0);
    }
    else{
        assert(pthread_setschedparam(pthread_self(), default_policy, &default_sch_params) == 0);
    }
}

void message_handler(int connection, DataServer& dataserver, int portid, std::string& recvd){
    EASY_LOG_INIT_M(string("started") + " with port " + to_string(portid));
    int result = 1;

    strVec data = DataTransfer::deserialize(recvd);
    std::string& method = data[0];

    if(method == "put"){
        DPRINTF(DEBUG_ABD_Server, "put, The key is %s ts: %s value_size: %ld class: %s \n", data[1].c_str(), data[2].c_str(), data[3].size(), data[4].c_str());
        // EASY_LOG_M(string("put, The key is ") + data[1] + " ts: " + data[2] + " value_size: " + to_string(data[3].size()) + " class: " + data[4]);
        result = DataTransfer::sendMsg(connection, dataserver.put(data[1], data[3], data[2], data[4], stoul(data[5])));
        DPRINTF(DEBUG_ABD_Server, "put finished");
        // EASY_LOG_M("put finished");
    }
    else if(method == "get"){
        // EASY_LOG_M(string("get, The key is ") + data[1] + " class: " + data[2] + " confid: " + data[3]);
        DPRINTF(DEBUG_ABD_Server, "get, The key is %s class: %s conf_id: %s\n", data[1].c_str(), data[2].c_str(), data[3].c_str());
        std::string phony_timestamp;
        result = DataTransfer::sendMsg(connection, dataserver.get(data[1], phony_timestamp, data[2], stoul(data[3])));
        // EASY_LOG_M("get finished");
        DPRINTF(DEBUG_ABD_Server, "get finished");
    }
    else if(method == "get_timestamp"){
        // EASY_LOG_M(string("get_timestamp, The key is ") + data[1] + " class: " + data[2]);
        DPRINTF(DEBUG_ABD_Server, "get_timestamp, The key is %s class: %s\n", data[1].c_str(), data[2].c_str());
        result = DataTransfer::sendMsg(connection, dataserver.get_timestamp(data[1], data[2], stoul(data[3])));
        // EASY_LOG_M("get_timestamp finished");
        DPRINTF(DEBUG_ABD_Server, "get_timestamp finished");
    }
    else if(method == "clear_key"){
        // EASY_LOG_M(string("clear_key, The key is ") + data[1] + " class: " + data[2]);
        DPRINTF(DEBUG_ABD_Server, "clear_key, The key is %s class: %s\n", data[1].c_str(), data[2].c_str());
        result = DataTransfer::sendMsg(connection, dataserver.clear_key(data[1], data[2], stoul(data[3])));
        // EASY_LOG_M("clear_key finished");
        DPRINTF(DEBUG_ABD_Server, "clear_key finished");
    }
    else{
        EASY_LOG_M("MethodNotFound");
        DataTransfer::sendMsg(connection, DataTransfer::serialize({"MethodNotFound", "Unknown method is called"}));
        EASY_LOG_M("MethodNotFound sent");
    }

    if(result != 1){
        DataTransfer::sendMsg(connection, DataTransfer::serialize({"Failure", "Server Response failed"}));
        EASY_LOG_M("Failure: Server Response failed");
    }
}

void server_connection(int connection, DataServer& dataserver, int portid){

#ifdef USE_TCP_NODELAY
    int yes = 1;
    int result = setsockopt(connection, IPPROTO_TCP, TCP_NODELAY, (char*) &yes, sizeof(int));
    if(result < 0){
        assert(false);
    }
#endif

    while(true){
        std::string recvd;
        int result = DataTransfer::recvMsg(connection, recvd);
        if(result != 1){
            close(connection);
            DPRINTF(DEBUG_METADATA_SERVER, "one connection closed.\n");
            return;
        }
        if(is_warmup_message(recvd)){
            std::string temp = std::string(WARM_UP_MNEMONIC) + get_random_string();
            result = DataTransfer::sendMsg(connection, temp);
            if(result != 1){
                DataTransfer::sendMsg(connection, DataTransfer::serialize({"Failure", "Server Response failed"}));
            }
            continue;
        }

        // Counting the max concurrent ops
//        unique_lock<mutex> ccm(concurrent_counter_mu);
//        concurrent_counter++;
//        if(max_concurrent_counter < concurrent_counter){
//            max_concurrent_counter = concurrent_counter;
//            DPRINTF(DEBUG_METADATA_SERVER, "max concurrent ops on this server is %d\n", max_concurrent_counter);
//        }
//        ccm.unlock();

        message_handler(connection, dataserver, portid, recvd);

        // Counting the max concurrent ops
//        ccm.lock();
//        concurrent_counter--;
//        ccm.unlock();
    }
}


void runServer(std::string& db_name, std::string& socket_port){
    
    DataServer* ds = new DataServer(db_name, socket_setup(socket_port));
    int portid = stoi(socket_port);
    while(1){
        int new_sock = accept(ds->getSocketDesc(), NULL, 0);
        if(new_sock < 0){
            DPRINTF(DEBUG_CAS_Client, "Error: accept: %d, errno is %d\n", new_sock, errno);
        }
        else{
            std::thread cThread([&ds, new_sock, portid](){ server_connection(new_sock, *ds, portid); });
            cThread.detach();
        }
    }
}

void runServer(const std::string& db_name, const std::string& socket_port, const std::string& socket_ip,
        const std::string& metadata_server_ip, const std::string& metadata_server_port){

    DataServer* ds = new DataServer(db_name, socket_setup(socket_port, &socket_ip), metadata_server_ip,
            metadata_server_port);
    int portid = stoi(socket_port);
    while(1){
        int new_sock = accept(ds->getSocketDesc(), NULL, 0);
        if(new_sock < 0){
            DPRINTF(DEBUG_CAS_Client, "Error: accept: %d, errno is %d\n", new_sock, errno);
        }
        else{
            std::thread cThread([&ds, new_sock, portid](){ server_connection(new_sock, *ds, portid); });
            cThread.detach();
        }
    }
    
}

int main(int argc, char** argv){

    signal(SIGPIPE, SIG_IGN);

    concurrent_counter = 0;
    max_concurrent_counter = 0;

    std::vector <std::string> socket_port;
    std::vector <std::string> db_list;
    
    if(argc == 1){
        socket_port = {"10000", "10001", "10002", "10003", "10004", "10005", "10006", "10007", "10008"};
        db_list = {"db1.temp", "db2.temp", "db3.temp", "db4.temp", "db5.temp", "db6.temp", "db7.temp", "db8.temp",
                "db9.temp"};
        for(uint i = 0; i < socket_port.size(); i++){
//        if(socket_port[i] == "10004" || socket_port[i] == "10005"){
//            continue;
//        }
            fflush(stdout);
            if(fork() == 0){
                std::setbuf(stdout, NULL);
                close(1);
                int pid = getpid();
                std::stringstream filename;
                filename << "server_" << pid << "_output.txt";
                FILE* out = fopen(filename.str().c_str(), "w");
                std::setbuf(out, NULL);
                runServer(db_list[i], socket_port[i]);
                exit(0);
            }
        }
    }
    else if(argc == 3){
        socket_port.push_back(argv[1]);
        db_list.push_back(std::string(argv[2]) + ".temp");
//        for(uint i = 0; i < socket_port.size(); i++){
//        if(socket_port[i] == "10004" || socket_port[i] == "10005"){
//            continue;
//        }
        fflush(stdout);
        if(fork() == 0){
        
            close(1);
            int pid = getpid();
            std::stringstream filename;
            filename << "server_" << pid << "_output.txt";
            fopen(filename.str().c_str(), "w");
            runServer(db_list[0], socket_port[0]);
        }
//        }
    }
    else if(argc == 6){
        socket_port.push_back(argv[2]);
        db_list.push_back(std::string(argv[3]) + ".temp");
//        for(uint i = 0; i < socket_port.size(); i++){
//        if(socket_port[i] == "10004" || socket_port[i] == "10005"){
//            continue;
//        }
        fflush(stdout);
        if(fork() == 0){
        
            close(1);
            int pid = getpid();
            std::stringstream filename;
            filename << "server_" << pid << "_output.txt";
            fopen(filename.str().c_str(), "w");
            runServer(db_list[0], socket_port[0], argv[1], argv[4], argv[5]);
        }
//        }
    }
    else{
        std::cout << "Enter the correct number of arguments :  ./Server <port_no> <db_name>" << std::endl;
        std::cout << "Or : ./Server <ext_ip> <port_no> <db_name> <metadate_server_ip> <metadata_server_port>" <<
            std::endl;
        return 0;
    }
    
    std::string ch;
    //Enter quit to exit the thread
    while(ch != "quit"){
        std::cin >> ch;
    }
    
//    std::cout << "Waiting for all detached threads to terminate!" << std::endl;
//    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    return 0;
}
