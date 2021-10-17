/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Metadata_Server.cpp
 * Author: shahrooz
 *
 * Created on August 26, 2020, 11:17 PM
 */

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include "Util.h"
#include <map>
#include <string>
#include <sys/socket.h>
//#include <stdlib.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <mutex>
#include "../inc/Data_Transfer.h"
#include "../inc/Timestamp.h"
#include <cstdlib>
#include <utility>
#include <sstream>
#include <iostream>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fstream>
#include <json.hpp>

using namespace std;
using json = nlohmann::json;

struct Value{
    uint32_t confid;
    Placement placement;
};

std::mutex lock_t;
std::condition_variable lock_t_cv;
uint64_t counter = 0;

std::vector<DC*> datacenters;

// Value* currConfig;
// Value* nextConfig;
std::map<string, std::pair<Value*, Value*>> keyConfMap;
std::mutex next_config_lock_t;

namespace ABD_helper_propagate_state{
    inline uint32_t number_of_received_responses(vector<bool>& done){
        int ret = 0;
        for(auto it = done.begin(); it != done.end(); it++){
            if(*it){
                ret++;
            }
        }
        return ret;
    }

    void _send_one_server(const string operation, promise <strVec>&& prm, const string key,
                          const Server server, const string current_class, const uint32_t conf_id, const string value = "",
                          const string timestamp = ""){

        DPRINTF(DEBUG_METADATA_SERVER, "started.\n");
        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with conf_id " + to_string(conf_id), DEBUG_METADATA_SERVER);

        strVec data;
        Connect c(server.ip, server.port);
        if(!c.is_connected()){
            prm.set_value(move(data));
            return;
        }

        EASY_LOG_M(string("Connected to ") + server.ip + ":" + to_string(server.port));

        data.push_back(operation); // clear_key, put, get
        data.push_back(key);
        if(operation == "put"){
            if(value.empty()){
                DPRINTF(DEBUG_ABD_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
            }
            data.push_back(timestamp);
            data.push_back(value);
        }
        else if(operation == "get"){

        }
        else if(operation == "clear_key"){

        }
        else{
            assert(false);
        }

        data.push_back(current_class);
        data.push_back(to_string(conf_id));

        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        EASY_LOG_M("request sent");

        data.clear();
        string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
            EASY_LOG_M(string("response received with status: ") + data[0]);
            prm.set_value(move(data));
        }
        else{
            data.clear();
            EASY_LOG_M("response failed");
            prm.set_value(move(data));
        }

        DPRINTF(DEBUG_METADATA_SERVER, "finished successfully. with port: %u\n", server.port);
        return;
    }

    /* This function will be used for all communication.
     * datacenters just have the information for servers
     */
    int failure_support_optimized(const string& operation, const string& key, const string& timestamp, const string& value,
                                  uint32_t quorom_size, vector<uint32_t> servers, vector<DC*>& datacenters,
                                  const string current_class, const uint32_t conf_id, uint32_t timeout_per_request, vector<strVec> &ret){
        DPRINTF(DEBUG_METADATA_SERVER, "started.\n");

        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with quorum size " + to_string(quorom_size), DEBUG_METADATA_SERVER);

        uint32_t total_num_servers = datacenters.size();
        map <uint32_t, future<strVec> > responses; // server_id, future
        vector<bool> done(total_num_servers, false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout
        for(auto it = servers.begin(); it != servers.end(); it++){
            promise <strVec> prm;
            responses.emplace(*it, prm.get_future());
            thread(&_send_one_server, operation, move(prm), key, *(datacenters[*it]->servers[0]),
                        current_class, conf_id, value, timestamp).detach();
        }

        EASY_LOG_M(string("requests were sent to all servers. Number of servers: ") + to_string(servers.size()));

        chrono::system_clock::time_point end = chrono::system_clock::now() +
                chrono::milliseconds(timeout_per_request);
        auto it = responses.begin();
        while(true){
            if(done[it->first]){
                it++;
                if(it == responses.end())
                    it = responses.begin();
                continue;
            }
            if(it->second.valid() && it->second.wait_for(chrono::milliseconds(1)) == future_status::ready){
                DPRINTF(DEBUG_METADATA_SERVER, "step3\n");
                strVec data = it->second.get();
                
                if(data.size() != 0){
                    ret.push_back(data);
                    done[it->first] = true;
                    uint32_t doneCount = number_of_received_responses(done);
                    DPRINTF(DEBUG_METADATA_SERVER, "step4 %ld %d %d\n", data.size(), quorom_size, doneCount);
                    if(doneCount == quorom_size){
                        DPRINTF(DEBUG_METADATA_SERVER, "Responses collected successfully\n");
                        EASY_LOG_M("Responses collected successfully");
                        op_status = 0;
                        break;
                    }
                }
            }

            if(chrono::system_clock::now() > end){
                op_status = -1;
                EASY_LOG_M("Responses collected, FAILURE");
                break;
            }

            it++;
            if(it == responses.end())
                it = responses.begin();
            continue;
        }
        DPRINTF(DEBUG_METADATA_SERVER, "Returning now......\n");
        return op_status;
    }
}

string initConfig(uint32_t curr_conf_id, const Placement& p, const vector<string>& keys) {
    unique_lock<mutex> nc_lock(next_config_lock_t);
    unique_lock<mutex> lock(lock_t);

    counter = 0;
    Value* currConfig = new Value;
    currConfig->confid = curr_conf_id;
    currConfig->placement = p;
    Value* nextConfig = NULL;

    for(uint32_t i = 0; i < keys.size(); i++) {
        keyConfMap[keys[i]].first = currConfig;
        keyConfMap[keys[i]].second = nextConfig;
    }

    return DataTransfer::serializeMDS("OK", "");
}

string getConfig(const string& key) {
    unique_lock<mutex> nc_lock(next_config_lock_t);

    unique_lock<mutex> lock(lock_t);
    counter++;

    DPRINTF(DEBUG_METADATA_SERVER, "getConfig counter=%ld\n", counter);

    // status, msg, key, curr_conf_id, placement, keys
    std::vector<std::string> keys;
    return DataTransfer::serializeMDS("OK", "counter="+to_string(counter), keyConfMap[key].first->confid, keyConfMap[key].first->placement, keys);
}

string doneOperation() {
    unique_lock<mutex> lock(lock_t);
    DPRINTF(DEBUG_METADATA_SERVER, "doneOperation counter=%ld\n", counter);
    
    counter--;

    if(counter == 0) {
       lock_t_cv.notify_one();
       DPRINTF(DEBUG_METADATA_SERVER, "counter=0 all notified\n");
       return DataTransfer::serializeMDS("OK", "counter=0 all notified");
    }

    return DataTransfer::serializeMDS("OK", "counter="+to_string(counter));
}

void readFromOldCfgToWriteToNewCfg(const std::string& key, const Value* nextConfig, promise <bool>&& prm) {
    assert(keyConfMap[key].first->placement.protocol == ABD_PROTOCOL_NAME);

    int op_status = 0;    // 0: Success, -1: timeout

    vector<Timestamp> tss;
    vector<string> vs;
    uint32_t idx = -1;
    vector<strVec> ret;

    DPRINTF(DEBUG_METADATA_SERVER, "calling failure_support_optimized to get (v,t*).\n");
    
    // read (v, t*) for key in oldConfig
    op_status = ABD_helper_propagate_state::failure_support_optimized(
        "get", key, "", "",
        keyConfMap[key].first->placement.quorums[0].Q1.size(), 
        keyConfMap[key].first->placement.servers, 
        datacenters, 
        keyConfMap[key].first->placement.protocol,
        keyConfMap[key].first->confid,
        100000, // TODO pass it in command line args 
        ret);

    DPRINTF(DEBUG_METADATA_SERVER, "get op_status: %d.\n", op_status);
    if(op_status == -1) {
        prm.set_value(false);
        return;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            tss.emplace_back((*it)[1]);
            vs.emplace_back((*it)[2]);
        }
        else{
            DPRINTF(DEBUG_METADATA_SERVER, "Bad message received from server for key : %s\n", key.c_str());
            assert(false);
        }
    }
    idx = Timestamp::max_timestamp3(tss);
    
    // Issue cancel for key in oldConfig
    // op_status = ABD_helper_propagate_state::failure_support_optimized(
    //     "clear_key", key, "", "",
    //     keyConfMap[key].first->placement.servers.size(), // TODO check if we really need to issue cancel to all servers 
    //     keyConfMap[key].first->placement.servers, 
    //     datacenters, 
    //     keyConfMap[key].first->placement.protocol,
    //     keyConfMap[key].first->confid,
    //     100000, // TODO pass it in command line args 
    //     ret);

    // DPRINTF(DEBUG_METADATA_SERVER, "clear_key op_status: %d.\n", op_status);
    // if(op_status == -1) {
    //     prm.set_value(false);
    //     return;
    // }
    // No need to check ret vector for "OK" status

    // Issue write (v, t*) for key in newConfig and wait for Q2 quorum
    op_status = ABD_helper_propagate_state::failure_support_optimized(
        "put", key, tss[idx].get_string(), vs[idx], 
        nextConfig->placement.quorums[0].Q2.size(),
        nextConfig->placement.servers, 
        datacenters, 
        nextConfig->placement.protocol,
        nextConfig->confid,
        100000, // TODO pass it in command line args
        ret);

    DPRINTF(DEBUG_METADATA_SERVER, "put op_status: %d.\n", op_status);
    if(op_status == -1) {
        prm.set_value(false);
        return;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            DPRINTF(DEBUG_METADATA_SERVER, "OK received for key : %s\n", key.c_str());
        }
        else{
            DPRINTF(DEBUG_METADATA_SERVER, "Bad message received from server for key : %s\n", key.c_str());
            assert(false); // Bad message received from server
        }
    }
    
    prm.set_value(true);
    return;
}

string setConfig(uint32_t new_conf_id, const Placement& new_placement, const std::vector<std::string>& keys) {
    unique_lock<mutex> nc_lock(next_config_lock_t);

    for(uint32_t i = 0; i < keys.size(); i++) {
        if(keyConfMap[keys[i]].second != NULL) { // will never occur coz getConfig & setConfig are synchorized with next_config_lock_t mutex
            return DataTransfer::serializeMDS("ERROR", "reconfiguration denied");
        }
    }
    
    Value* nextConfig = new Value;
    nextConfig->confid = new_conf_id;
    nextConfig->placement = new_placement;

    unique_lock<mutex> lock(lock_t);
    DPRINTF(DEBUG_METADATA_SERVER, "setConfig counter=%ld\n", counter);
    while(counter > 0) {
        DPRINTF(DEBUG_METADATA_SERVER, "setConfig waiting counter=%ld\n", counter);
        lock_t_cv.wait(lock);
        DPRINTF(DEBUG_METADATA_SERVER, "setConfig awoke counter=%ld\n", counter);
    }

    map <string, future<bool> > responses;
    for(string key:keys) {
        promise <bool> prm;
        responses.emplace(key, prm.get_future());
        thread(&readFromOldCfgToWriteToNewCfg, key, nextConfig, move(prm)).detach();
        // bool status = readFromOldCfgToWriteToNewCfg(key, nextConfig);
        // assert(status);
    }

    for(auto respit = responses.begin(); respit != responses.end(); respit++) {
        bool ret_obj = respit->second.get();
        assert(ret_obj);
    }

    for(string key:keys) { // one iteration should suffice
        keyConfMap[key].first->confid = new_conf_id;
        keyConfMap[key].first->placement = new_placement;
    }

    delete(nextConfig);
    nextConfig = NULL;

    return DataTransfer::serializeMDS("OK", "reconfiguration successful");
}

void message_handler(int connection, int portid, const std::string& recvd){
    std::string status;
    std::string msg;
    uint32_t conf_id;
    std::vector<std::string> keys;
    int result = 1;
    
    Placement p = DataTransfer::deserializeMDS(recvd, status, msg, conf_id, keys);
    std::string& method = status; // Method: ask/update, key, conf_id
    
    if(method == "init_config"){
        result = DataTransfer::sendMsg(connection, initConfig(conf_id, p, keys));
    }
    else if(method == "get_config"){
        result = DataTransfer::sendMsg(connection, getConfig(keys[0]));
    }
    else if(method == "done_operation"){
        result = DataTransfer::sendMsg(connection, doneOperation());
    }
    else if(method == "set_config"){
        result = DataTransfer::sendMsg(connection, setConfig(conf_id, p, keys));
    }
    // else if(method == "ask"){
    //     result = DataTransfer::sendMsg(connection, ask(key, to_string(curr_conf_id)));
    // }
    // else if(method == "update"){
    //     result = DataTransfer::sendMsg(connection, update(key, to_string(curr_conf_id), to_string(new_conf_id), timestamp, p));
    // }
    else{
        DPRINTF(DEBUG_METADATA_SERVER, "Unknown method is called\n");
        DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Unknown method is called"));
    }
    
    if(result != 1){
        DPRINTF(DEBUG_METADATA_SERVER, "Server Response failed\n");
        DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Server Response failed"));
    }
    
//    shutdown(connection, SHUT_WR);
}

void server_connection(int connection, int portid){

//    int n = 1;
//    int result = setsockopt(connection, SOL_SOCKET, SO_NOSIGPIPE, &n, sizeof(n));
//    if(result < 0){
//        assert(false);
//    }

#ifdef USE_TCP_NODELAY
    int yes = 1;
    int result = setsockopt(connection, IPPROTO_TCP, TCP_NODELAY, (char*) &yes, sizeof(int));
    if(result < 0){
        DPRINTF(DEBUG_METADATA_SERVER, "Error in setsockopt(TCP_NODELAY): %d\n", result);
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
                DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Server Response failed"));
            }
            continue;
        }
        message_handler(connection, portid, recvd);
    }
}

void runServer(const std::string& socket_port, const std::string& socket_ip){
    
    int sock = socket_setup(socket_port, &socket_ip);
    DPRINTF(DEBUG_METADATA_SERVER, "Alive port is %s\n", socket_port.c_str());

    int portid = stoi(socket_port);
    while(1){
        int new_sock = accept(sock, NULL, 0);
        if(new_sock < 0){
            DPRINTF(DEBUG_CAS_Client, "Error: accept: %d, errno is %d\n", new_sock, errno);
        }
        else{
            std::thread cThread([new_sock, portid](){ server_connection(new_sock, portid); });
            cThread.detach();
        }
    }
    
    close(sock);
}

int read_detacenters_info(const string& file){
    ifstream cfg(file);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
            return -1;
        }
    }
    else{
        DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
        return -1;
    }

    for(auto& it : j.items()){
        DC* dc = new DC;
        dc->id = stoui(it.key());

        for(auto& server : it.value()["servers"].items()){
            Server* sv = new Server;
            sv->id = stoui(server.key());
            server.value()["host"].get_to(sv->ip);

            sv->port = stoui(server.value()["port"].get<string>());
            sv->datacenter = dc;
            dc->servers.push_back(sv);
        }

        datacenters.push_back(dc);
    }
    cfg.close();
    return 0;
}

int main(int argc, char** argv){

    signal(SIGPIPE, SIG_IGN);

//    std::cout << "AAAA" << std::endl;
    if(argc != 3){
        std::cout << "Enter the correct number of arguments: " << argv[0] << " <ext_ip> <port_no>" << std::endl;
        return -1;
    }

#ifdef LOCAL_TEST
    assert(read_detacenters_info("./config/local_config.json") == 0);
#else
    assert(read_detacenters_info("./config/auto_test/datacenters_access_info.json") == 0);
#endif

    runServer(argv[2], argv[1]);
    
    return 0;
}




// inline std::string construct_key_metadata(const std::string& key, const std::string& conf_id){
//     std::string ret;
//     ret += key;
//     ret += "!";
//     ret += conf_id;
//     return ret;
// }
// inline std::string construct_key_metadata(const std::string& key, const uint32_t conf_id){
//     return construct_key_metadata(key, to_string(conf_id));
// }
// inline std::string construct_confid_timestamp(const std::string& confid, const std::string& timestamp){
//     return confid + "!" + timestamp;
// }

// string ask(const string& key, const string& confid){ // respond with (requested_confid!new_confid!timestamp, p)
//     DPRINTF(DEBUG_METADATA_SERVER, "started. key: %s, confid: %s\n", key.c_str(), confid.c_str());
    
//     if(confid == "0"){ // exception case.
//         auto it = key_info.find(construct_key_metadata(key, confid));

//         uint32_t saved_conf_id = it->second.newconfid; //stoui(it->second.first.substr(0, it->second.first.find("!")));

//         auto it2 = key_info.find(construct_key_metadata(key, saved_conf_id));
//         return DataTransfer::serializeMDS("OK", "", key, saved_conf_id, it2->second.newconfid, it2->second.timestamp, it2->second.placement);
//     }
    
//     auto it = key_info.find(construct_key_metadata(key, confid));
    
//     if(it == key_info.end()){ // Not found!
//         return DataTransfer::serializeMDS("ERROR", "key with that conf_id does not exist");
//     }

//     return DataTransfer::serializeMDS("OK", "", key, stoui(confid), it->second.newconfid, it->second.timestamp, it->second.placement);
// }

// string update(const string& key, const string& old_confid, const string& new_confid, const string& timestamp, const Placement& p){
//     DPRINTF(DEBUG_METADATA_SERVER, "started. key: %s, old_confid: %s\n", key.c_str(), old_confid.c_str());
//     auto it = key_info.find(construct_key_metadata(key, old_confid));
    
//     if(it == key_info.end()){ // Not found!
//         if(old_confid != new_confid){
//             DPRINTF(DEBUG_METADATA_SERVER, "key %s does not exist and cannot be initialized\n", key.c_str());
//             return DataTransfer::serializeMDS("ERROR", "key does not exist and cannot be initialized");
//         }
//         key_info[construct_key_metadata(key, 0)].newconfid = stoui(old_confid);
//         key_info[construct_key_metadata(key, 0)].timestamp = "";
//         key_info[construct_key_metadata(key, 0)].placement = Placement();
//     }
//     else{
//         if(old_confid == new_confid){
//             return DataTransfer::serializeMDS("WARN", "repetitive message");
//         }
//     }

//     if(timestamp.find("-") == string::npos && timestamp.size() != 0){
//         assert(false);
//     }

//     key_info[construct_key_metadata(key, old_confid)].newconfid = stoui(new_confid);
//     key_info[construct_key_metadata(key, old_confid)].timestamp = timestamp;

//     key_info[construct_key_metadata(key, new_confid)].newconfid = stoui(new_confid);
//     key_info[construct_key_metadata(key, new_confid)].timestamp = "";
//     key_info[construct_key_metadata(key, new_confid)].placement = p;

//     DPRINTF(DEBUG_METADATA_SERVER, "key %s with conf %s updated to conf %s and timestamp \"%s\":\n", key.c_str(),
//             old_confid.c_str(), new_confid.c_str(), timestamp.c_str());
//     return DataTransfer::serializeMDS("OK", "key updated");
// }