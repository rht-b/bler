/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Client_Node.cpp
 * Author: shahrooz
 * 
 * Created on August 30, 2020, 5:37 AM
 */

#include "../inc/Client_Node.h"

Client_Node::Client_Node(uint32_t id, uint32_t local_datacenter_id, uint32_t conf_id, uint32_t retry_attempts,
        uint32_t metadata_server_timeout, uint32_t timeout_per_request, std::vector<DC*>& datacenters){
        
    conf_id_in_context = conf_id;
    cas = new CAS_Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout, timeout_per_request,
            datacenters, this);
    abd = new ABD_Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout, timeout_per_request,
            datacenters, this);
}

Client_Node::~Client_Node(){
    if(abd != nullptr){
        delete abd;
        abd = nullptr;
    }
    if(cas != nullptr){
        delete cas;
        cas = nullptr;
    }
}

const uint32_t& Client_Node::get_id() const{
    if(abd != nullptr){
        return abd->get_id();
    }
    // if(cas != nullptr){
    //     return cas->get_id();
    // }
    static uint32_t ret = -1;
    return ret;
}

int Client_Node::update_placement(const std::string& key, const uint32_t conf_id){
    assert(key != "");
    int ret = 0;
    
    uint32_t requested_conf_id;
    uint32_t new_conf_id; // Not usefull for client
    std::string timestamp; // Not usefull for client
    Placement p;

    if(this->cas != nullptr){
        ret = ask_metadata(this->cas->get_metadata_server_ip(), this->cas->get_metadata_server_port(), key,
                           conf_id, requested_conf_id, new_conf_id, timestamp, p, this->cas->get_retry_attempts(),
                           this->cas->get_metadata_server_timeout());
    }
    else if(this->abd != nullptr){
        ret = ask_metadata(this->abd->get_metadata_server_ip(), this->abd->get_metadata_server_port(), key,
                           conf_id, requested_conf_id, new_conf_id, timestamp, p, this->abd->get_retry_attempts(),
                           this->abd->get_metadata_server_timeout());
    }
    else{
        ret = -1;
        return ret;
    }
    
    assert(ret == 0);
    
    keys_info[key] = std::pair<uint32_t, Placement>(requested_conf_id, p);
//    if(p->protocol == CAS_PROTOCOL_NAME){
//        if(this->desc != -1){
//            destroy_liberasure_instance(((this->desc)));
//        }
//        this->desc = create_liberasure_instance(p);
//        DPRINTF(DEBUG_CAS_Client, "desc is %d\n", desc);
//        fflush(stdout);
//    }
    ret = 0;

    assert(p.m != 0);

//    if(p != nullptr) {
//        delete p;
//        p = nullptr;
//    }
    
    DPRINTF(DEBUG_CAS_Client, "finished\n");
    return ret;
}

const Placement& Client_Node::get_placement(const std::string& key, const bool force_update, const uint32_t conf_id){
    assert(key != "");
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    
    if(force_update){
        assert(update_placement(key, conf_id) == 0);
        auto it = this->keys_info.find(key);
        DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
        return it->second.second;
    }
    else{
        auto it = this->keys_info.find(key);
        if(it != this->keys_info.end()){
//            if(it->second.first < conf_id){
            DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
            return it->second.second;
//            }
//            else{
//                assert(update_placement(key, conf_id) == 0);
//                return this->keys_info[key].second;
//            }
        }
        else{
            assert(update_placement(key, conf_id) == 0);
            DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
            return this->keys_info[key].second;
        }
    }
    
}

const Placement& Client_Node::get_placement(const std::string& key, const bool force_update, const std::string& conf_id){
    return this->get_placement(key, force_update, stoul(conf_id));
}

bool Client_Node::getConfigAtMDS(Placement& p) {
    if(this->abd != nullptr){

        DPRINTF(DEBUG_ABD_Client, "metadata_server_ip port is %s %s\n", this->abd->get_metadata_server_ip().c_str(), this->abd->get_metadata_server_port().c_str());

        Connect c(this->abd->get_metadata_server_ip(), this->abd->get_metadata_server_port());
        if(!c.is_connected()){
            DPRINTF(DEBUG_ABD_Client, "connection error\n");
            return false;
        }

        std::string status, msg;
        std::string recvd;
        std::chrono::milliseconds span(this->abd->get_metadata_server_timeout());
        bool flag = false;

        std::promise<std::string> data_set;
        std::future<std::string> data_set_fut = data_set.get_future();
        std::vector<std::string> keys;
        DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("get_config", ""));
        std::future<int> fut = std::async(std::launch::async, DataTransfer::recvMsg_async, *c, std::move(data_set));

        if(data_set_fut.valid()){
//            DPRINTF(DEBUG_CLIENT_NODE, "data_set_fut is valid\n");
            std::future_status aaa = data_set_fut.wait_for(span);
            if(aaa == std::future_status::ready){
                int ret = fut.get();
//                DPRINTF(DEBUG_CLIENT_NODE, "Future ret value is %d\n", ret);
                if(ret == 1){
                    flag = true;
                    recvd = data_set_fut.get();
                }
            }
//            DPRINTF(DEBUG_CLIENT_NODE, "aaaa is %d and to is %d\n", aaa, std::future_status::timeout);
        }
        else{
            DPRINTF(DEBUG_CLIENT_NODE, "data_set_fut is not valid\n");
            return false;
        }

        if(flag){
            status.clear();
            msg.clear();

            p = DataTransfer::deserializeMDS(recvd, status, msg);
            DPRINTF(DEBUG_CLIENT_NODE, "getConfigAtMDS msg %s \n", msg.c_str());
            assert(status == "OK");
        }
        else{
            DPRINTF(DEBUG_CLIENT_NODE, "Metadata server timeout for request: %s\n", msg.c_str());
            return false;
        }
    }
    return true;
}

bool Client_Node::recordDoneOprAtMDS() {
    if(this->abd != nullptr){

        DPRINTF(DEBUG_ABD_Client, "metadata_server_ip port is %s %s\n", this->abd->get_metadata_server_ip().c_str(), this->abd->get_metadata_server_port().c_str());

        Connect c(this->abd->get_metadata_server_ip(), this->abd->get_metadata_server_port());
        if(!c.is_connected()){
            DPRINTF(DEBUG_ABD_Client, "connection error\n");
            return false;
        }

        std::string status, msg;
        std::string recvd;
        std::chrono::milliseconds span(this->abd->get_metadata_server_timeout());
        bool flag = false;

        std::promise<std::string> data_set;
        std::future<std::string> data_set_fut = data_set.get_future();
        std::vector<std::string> keys;
        DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("done_operation", ""));
        std::future<int> fut = std::async(std::launch::async, DataTransfer::recvMsg_async, *c, std::move(data_set));

        if(data_set_fut.valid()){
//            DPRINTF(DEBUG_CLIENT_NODE, "data_set_fut is valid\n");
            std::future_status aaa = data_set_fut.wait_for(span);
            if(aaa == std::future_status::ready){
                int ret = fut.get();
//                DPRINTF(DEBUG_CLIENT_NODE, "Future ret value is %d\n", ret);
                if(ret == 1){
                    flag = true;
                    recvd = data_set_fut.get();
                }
            }
//            DPRINTF(DEBUG_CLIENT_NODE, "aaaa is %d and to is %d\n", aaa, std::future_status::timeout);
        }
        else{
            DPRINTF(DEBUG_CLIENT_NODE, "data_set_fut is not valid\n");
            return false;
        }

        if(flag){
            status.clear();
            msg.clear();

            Placement p = DataTransfer::deserializeMDS(recvd, status, msg);
            assert(status == "OK");
            DPRINTF(DEBUG_CLIENT_NODE, "recordDoneOprAtMDS msg %s \n", msg.c_str());
        }
        else{
            DPRINTF(DEBUG_CLIENT_NODE, "Metadata server timeout for request: %s\n", msg.c_str());
            return false;
        }
    }
    return true;
}

const uint32_t& Client_Node::get_conf_id(const std::string& key){
    return conf_id_in_context;
    // assert(key != "");
    // auto it = this->keys_info.find(key);
    // if(it != this->keys_info.end()){
    //     return it->second.first;
    // }
    // else{
    //     assert(update_placement(key, 0) == 0);
    //     return this->keys_info[key].first;
    // }
}

int Client_Node::put(const std::string& key, const std::string& value){
    // const Placement& p = get_placement(key);
    // if(p.protocol == CAS_PROTOCOL_NAME){
    //     return this->cas->put(key, value);
    // }
    // else{
        return this->abd->put(key, value);
    // }
}


int Client_Node::get(const std::string& key, std::string& value){
    // const Placement& p = get_placement(key);
    // if(p.protocol == CAS_PROTOCOL_NAME){
    //     return this->cas->get(key, value);
    // }
    // else{
        return this->abd->get(key, value);
    // }
}
