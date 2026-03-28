/**
 * @file calc_service.h
 * @brief Calculator 服务实现
 */

#pragma once
#include "calc.pb.h"
#include "../../include/rpc/rpc_server.hpp"

class CalculatorServiceImpl : public testrpc::Calculator {
public:
    void Add(google::protobuf::RpcController* controller,
             const testrpc::AddRequest* request,
             testrpc::AddResponse* response,
             google::protobuf::Closure* done) override {
        printf("[CalcService] Add(%d + %d)\n", request->a(), request->b());
        fflush(stdout);
        
        response->set_result(request->a() + request->b());
        
        if (done) {
            done->Run();
        }
    }
};
