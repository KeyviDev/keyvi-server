/* keyviserver - A key value store server based on keyvi.
 *
 * Copyright 2019 Hendrik Muhs<hendrik.muhs@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * IndexImpl.h
 *
 *  Created on: Mar 12, 2019
 *      Author: hendrik
 */

#ifndef KEYVI_SERVER_SERVICE_INDEX_IMPL_H_
#define KEYVI_SERVER_SERVICE_INDEX_IMPL_H_

#include "index.pb.h"  //NOLINT

#include <keyvi/index/index.h>

#include <string>

#include "keyvi_server/core/data_backend.h"

namespace keyvi_server {
namespace service {

class IndexImpl : public Index {
 public:
  explicit IndexImpl(const keyvi_server::core::data_backend_t& backend);
  ~IndexImpl();

  void Info(google::protobuf::RpcController* cntl_base, const InfoRequest* request, InfoResponse* response,
            google::protobuf::Closure* done);
  void Get(google::protobuf::RpcController* cntl_base, const GetRequest* request, GetResponse* response,
           google::protobuf::Closure* done);
  void Set(google::protobuf::RpcController* cntl_base, const SetRequest* request, EmptyBodyResponse* response,
           google::protobuf::Closure* done);
  void MSet(google::protobuf::RpcController* cntl_base, const MSetRequest* request, EmptyBodyResponse* response,
            google::protobuf::Closure* done);
  void Flush(google::protobuf::RpcController* cntl_base, const FlushRequest* request, EmptyBodyResponse* response,
             google::protobuf::Closure* done);
  void ForceMerge(google::protobuf::RpcController* cntl_base, const ForceMergeRequest* request,
                  EmptyBodyResponse* response, google::protobuf::Closure* done);

 private:
  keyvi_server::core::data_backend_t backend_;
};
}  // namespace service
}  // namespace keyvi_server

#endif  // KEYVI_SERVER_SERVICE_INDEX_IMPL_H_
