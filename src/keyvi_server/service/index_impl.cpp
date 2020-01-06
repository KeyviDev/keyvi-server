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
 * IndexImpl.cpp
 *
 *  Created on: Mar 12, 2019
 *      Author: hendrik
 */

#include "keyvi_server/service/index_impl.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <google/protobuf/map.h>

#include <memory>
#include <string>

#include "keyvi_server/util/executable_finder.h"

namespace keyvi_server {
namespace service {

IndexImpl::IndexImpl(std::string name)
    : index_(name, {{KEYVIMERGER_BIN, util::ExecutableFinder::GetKeyviMergerBin()}}) {}

IndexImpl::~IndexImpl() {}

void IndexImpl::Info(google::protobuf::RpcController *cntl_base, const InfoRequest *request, InfoResponse *response,
                     google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
  (*response->mutable_info())["version"] = "0.0.1";
}

void IndexImpl::Get(google::protobuf::RpcController *cntl_base, const GetRequest *request, GetResponse *response,
                    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  keyvi::dictionary::Match match = index_[request->key()];

  response->set_value(match.GetValueAsString());
}

void IndexImpl::Set(google::protobuf::RpcController *cntl_base, const SetRequest *request, EmptyBodyResponse *response,
                    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  index_.Set(request->key(), request->value());
}

void IndexImpl::MSet(google::protobuf::RpcController *cntl_base, const MSetRequest *request,
                     EmptyBodyResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
  std::shared_ptr<google::protobuf::Map<std::string, std::string>> key_values =
      std::make_shared<google::protobuf::Map<std::string, std::string>>();

  // hack: cast to remove const and use map from request
  MSetRequest *request_m = const_cast<MSetRequest *>(request);
  (*request_m->mutable_key_values()).swap(*key_values.get());

  index_.MSet(key_values);
}

void IndexImpl::Flush(google::protobuf::RpcController *cntl_base, const FlushRequest *request,
                      EmptyBodyResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  index_.Flush(request->async());
}

void IndexImpl::ForceMerge(google::protobuf::RpcController *cntl_base, const ForceMergeRequest *request,
                           EmptyBodyResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  index_.ForceMerge(request->max_segments());
}

}  // namespace service
}  // namespace keyvi_server
