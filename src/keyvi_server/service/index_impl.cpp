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

namespace keyvi_server {
namespace service {

IndexImpl::IndexImpl(const keyvi_server::core::data_backend_t &backend) : backend_(backend) {}

IndexImpl::~IndexImpl() {}

void IndexImpl::Info(google::protobuf::RpcController *cntl_base, const InfoRequest *request, InfoResponse *response,
                     google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
  (*response->mutable_info())["version"] = "0.0.1";
}

void IndexImpl::Delete(google::protobuf::RpcController *cntl_base, const DeleteRequest *request,
                       EmptyBodyResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  backend_->GetIndex().Delete(request->key());
}

void IndexImpl::Contains(google::protobuf::RpcController *cntl_base, const ContainsRequest *request,
                         ContainsResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  response->set_contains(backend_->GetIndex().Contains(request->key()));
}

void IndexImpl::Get(google::protobuf::RpcController *cntl_base, const GetRequest *request,
                    StringValueResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  keyvi::dictionary::Match match = backend_->GetIndex()[request->key()];

  response->set_value(match.GetValueAsString());
}

void IndexImpl::GetFuzzy(google::protobuf::RpcController *cntl_base, const GetFuzzyRequest *request,
                         GetFuzzyResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  auto matches =
      backend_->GetIndex().GetFuzzy(request->key(), request->max_edit_distance(), request->min_exact_prefix());
  for (auto m : matches) {
    Match *match = response->add_matches();
    match->set_matched_string(m.GetMatchedString());
    match->set_value(m.GetValueAsString());
  }
}

void IndexImpl::GetNear(google::protobuf::RpcController *cntl_base, const GetNearRequest *request,
                        GetNearResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  auto matches = backend_->GetIndex().GetNear(request->key(), request->min_exact_prefix(), request->greedy());
  for (auto m : matches) {
    Match *match = response->add_matches();
    match->set_matched_string(m.GetMatchedString());
    match->set_value(m.GetValueAsString());
  }
}

void IndexImpl::GetRaw(google::protobuf::RpcController *cntl_base, const GetRawRequest *request,
                       StringValueResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  keyvi::dictionary::Match match = backend_->GetIndex()[request->key()];

  response->set_value(match.GetRawValueAsString());
}

void IndexImpl::Set(google::protobuf::RpcController *cntl_base, const SetRequest *request, EmptyBodyResponse *response,
                    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  backend_->GetIndex().Set(request->key(), request->value());
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

  backend_->GetIndex().MSet(key_values);
}

void IndexImpl::Flush(google::protobuf::RpcController *cntl_base, const FlushRequest *request,
                      EmptyBodyResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  backend_->GetIndex().Flush(request->asynchronous());
}

void IndexImpl::ForceMerge(google::protobuf::RpcController *cntl_base, const ForceMergeRequest *request,
                           EmptyBodyResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  backend_->GetIndex().ForceMerge(request->max_segments());
}

}  // namespace service
}  // namespace keyvi_server
