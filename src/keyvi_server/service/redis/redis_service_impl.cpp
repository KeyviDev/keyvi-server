/* keyviserver - A key value store server based on keyvi.
 *
 * Copyright 2020 Hendrik Muhs<hendrik.muhs@gmail.com>
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
 * redis_service_impl.cpp
 *
 *  Created on: Apr 11, 2020
 *      Author: hendrik
 */

#include "keyvi_server/service/redis/redis_service_impl.h"

namespace keyvi_server {
namespace service {
namespace redis {

RedisServiceImpl::RedisServiceImpl(const keyvi_server::core::data_backend_t& backend) : backend_(backend) {}

bool RedisServiceImpl::Set(const std::string& key, const std::string& value) {
  backend_->GetIndex().Set(key, value);
  return true;
}

bool RedisServiceImpl::Get(const std::string& key, std::string* value) {
  keyvi::dictionary::Match match = backend_->GetIndex()[key];

  if (match.IsEmpty()) {
    return false;
  }
  *value = match.GetValueAsString();
  return true;
}

bool RedisServiceImpl::MSet(const std::shared_ptr<std::map<std::string, std::string>>& key_values) {
  backend_->GetIndex().MSet(key_values);
  return true;
}

bool RedisServiceImpl::Save() {
  backend_->GetIndex().Flush();
  return true;
}

}  // namespace redis
}  // namespace service
}  // namespace keyvi_server
