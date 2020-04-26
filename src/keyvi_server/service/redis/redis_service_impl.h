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
 * redis_service_impl.h
 *
 *  Created on: Apr 11, 2020
 *      Author: hendrik
 */

#ifndef KEYVI_SERVER_SERVICE_REDIS_REDIS_SERVICE_IMPL_H_
#define KEYVI_SERVER_SERVICE_REDIS_REDIS_SERVICE_IMPL_H_

#include <brpc/redis.h>

#include <map>
#include <memory>
#include <string>

#include "keyvi_server/core/data_backend.h"

namespace keyvi_server {
namespace service {
namespace redis {

class RedisServiceImpl : public brpc::RedisService {
 public:
  explicit RedisServiceImpl(const keyvi_server::core::data_backend_t& backend);

  bool Set(const std::string& key, const std::string& value);

  bool Get(const std::string& key, std::string* value);

  bool MSet(const std::shared_ptr<std::map<std::string, std::string>>& key_values);

 private:
  keyvi_server::core::data_backend_t backend_;
};

}  // namespace redis
}  // namespace service
}  // namespace keyvi_server

#endif  // KEYVI_SERVER_SERVICE_REDIS_REDIS_SERVICE_IMPL_H_
