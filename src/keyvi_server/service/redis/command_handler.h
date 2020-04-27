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
 * set_command_handler.h
 *
 *  Created on: Apr 11, 2020
 *      Author: hendrik
 */

#ifndef KEYVI_SERVER_SERVICE_REDIS_COMMAND_HANDLER_H_
#define KEYVI_SERVER_SERVICE_REDIS_COMMAND_HANDLER_H_

#include <brpc/redis.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "keyvi_server/service/redis/redis_service_impl.h"

namespace keyvi_server {
namespace service {
namespace redis {

class CommandHandler {
 public:
  class GetCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit GetCommandHandler(RedisServiceImpl* rsimpl) : redis_service_impl_(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<const char*>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() != 2ul) {
        output->FormatError("Expect 1 arg for 'get', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
      }
      const std::string key(args[1]);
      std::string value;
      if (redis_service_impl_->Get(key, &value)) {
        output->SetString(value);
      } else {
        output->SetNullString();
      }
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* redis_service_impl_;
  };

  class SetCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit SetCommandHandler(RedisServiceImpl* rsimpl) : redis_service_impl_(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<const char*>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() != 3ul) {
        output->FormatError("Expect 2 args for 'set', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
      }
      const std::string key(args[1]);
      const std::string value(args[2]);
      redis_service_impl_->Set(key, value);
      output->SetStatus("OK");
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* redis_service_impl_;
  };

  class MSetCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit MSetCommandHandler(RedisServiceImpl* rsimpl) : redis_service_impl_(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<const char*>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() < 3ul || args.size() % 2 != 1) {
        output->FormatError("wrong number of arguments for 'mset' command");
        return brpc::REDIS_CMD_HANDLED;
      }

      // copy the key values into a map
      // note: the vector contains raw pointers into the input buffer which we have no access, too
      // as long as mset works async we have to copy
      std::shared_ptr<std::map<std::string, std::string>> key_values =
          std::make_shared<std::map<std::string, std::string>>();

      for (size_t i = 1; i < args.size();) {
        key_values->emplace(args[i], args[i + 1]);
        i += 2;
      }

      redis_service_impl_->MSet(key_values);
      output->SetStatus("OK");
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* redis_service_impl_;
  };
};

}  // namespace redis
}  // namespace service
}  // namespace keyvi_server

#endif  // KEYVI_SERVER_SERVICE_REDIS_COMMAND_HANDLER_H_
