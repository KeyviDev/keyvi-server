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
    explicit GetCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<const char*>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() != 2ul) {
        output->FormatError("Expect 1 arg for 'get', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
      }
      const std::string key(args[1]);
      std::string value;
      if (_rsimpl->Get(key, &value)) {
        output->SetString(value);
      } else {
        output->SetNullString();
      }
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* _rsimpl;
  };

  class SetCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit SetCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<const char*>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() != 3ul) {
        output->FormatError("Expect 2 args for 'set', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
      }
      const std::string key(args[1]);
      const std::string value(args[2]);
      _rsimpl->Set(key, value);
      output->SetStatus("OK");
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* _rsimpl;
  };
};

}  // namespace redis
}  // namespace service
}  // namespace keyvi_server

#endif  // KEYVI_SERVER_SERVICE_REDIS_COMMAND_HANDLER_H_
