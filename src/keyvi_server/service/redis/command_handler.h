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
#include <utility>
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

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() != 2ul) {
        output->FormatError("Expect 1 arg for 'get', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
      }
      const std::string key(args[1].data(), args[1].size());
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

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() != 3ul) {
        output->FormatError("Expect 2 args for 'set', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
      }
      const std::string key(args[1].data(), args[1].size());
      const std::string value(args[2].data(), args[2].size());
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

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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
        key_values->emplace(std::make_pair(args[i].as_string(), args[i + 1].as_string()));
        i += 2;
      }

      redis_service_impl_->MSet(key_values);
      output->SetStatus("OK");
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* redis_service_impl_;
  };

  class SaveCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit SaveCommandHandler(RedisServiceImpl* rsimpl) : redis_service_impl_(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      redis_service_impl_->Save();
      output->SetStatus("OK");
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* redis_service_impl_;
  };

  class DeleteCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit DeleteCommandHandler(RedisServiceImpl* rsimpl) : redis_service_impl_(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() < 2ul) {
        output->FormatError("Expected at least 1 arg for 'del'");
        return brpc::REDIS_CMD_HANDLED;
      }

      int64_t deletes = 0;
      for (size_t i = 1; i < args.size(); ++i) {
        redis_service_impl_->Delete(args[i].as_string());
        ++deletes;
      }

      output->SetInteger(deletes);
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* redis_service_impl_;
  };

  class DumpCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit DumpCommandHandler(RedisServiceImpl* rsimpl) : redis_service_impl_(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() != 2ul) {
        output->FormatError("Expect 1 arg for 'dump', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
      }
      const std::string key(args[1].data(), args[1].size());
      std::string value;
      if (redis_service_impl_->Dump(key, &value)) {
        output->SetString(value);
      } else {
        output->SetNullString();
      }
      return brpc::REDIS_CMD_HANDLED;
    }

   private:
    RedisServiceImpl* redis_service_impl_;
  };

  class ExistsCommandHandler : public brpc::RedisCommandHandler {
   public:
    explicit ExistsCommandHandler(RedisServiceImpl* rsimpl) : redis_service_impl_(rsimpl) {}

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
      if (args.size() < 2ul) {
        output->FormatError("Expected at least 1 arg for 'exists'");
        return brpc::REDIS_CMD_HANDLED;
      }

      int64_t found = 0;
      for (size_t i = 1; i < args.size(); ++i) {
        if (redis_service_impl_->Exists(args[i].as_string())) {
          ++found;
        }
      }

      output->SetInteger(found);
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
