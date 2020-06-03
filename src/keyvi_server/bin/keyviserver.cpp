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

#include <brpc/server.h>
#include <butil/logging.h>

#include <memory>

#include <boost/program_options.hpp>

#include "keyvi_server/core/data_backend.h"
#include "keyvi_server/service/index_impl.h"
#include "keyvi_server/service/redis/command_handler.h"
#include "keyvi_server/service/redis/redis_service_impl.h"

brpc::RedisService* createRedisService(const keyvi_server::core::data_backend_t& backend) {
  keyvi_server::service::redis::RedisServiceImpl* redis_service_impl =
      new keyvi_server::service::redis::RedisServiceImpl(backend);
  redis_service_impl->AddCommandHandler(
      "set", new keyvi_server::service::redis::CommandHandler::SetCommandHandler(redis_service_impl));
  redis_service_impl->AddCommandHandler(
      "mset", new keyvi_server::service::redis::CommandHandler::MSetCommandHandler(redis_service_impl));
  redis_service_impl->AddCommandHandler(
      "get", new keyvi_server::service::redis::CommandHandler::GetCommandHandler(redis_service_impl));
  redis_service_impl->AddCommandHandler(
      "save", new keyvi_server::service::redis::CommandHandler::SaveCommandHandler(redis_service_impl));

  return redis_service_impl;
}

int main(int argc, char** argv) {
  boost::program_options::options_description description("keyviserver options:");
  description.add_options()("help,h", "Display this help message");
  description.add_options()("port,p", boost::program_options::value<int32_t>()->default_value(7586),
                            "TCP Port of the server");
  description.add_options()("internal-port", boost::program_options::value<int32_t>()->default_value(-1),
                            "TCP Port of the builtin services");
  description.add_options()("redis,r", boost::program_options::bool_switch()->default_value(false),
                            "Whether to enable resp (redis protocol)");

  boost::program_options::variables_map vm;

  int32_t port;
  int32_t internal_port;

  try {
    boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(description).run(),
                                  vm);

    boost::program_options::notify(vm);

    if (vm.count("help")) {
      std::cout << description;
      return 0;
    }

    internal_port = vm["internal-port"].as<int32_t>();
    port = vm["port"].as<int32_t>();
  } catch (std::exception& e) {
    std::cout << "ERROR: arguments wrong or missing." << std::endl << std::endl;

    std::cout << e.what() << std::endl << std::endl;
    std::cout << description;

    return 1;
  }
  // Generally you only need one Server.
  brpc::Server server;

  // data backend
  keyvi_server::core::data_backend_t data_backend = std::make_shared<keyvi_server::core::DataBackend>("data");

  // Instance of your service.
  keyvi_server::service::IndexImpl index_service_impl(data_backend);

  // Add the service into server. Notice the second parameter, because the
  // service is put on stack, we don't want server to delete it, otherwise
  // use brpc::SERVER_OWNS_SERVICE.
  if (server.AddService(&index_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    return -1;
  }

  // Start the server.
  brpc::ServerOptions options;

  // for now no idle timeout
  options.idle_timeout_sec = -1;

  // for now unlimited concurrency
  options.max_concurrency = 0;

  options.internal_port = internal_port;

  bool resp = vm.count("redis") ? vm["redis"].as<bool>() : false;
  if (resp) {
    options.redis_service = createRedisService(data_backend);
  }

  if (server.Start(port, &options) != 0) {
    LOG(ERROR) << "Failed to start KeyviServer";
    return -1;
  }

  // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
  server.RunUntilAskedToQuit();

  return 0;
}
