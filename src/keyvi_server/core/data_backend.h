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
 * data_backend.h
 *
 *  Created on: Apr 10, 2020
 *      Author: hendrik
 */

#ifndef KEYVI_SERVER_CORE_DATA_BACKEND_H_
#define KEYVI_SERVER_CORE_DATA_BACKEND_H_

#include <keyvi/index/index.h>

#include <memory>
#include <string>

namespace keyvi_server {
namespace core {

class DataBackend {
 public:
  explicit DataBackend(const std::string& path);

  keyvi::index::Index& GetIndex();

 private:
  keyvi::index::Index index_;
};

using data_backend_t = std::shared_ptr<DataBackend>;

}  // namespace core
}  // namespace keyvi_server

#endif  // KEYVI_SERVER_CORE_DATA_BACKEND_H_
