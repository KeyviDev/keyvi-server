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
 * data_backend.cpp
 *
 *  Created on: Apr 10, 2020
 *      Author: hendrik
 */

#include "keyvi_server/core/data_backend.h"

#include "keyvi_server/util/executable_finder.h"

namespace keyvi_server {
namespace core {

DataBackend::DataBackend(const std::string& path)
    : index_(path, {{KEYVIMERGER_BIN, util::ExecutableFinder::GetKeyviMergerBin()}}) {}

keyvi::index::Index& DataBackend::GetIndex() { return index_; }

}  // namespace core
}  // namespace keyvi_server
