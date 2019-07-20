/* * keyvi - A key value store.
 *
 * Copyright 2018 Hendrik Muhs<hendrik.muhs@gmail.com>
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
 * os_utils.h
 *
 *  Created on: Mar 21, 2018
 *      Author: hendrik
 */

#ifndef KEYVI_UTIL_OS_UTILS_H_
#define KEYVI_UTIL_OS_UTILS_H_

#include <sys/resource.h>

#include <cstddef>
#include <vector>

namespace keyvi {
namespace util {

class OsUtils {
 public:
  static size_t TryIncreaseFileDescriptors() {
    rlimit limit;

    getrlimit(RLIMIT_NOFILE, &limit);
    if (limit.rlim_cur != limit.rlim_max) {
      // try different limits, as platforms behave differently
      std::vector<size_t> limits{limit.rlim_max, 10000ul, 3200, 1024};

      for (size_t l : limits) {
        limit.rlim_cur = l;
        if (setrlimit(RLIMIT_NOFILE, &limit) == 0) {
          break;
        }
      }
    }
    // read back to return what ever is set now
    getrlimit(RLIMIT_NOFILE, &limit);

    return limit.rlim_cur;
  }
};

} /* namespace util */
} /* namespace keyvi */

#endif  // KEYVI_UTIL_OS_UTILS_H_
