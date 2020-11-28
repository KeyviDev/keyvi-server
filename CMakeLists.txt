cmake_minimum_required(VERSION 3.0.2)
project(keyvi-server CXX)

#### Build Type
if (CMAKE_BUILD_TYPE)
  string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UPPER)
endif()

#### Cmake modules
set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/")

#### Compiler Flags ####

# configure C++11
if(NOT CMAKE_VERSION VERSION_LESS 3.1)
    set(CMAKE_CXX_STANDARD 11)
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()

include(FindThreads)
include(FindProtobuf)

protobuf_generate_cpp(PROTO_SRC PROTO_HEADER proto/index.proto)

# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
include_directories(${GPERFTOOLS_INCLUDE_DIR})

# include implementation
include_directories("src")
FILE(GLOB_RECURSE SERVER_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} src/keyvi_server/*.cpp)

# include upstream keyvi
add_subdirectory(src/3rdparty/keyvi EXCLUDE_FROM_ALL)

# BOOST
set (_KEYVI_BOOST_LIBRARIES "program_options" "iostreams" "filesystem" "system" "regex" "thread")
set (_KEYVI_BOOST_LIBRARIES_TEST "unit_test_framework")
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost REQUIRED COMPONENTS ${_KEYVI_BOOST_LIBRARIES} ${_KEYVI_BOOST_LIBRARIES_TEST})
if (NOT Boost_FOUND)
    message(FATAL_ERROR "Can not find Boost")
endif ()


if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(_KEYVI_LINK_LIBRARIES_STATIC "${_KEYVI_LINK_LIBRARIES_STATIC} boost_program_options boost_iostreams boost_filesystem boost_system boost_regex boost_thread-mt")
else ()
    set(_KEYVI_LINK_LIBRARIES_DYNAMIC "${_KEYVI_LINK_LIBRARIES_DYNAMIC} boost_program_options boost_iostreams boost_filesystem boost_system boost_regex boost_thread")
endif ()

#### BRPC ####

add_subdirectory(src/3rdparty/brpc EXCLUDE_FROM_ALL)

add_executable(keyviserver ${SERVER_SOURCES} ${PROTO_SRC} ${PROTO_HEADER})
target_link_libraries(keyviserver
    PUBLIC
        Boost::program_options Boost::iostreams Boost::filesystem Boost::system Boost::regex Boost::thread brpc-shared keyvi
)
# not ideal but the BRPC cmake module does not export includes properly yet
target_include_directories(keyviserver PRIVATE "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/src/3rdparty/brpc/output/include/>" ${OPENSSL_INCLUDE_DIR} ${PROTOBUF_INCLUDE_DIRS})

add_custom_target(merger-bin ALL
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:keyvimerger> ${CMAKE_BINARY_DIR}
    DEPENDS keyvimerger
)

