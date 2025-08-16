cmake_minimum_required(VERSION 3.15)
project(ThePlusTVServer)

set(CMAKE_CXX_STANDARD 17)

# Find necessary packages
find_package(Boost REQUIRED COMPONENTS system thread)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

# Find nlohmann_json (fall back to download if necessary)
find_package(nlohmann_json 3.11.2 QUIET)
if(NOT nlohmann_json_FOUND)
    include(FetchContent)
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.2
    )
    FetchContent_MakeAvailable(nlohmann_json)
endif()

# NEW: Find Crow (fall back to download if necessary)
include(FetchContent)
FetchContent_Declare(
    crow
    GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
    GIT_TAG v1.0.8
)
FetchContent_MakeAvailable(crow)


# Find PostgreSQL libpq headers and library
find_library(LIBPQ_LIBRARY NAMES pq PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib)
find_path(LIBPQ_INCLUDE_DIR NAMES libpq-fe.h PATHS /usr/include/postgresql /usr/include)

if(NOT LIBPQ_LIBRARY OR NOT LIBPQ_INCLUDE_DIR)
    message(FATAL_ERROR "PostgreSQL libpq library or includes not found")
endif()

# Add project directories to the include path
include_directories(
    ${Boost_INCLUDE_DIRS}
    ${LIBPQ_INCLUDE_DIR}
    ${OPENSSL_INCLUDE_DIR}
)

# Executable definition now includes LocationService.cpp
add_executable(ThePlusTVServer
    main.cpp
    LocationService.cpp
)

# Link libraries, including OpenSSL, PostgreSQL, and Crow
target_link_libraries(ThePlusTVServer PRIVATE
    Threads::Threads
    Boost::system
    Boost::thread
    nlohmann_json::nlohmann_json
    ${LIBPQ_LIBRARY}
    OpenSSL::SSL
    OpenSSL::Crypto
    crow
)