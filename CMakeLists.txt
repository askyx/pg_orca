cmake_minimum_required(VERSION 3.20)
project(pg_orca LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif(NOT CMAKE_BUILD_TYPE)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

enable_testing()

set(ORCA_COMPILE_OPTIONS -march=native -Werror -Wall)
set(ORCA_LINK_OPTIONS "")

option(ENABLE_COVERAGE "Enable coverage reporting" OFF)

if(ENABLE_COVERAGE)
  if(NOT "${CMAKE_BUILD_TYPE}" STREQUAL "Debug")                      # If coverage is required on non-DEBUG builds,
    message(FATAL_ERROR "Coverage requires a debug build type!")      # Then error out.
  endif()
  set(ORCA_COMPILE_OPTIONS ${ORCA_COMPILE_OPTIONS} --coverage)
  set(ORCA_LINK_OPTIONS ${ORCA_LINK_OPTIONS} --coverage)
endif()
message(STATUS "Coverage ${ENABLE_COVERAGE}")

####################################
# build orca core library
####################################
file(
  GLOB_RECURSE
  ORCA_SRC
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/libnaucrates/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpdbcost/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpopt/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpos/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp 
)

add_library(
  orca_core
    OBJECT
      ${ORCA_SRC}
)

target_compile_options(orca_core
  PUBLIC
    ${ORCA_COMPILE_OPTIONS}
)

target_link_options(orca_core
  PUBLIC
    ${ORCA_LINK_OPTIONS}
)

target_compile_definitions(
  orca_core
    PUBLIC
      GPOS_DEBUG
      USE_CMAKE
)

target_include_directories(
  orca_core
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/libnaucrates/include
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpopt/include
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpdbcost/include
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpos/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src/include
    ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

find_package(PostgreSQL REQUIRED)

add_postgresql_extension(
  pg_orca
    VERSION 1.0
    SOURCES pg_orca.cpp
    SCRIPTS pg_orca--1.0.sql
)

target_link_libraries(pg_orca orca_core)

# clang-format
add_custom_target(format
  COMMAND git diff --name-only --diff-filter=d --cached '*.cpp' '*.h' | xargs clang-format -i
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)