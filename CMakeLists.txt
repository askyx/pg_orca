cmake_minimum_required(VERSION 3.25)
project(gpopt_main LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)

# Check build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif(NOT CMAKE_BUILD_TYPE)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Turn on compiler flags for all warnings if available.
include(CheckCXXCompilerFlag)

check_cxx_compiler_flag("-Wall" COMPILER_HAS_WALL)
if (COMPILER_HAS_WALL)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -fPIC")
endif()

check_cxx_compiler_flag("-Werror" COMPILER_HAS_WERROR)
if (COMPILER_HAS_WERROR)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

check_cxx_compiler_flag("-Wextra" COMPILER_HAS_WEXTRA)
if (COMPILER_HAS_WEXTRA)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif()

check_cxx_compiler_flag("-pedantic-errors" COMPILER_HAS_PEDANTIC_ERRORS)
if (COMPILER_HAS_PEDANTIC_ERRORS)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic-errors")
endif()

# Generate maximum detail for DEBUG information with -g3 if available.
check_cxx_compiler_flag("-g3" COMPILER_HAS_G3)
if (COMPILER_HAS_G3)
  SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3")
  SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -g3")
endif()

# Do not omit frame pointer. Even with RELEASE builds, it is used for
# backtracing.
check_cxx_compiler_flag("-fno-omit-frame-pointer"
                        COMPILER_HAS_FNO_OMIT_FRAME_POINTER)
if (COMPILER_HAS_FNO_OMIT_FRAME_POINTER)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
endif()

# Turn on GPOS_DEBUG define for DEBUG builds.
cmake_policy(SET CMP0043 NEW)

SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DGPOS_DEBUG")

add_compile_options(-DUSE_CMAKE)
# Turn on platform-specific defines.
set(GPOS_${CMAKE_SYSTEM_NAME} 1)
set(GPOS_${CMAKE_SYSTEM_PROCESSOR} 1)

# Library dependencies for optimizer.
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Xerces.
find_package(Xerces REQUIRED)
include_directories(${XERCES_INCLUDE_DIRS})

# GPOS.
include_directories(libgpos/include)

enable_testing()

# Sub-libraries.
add_subdirectory(libgpos)

file(
  GLOB_RECURSE
  naucrates_src
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/libnaucrates/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpdbcost/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpopt/src/*.cpp
)

add_library(
  naucrates
      ${naucrates_src}
)

target_include_directories(
  naucrates
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/libnaucrates/include
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpopt/include
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpdbcost/include
    ${CMAKE_CURRENT_SOURCE_DIR}/libgpos/include
    ${PROJECT_BINARY_DIR}/libgpos/include
)

target_link_libraries(
  naucrates
    ${CMAKE_THREAD_LIBS_INIT}
    ${CMAKE_DL_LIBS}
    gpos
    ${XERCES_LIBRARIES}
    ${GPORCA_SOLARIS_EXTRA_LIBS}
)

# add_subdirectory(libgpopt)

# Tests.
add_subdirectory(server)

# clang-format
add_custom_target(format
  COMMAND git diff --name-only --diff-filter=d --cached '*.cpp' '*.h' | xargs clang-format -i
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

