file(
  GLOB_RECURSE
  srcs
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
)

add_library(
  gpos
      ${srcs}
)

target_include_directories(
  gpos
    PUBLIC
      ${CMAKE_CURRENT_SOURCE_DIR}/include
      ${PROJECT_SOURCE_DIR}/libgpopt/include
      ${PROJECT_SOURCE_DIR}/libnaucrates/include
)

target_link_libraries(
  gpos
    ${CMAKE_DL_LIBS}
)

# Tests reside in the 'server' subdirectory.
add_subdirectory(server)
