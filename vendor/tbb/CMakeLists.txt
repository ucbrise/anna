CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

include(ExternalProject)

ExternalProject_Add(tbb
    URL "https://github.com/01org/tbb/archive/2018_U1.tar.gz"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make compiler=clang stdlib=libc++ std=c++11
    INSTALL_COMMAND ""
)


set(RENAMETBB "rename.x.y.z_release.to.tbb_release")
add_custom_command(OUTPUT  ${RENAMETBB}
  COMMAND  echo "I move " 
  COMMAND  ls ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build | grep _release
  COMMAND  rm  ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build/tbb_release -rf
  COMMAND  mv ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build/*_release   ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build/tbb_release
  COMMAND  echo to
  COMMAND   ls ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build | grep _release
  COMMENT  "rename tbb build dir see @issue https://github.com/ucbrise/anna/issues/4"
 )
add_custom_target(T1 ALL DEPENDS ${RENAMETBB})

set(TBB_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/include PARENT_SCOPE)
set(TBB_LINK_DIRS
  ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build/tbb_release
  PARENT_SCOPE)
set(TBB_LIBRARIES
  ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build/tbb_release/libtbb.so
  PARENT_SCOPE)
