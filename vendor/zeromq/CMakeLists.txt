# Resources:
#  - http://zeromq.org/intro:get-the-software
#  - http://bit.ly/2dK0UBT
#
# Remember to have libtool, pkg-config, build-essential, autoconf, and automake
# installed.

CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

include(ExternalProject)

SET(CXX "clang++ -stdlib=c++")

ExternalProject_Add(zeromq
    URL "https://github.com/zeromq/zeromq4-1/releases/download/v4.1.5/zeromq-4.1.5.tar.gz"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ./configure
    BUILD_COMMAND make
    INSTALL_COMMAND ""
)

set(ZEROMQ_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/zeromq/include PARENT_SCOPE)
set(ZEROMQ_LINK_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/zeromq/.libs PARENT_SCOPE)