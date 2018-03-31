# Resources:
#  - http://zeromq.org/intro:get-the-software
#  - http://bit.ly/2dK0UBT
#
# Remember to have libtool, pkg-config, build-essential, autoconf, and automake
# installed.

CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

include(ExternalProject)

ExternalProject_Add(zeromqcpp
    GIT_REPOSITORY "https://github.com/zeromq/cppzmq.git"
    GIT_TAG "master"
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

set(ZEROMQCPP_INCLUDE_DIRS
    ${CMAKE_CURRENT_BINARY_DIR}/zeromqcpp-prefix/src/zeromqcpp PARENT_SCOPE)
