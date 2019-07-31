cmake_minimum_required(VERSION 2.8)

project(cyber-notifier)
SET(BOOST_ROOT "~/opt/boost")
FIND_PACKAGE(Boost 1.67 REQUIRED COMPONENTS
    system
    iostreams)
if(APPLE)
  include_directories(/usr/local/include)
  link_directories(/usr/local/lib)
endif(APPLE)
# Uncomment to have the build process verbose
#set(CMAKE_VERBOSE_MAKEFILE TRUE)

# Uncomment to have the executable moved to 'build' instead of their respective 'build/xxx' directories
#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

add_definitions(-std=c++14)
add_definitions(-DNATS_HAS_STREAMING)
add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY)
#add_definitions(-DNATS_CONN_STATUS_NO_PREFIX)

# Platform specific settings
if(UNIX)
#   include(GNUInstallDirs)
  if(NOT APPLE)
    set(NATS_EXTRA_LIB "rt")
  endif()
endif(UNIX)


set(outname "cyber-notifier")
if(APPLE)
  set(PROTOBUF_LIB "/usr/local/nats.c/pbuf/lib/darwin/libprotobuf-c.a")
else(APPLE)
  set(PROTOBUF_LIB "/opt/nats.c/pbuf/lib/linux/libprotobuf-c.so")
endif(APPLE)

# Build the executable
add_executable(${outname} ${PROJECT_SOURCE_DIR}/notifier.cpp)

# Link
if(APPLE)
  target_link_libraries(${outname} nats ${PROTOBUF_LIB})
else(APPLE)
  target_link_libraries(${outname} nats ${NATS_EXTRA_LIB} ${PROTOBUF_LIB})
endif(APPLE)

target_link_libraries(${outname} ${CMAKE_THREAD_LIBS_INIT})

