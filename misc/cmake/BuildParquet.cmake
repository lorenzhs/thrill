# First, we need Apache Arrow
SET(ARROW_ROOT "${CMAKE_BINARY_DIR}/extlib/arrow")

# adapted from parquet-cpp's cmake_modules/ThirdpartyToolchain.cmake
set(ARROW_INCLUDE_DIR "${ARROW_ROOT}/include")
set(ARROW_LIB_DIR "${ARROW_ROOT}/lib")

if (MSVC)
  set(ARROW_SHARED_LIB "${ARROW_ROOT}/bin/arrow.dll")
  set(ARROW_SHARED_IMPLIB "${ARROW_LIB_DIR}/arrow.lib")
  set(ARROW_STATIC_LIB "${ARROW_LIB_DIR}/arrow_static.lib")
else()
  set(ARROW_SHARED_LIB "${ARROW_LIB_DIR}/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_STATIC_LIB "${ARROW_LIB_DIR}/libarrow.a")
endif()

set(ARROW_CMAKE_ARGS
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_INSTALL_PREFIX=${ARROW_ROOT}
  -DCMAKE_INSTALL_LIBDIR=${ARROW_LIB_DIR}
  -DARROW_JEMALLOC=${THRILL_USE_JEMALLOC}
  -DARROW_IPC=OFF
  -DARROW_WITH_LZ4=ON
  -DARROW_WITH_ZSTD=ON
  -DARROW_BUILD_SHARED=ON
  -DARROW_BUILD_UTILITIES=OFF
  -DARROW_BUILD_TESTS=OFF)

if (CMAKE_VERSION VERSION_GREATER "3.7")
  set(ARROW_CONFIGURE SOURCE_SUBDIR "cpp" CMAKE_ARGS ${ARROW_CMAKE_ARGS})
else()
  set(ARROW_CONFIGURE CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}"
    ${ARROW_CMAKE_ARGS} "${ARROW_ROOT}/src/arrow_ep/cpp")
endif()

# figure out the version to use
if ("$ENV{THRILL_ARROW_VERSION}" STREQUAL "")
  set(ARROW_VERSION "d3226349fc61a0ffbb2139f259053ae787e500c8")
else()
  set(ARROW_VERSION "$ENV{THRILL_ARROW_VERSION}")
endif()
message(STATUS "Building Apache Arrow from commit: ${ARROW_VERSION}")

set(ARROW_URL "https://github.com/apache/arrow/archive/${ARROW_VERSION}.tar.gz")

include(ExternalProject)
ExternalProject_Add(arrow_ep
  URL ${ARROW_URL}
  PREFIX ${ARROW_ROOT}
  ${ARROW_CONFIGURE}
  BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${ARROW_STATIC_LIB}")

# define the libraries that fell out of the external project
add_library(arrow SHARED IMPORTED)
if(MSVC)
  set_target_properties(arrow PROPERTIES IMPORTED_IMPLIB "${ARROW_SHARED_IMPLIB}")
else()
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION "${ARROW_SHARED_LIB}")
endif()
add_library(arrow_static STATIC IMPORTED)
set_target_properties(arrow_static PROPERTIES IMPORTED_LOCATION ${ARROW_STATIC_LIB})
# ...and add their dependencies on arrow_ep
add_dependencies(arrow arrow_ep)
add_dependencies(arrow_static arrow_ep)


## BUILD PARQUET
SET(PARQUET_ROOT "${CMAKE_BINARY_DIR}/extlib/parquet-cpp")
# adapted from parquet-cpp's cmake_modules/ThirdpartyToolchain.cmake
set(PARQUET_INCLUDE_DIR "${PARQUET_ROOT}/include")
set(PARQUET_LIB_DIR "${PARQUET_ROOT}/lib")

if (MSVC)
  set(PARQUET_SHARED_LIB "${PARQUET_ROOT}/bin/parquet.dll")
  set(PARQUET_SHARED_IMPLIB "${PARQUET_LIB_DIR}/parquet.lib")
  set(PARQUET_STATIC_LIB "${PARQUET_LIB_DIR}/parquet_static.lib")
else()
  set(PARQUET_SHARED_LIB "${PARQUET_LIB_DIR}/libparquet${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(PARQUET_STATIC_LIB "${PARQUET_LIB_DIR}/libparquet.a")
endif()

set(PARQUET_CMAKE_ARGS
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_INSTALL_PREFIX=${PARQUET_ROOT}
  -DCMAKE_INSTALL_LIBDIR=${PARQUET_LIB_DIR}
  -DARROW_HOME=${ARROW_ROOT}
  -DPARQUET_BUILD_BENCHMARKS=OFF
  -DPARQUET_BUILD_EXECUTABLES=OFF
  -DPARQUET_BUILD_TESTS=OFF)

if (CMAKE_VERSION VERSION_GREATER "3.7")
  set(PARQUET_CONFIGURE CMAKE_ARGS ${PARQUET_CMAKE_ARGS})
else()
  set(PARQUET_CONFIGURE CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}"
    ${PARQUET_CMAKE_ARGS} "${PARQUET_ROOT}/src/parquet_ep/")
endif()

# figure out the version to use
if ("$ENV{THRILL_PARQUET_VERSION}" STREQUAL "")
  set(PARQUET_VERSION "d5fc8482221c9350dafcce5864e80ad42b853387")
else()
  set(PARQUET_VERSION "$ENV{THRILL_PARQUET_VERSION}")
endif()
message(STATUS "Building Apache Parquet from commit: ${PARQUET_VERSION}")

set(PARQUET_URL "https://github.com/apache/parquet-cpp/archive/${PARQUET_VERSION}.tar.gz")

ExternalProject_Add(parquet_ep
  URL ${PARQUET_URL}
  PREFIX ${PARQUET_ROOT}
  ${PARQUET_CONFIGURE}
  BUILD_BYPRODUCTS "${PARQUET_SHARED_LIB}" "${PARQUET_STATIC_LIB}")
# import build artifacts
add_library(parquet SHARED IMPORTED)
if(MSVC)
  set_target_properties(parquet PROPERTIES IMPORTED_IMPLIB "${PARQUET_SHARED_IMPLIB}")
else()
  set_target_properties(parquet PROPERTIES IMPORTED_LOCATION "${PARQUET_SHARED_LIB}")
endif()
add_library(parquet_static STATIC IMPORTED)
set_target_properties(parquet_static PROPERTIES IMPORTED_LOCATION ${PARQUET_STATIC_LIB})

# parquet_ep depends on libarrow.so
add_dependencies(parquet_ep arrow)
# libparquet.so and libparquet.a depend on parquet_ep
add_dependencies(parquet parquet_ep)
add_dependencies(parquet_static parquet_ep)

# add include directories for parquet and arrow
set(THRILL_INCLUDE_DIRS "${PARQUET_INCLUDE_DIR};${ARROW_INCLUDE_DIR};${THRILL_INCLUDE_DIRS}")
# link against both (dynamically)
set(THRILL_LINK_LIBRARIES parquet arrow ${THRILL_LINK_LIBRARIES})
list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_PARQUET=1")
