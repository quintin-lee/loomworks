# =============================================================================
# loomworks build type definitions
#
# Supported build types: Debug, Release, ASan, TSan, UBSan
# Example: cmake -S . -B build -DCMAKE_BUILD_TYPE=ASan
# =============================================================================

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type (Debug/Release/ASan/TSan/UBSan)" FORCE)
endif()

# Base flags (linting) — always applied
set(_BASE_CFLAGS "-Wall -Wextra -Werror -pedantic")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS   "${_BASE_CFLAGS} -O0 -g")
    set(CMAKE_EXE_LINKER_FLAGS "")

elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_C_FLAGS   "${_BASE_CFLAGS} -O3 -DNDEBUG")
    set(CMAKE_EXE_LINKER_FLAGS "")

elseif(CMAKE_BUILD_TYPE STREQUAL "ASan")
    set(CMAKE_C_FLAGS   "${_BASE_CFLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
    set(CMAKE_EXE_LINKER_FLAGS "-fsanitize=address")

elseif(CMAKE_BUILD_TYPE STREQUAL "TSan")
    set(CMAKE_C_FLAGS   "${_BASE_CFLAGS} -fsanitize=thread -g")
    set(CMAKE_EXE_LINKER_FLAGS "-fsanitize=thread")

elseif(CMAKE_BUILD_TYPE STREQUAL "UBSan")
    set(CMAKE_C_FLAGS   "${_BASE_CFLAGS} -fsanitize=undefined -fno-omit-frame-pointer -g")
    set(CMAKE_EXE_LINKER_FLAGS "-fsanitize=undefined")

else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

message(STATUS "Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "CFLAGS     : ${CMAKE_C_FLAGS}")
message(STATUS "LDFLAGS    : ${CMAKE_EXE_LINKER_FLAGS}")
