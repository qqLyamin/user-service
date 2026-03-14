cmake_minimum_required(VERSION 3.20)

project(user_service LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(user-service
    cmd/user-service/main.cpp
)

if(MSVC)
    target_compile_options(user-service PRIVATE /W4 /EHsc)
else()
    target_compile_options(user-service PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(WIN32)
    target_link_libraries(user-service PRIVATE ws2_32)
endif()
