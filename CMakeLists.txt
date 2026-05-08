cmake_minimum_required(VERSION 3.20)

project(user_service LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(user-service
    cmd/user-service/main.cpp
)

option(USER_SERVICE_USE_LIBPQ "Use a persistent libpq connection pool for PostgreSQL access" OFF)

if(MSVC)
    target_compile_options(user-service PRIVATE /W4 /EHsc)
else()
    target_compile_options(user-service PRIVATE -Wall -Wextra -Wpedantic)
    target_link_options(user-service PRIVATE -static-libstdc++ -static-libgcc)
endif()

if(WIN32)
    target_link_libraries(user-service PRIVATE ws2_32)
endif()

if(USER_SERVICE_USE_LIBPQ)
    find_package(PostgreSQL REQUIRED)
    target_compile_definitions(user-service PRIVATE USER_SERVICE_USE_LIBPQ=1)
    target_include_directories(user-service PRIVATE ${PostgreSQL_INCLUDE_DIRS})
    target_link_libraries(user-service PRIVATE ${PostgreSQL_LIBRARIES})
endif()
