
cmake_minimum_required(VERSION 3.16)
project(trafodion)

file(GLOB_RECURSE
        comexe_SRCS               
        CONFIGURE_DEPENDS       
        ${PROJECT_SOURCE_DIR}/comexe/*.cpp
        ${PROJECT_SOURCE_DIR}/include/*.h
        )

add_library(comexe OBJECT ${comexe_SRCS})
target_include_directories(comexe 
        PUBLIC 
        ${PROJECT_SOURCE_DIR}/include
        /usr/lib/jvm/java-11-openjdk-amd64/include/
        /usr/lib/jvm/java-11-openjdk-amd64/include/linux
        )

file(GLOB_RECURSE
        sqlcomp_SRCS               
        CONFIGURE_DEPENDS       
        ${PROJECT_SOURCE_DIR}/sqlcomp/*.cpp
        ${PROJECT_SOURCE_DIR}/include/*.h
        )

add_library(sqlcomp OBJECT ${sqlcomp_SRCS})
target_include_directories(sqlcomp 
        PUBLIC 
        ${PROJECT_SOURCE_DIR}/include
        /usr/lib/jvm/java-11-openjdk-amd64/include/
        /usr/lib/jvm/java-11-openjdk-amd64/include/linux
        )