
cmake_minimum_required(VERSION 3.16)
project(trafodion)
set(CMAKE_CXX_STANDARD_REQUIRED c++20)

# file(GLOB_RECURSE
#         comexe_SRCS               
#         CONFIGURE_DEPENDS       
#         ${PROJECT_SOURCE_DIR}/comexe/*.cpp
#         ${PROJECT_SOURCE_DIR}/include/*.h
#         )

# add_library(comexe OBJECT ${comexe_SRCS})
# target_include_directories(comexe 
#         PUBLIC 
#         ${PROJECT_SOURCE_DIR}/include
#         /usr/lib/jvm/java-11-openjdk-amd64/include/
#         /usr/lib/jvm/java-11-openjdk-amd64/include/linux
#         )

find_package(FLEX)
find_package(BISON)
BISON_TARGET(sql_yacc ${PROJECT_SOURCE_DIR}/sqlci/sqlci_yacc.y ${PROJECT_SOURCE_DIR}/sqlci_yacc.cpp COMPILE_FLAGS " -p sqlci -l -v -d" DEFINES_FILE ${PROJECT_SOURCE_DIR}/include/sqlci/sqlci_yacc.h)
FLEX_TARGET(sql_lex ${PROJECT_SOURCE_DIR}/sqlci/sqlci_lex.ll ${PROJECT_SOURCE_DIR}/sqlci_lex.cpp COMPILE_FLAGS " -P sqlci -iB")
ADD_FLEX_BISON_DEPENDENCY(sql_lex sql_yacc)


file(GLOB_RECURSE
        sqlcomp_SRCS               
        CONFIGURE_DEPENDS       
        ${PROJECT_SOURCE_DIR}/sqlcomp/*.cpp
        ${PROJECT_SOURCE_DIR}/include/*.h
        )

add_library(sqlcomp OBJECT ${BISON_sql_yacc_OUTPUTS} ${FLEX_sql_lex_OUTPUTS} ${sqlcomp_SRCS})
target_include_directories(sqlcomp 
        PUBLIC 
        ${PROJECT_SOURCE_DIR}/include
        /usr/lib/jvm/java-11-openjdk-amd64/include/
        /usr/lib/jvm/java-11-openjdk-amd64/include/linux
        )


add_executable(sqlci ${PROJECT_SOURCE_DIR}/bin/SqlciMain.cpp)
target_link_libraries(sqlci PUBLIC sqlcomp)