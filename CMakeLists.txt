
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

# find_package(FLEX)
# find_package(BISON)
# BISON_TARGET(sql_yacc ${PROJECT_SOURCE_DIR}/core/sqlci/sqlci_yacc.y ${PROJECT_SOURCE_DIR}/sqlci_yacc.cpp COMPILE_FLAGS " -p sqlci -l -v -d" DEFINES_FILE ${PROJECT_SOURCE_DIR}/includecore/sqlci/sqlci_yacc.h)
# FLEX_TARGET(sql_lex ${PROJECT_SOURCE_DIR}/core/sqlci/sqlci_lex.ll ${PROJECT_SOURCE_DIR}/sqlci_lex.cpp COMPILE_FLAGS " -P sqlci -iB")
# ADD_FLEX_BISON_DEPENDENCY(sql_lex sql_yacc)


file(GLOB_RECURSE
        sqlcomp_SRCS               
        CONFIGURE_DEPENDS       
    #     ${PROJECT_SOURCE_DIR}/exp/ExpConvMxcs.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/Formatter.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/InputStmt.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/Log.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/Param.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/Prepare.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/ShellCmd.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciCmd.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciEnv.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciError.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciNode.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciParser.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciReset.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciStats.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlciStmts.cpp 
    # ${PROJECT_SOURCE_DIR}/core/sqlci/SqlCmd.cpp 
    ${PROJECT_SOURCE_DIR}/core/parser/*.cpp
    ${PROJECT_SOURCE_DIR}/core/optimizer/*.cpp
)

#         list(REMOVE_ITEM NOISEPAGE_SRCS ${PROJECT_SOURCE_DIR}/core/executor/*.cpp)
#         list(REMOVE_ITEM NOISEPAGE_SRCS ${PROJECT_SOURCE_DIR}/core/exp/*.cpp)
#         list(REMOVE_ITEM NOISEPAGE_SRCS ${PROJECT_SOURCE_DIR}/core/sort/*.cpp)

# SET(sqlcomp_SRCS "")
# list(APPEND sqlcomp_SRCS
#     ${PROJECT_SOURCE_DIR}/core/sqlcomp/parser.cpp
#     ${PROJECT_SOURCE_DIR}/core/common/NAMemory.cpp
#     core/arkcmp/CmpContext.cpp
# )


# MESSAGE("${sqlcomp_SRCS}")

add_library(sqlcomp OBJECT  ${sqlcomp_SRCS})
target_include_directories(sqlcomp 
        PUBLIC 
        ${PROJECT_SOURCE_DIR}/core/include
        /usr/lib/jvm/java-11-openjdk-amd64/include/
        /usr/lib/jvm/java-11-openjdk-amd64/include/linux
        )

target_compile_features(sqlcomp PUBLIC    
        cxx_std_17                                
        )

target_compile_definitions(sqlcomp PUBLIC
        "-D_DEBUG -DNDEBUG"
)

add_executable(sqlci ${PROJECT_SOURCE_DIR}/core/main/main.cpp)
target_link_libraries(sqlci PUBLIC sqlcomp)