# Included by the core's modules/CMakeLists.txt after the `modules` target exists.
#
# The core links the MySQL client library privately into its `database` target,
# so its include directories do not reach module code. PlayerbotsDatabase.cpp
# needs the complete MySQLPreparedStatement type (which pulls in mysql.h) and
# Playerbots.cpp uses ER_BAD_DB_ERROR from mysqld_error.h. Linking the imported
# `mysql` target here propagates those include directories to the module build.
target_link_libraries(modules
  PRIVATE
    mysql)

# ToCloud9 supplies libsidecar in the AzerothCore source tree. The bridge is
# optional so the module still builds against the standalone playerbots core.
set(PLAYERBOTS_TOCLOUD9_SIDECAR_DIR "${CMAKE_SOURCE_DIR}/deps/libsidecar")
if(EXISTS "${PLAYERBOTS_TOCLOUD9_SIDECAR_DIR}/include/libsidecar.h" AND
   EXISTS "${PLAYERBOTS_TOCLOUD9_SIDECAR_DIR}/libsidecar.so")
  target_include_directories(modules PRIVATE "${PLAYERBOTS_TOCLOUD9_SIDECAR_DIR}/include")
  target_link_libraries(modules PRIVATE "${PLAYERBOTS_TOCLOUD9_SIDECAR_DIR}/libsidecar.so")
  target_compile_definitions(modules PRIVATE PLAYERBOTS_WITH_TOCLOUD9_SIDECAR=1)
endif()
