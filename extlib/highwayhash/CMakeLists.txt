add_custom_command(
  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/highwayhash/libhighwayhash.a
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/highwayhash
  COMMAND make HH_CXXFLAGS=-fPIC libhighwayhash.a
  )

add_custom_target(make_hh DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/highwayhash/libhighwayhash.a)
