################################################################################
# examples/checkers/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(reduce_checker)
thrill_build_prog(reduce_checker_timings)
thrill_build_prog(sort_checker)
thrill_build_prog(word_count_checker)
thrill_build_prog(word_count_checker_timings)
thrill_build_prog(word_count_checker_checkonly)

if(CHECKERS_FULL)
  target_compile_definitions(reduce_checker PUBLIC CHECKERS_FULL)
  target_compile_definitions(reduce_checker_timings PUBLIC CHECKERS_FULL)
  target_compile_definitions(sort_checker PUBLIC CHECKERS_FULL)
  target_compile_definitions(word_count_checker PUBLIC CHECKERS_FULL)
  target_compile_definitions(word_count_checker_timings PUBLIC CHECKERS_FULL)
  target_compile_definitions(word_count_checker_checkonly PUBLIC CHECKERS_FULL)
endif()

################################################################################
