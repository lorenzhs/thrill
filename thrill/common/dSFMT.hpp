/*******************************************************************************
 * thrill/common/dSFMT.hpp
 *
 * A double-precision SIMD-oriented Fast Mersenne Twister
 *
 * Generates double precision floating point pseudorandom numbers which
 * distribute in the range of [1, 2), [0, 1), (0, 1] and (0, 1), with period
 * 19937
 *
 * Adapted from Mutsuo Saito and Makoto Matsumoto's dSFMT 2.2.3, available at
 * http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/SFMT/ under the following
 * license:
 *
 * Copyright (c) 2007, 2008, 2009 Mutsuo Saito, Makoto Matsumoto and Hiroshima
 * University.
 * Copyright (c) 2011, 2002 Mutsuo Saito, Makoto Matsumoto, Hiroshima University
 * and The University of Tokyo.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials provided
 *    with the distribution.
 *  * Neither the name of the Hiroshima University nor the names of
 *    its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written
 *    permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/


/*
 * inlined: dSFMT.h
 */

#pragma once

#include <thrill/common/logger.hpp>

#include <vector>

namespace thrill {
namespace common {
namespace _dSFMT {

extern "C" {

#include <stdio.h>
#include <assert.h>

#define DSFMT_MEXP 19937

/*-----------------
  BASIC DEFINITIONS
  -----------------*/
/* Mersenne Exponent. The period of the sequence
 *  is a multiple of 2^DSFMT_MEXP-1.
 * #define DSFMT_MEXP 19937 */
/** DSFMT generator has an internal state array of 128-bit integers,
 * and N is its size. */
#define DSFMT_N ((DSFMT_MEXP - 128) / 104 + 1)
/** N32 is the size of internal state array when regarded as an array
 * of 32-bit integers.*/
#define DSFMT_N32 (DSFMT_N * 4)
/** N64 is the size of internal state array when regarded as an array
 * of 64-bit integers.*/
#define DSFMT_N64 (DSFMT_N * 2)

#if !defined(DSFMT_BIG_ENDIAN)
#  if defined(__BYTE_ORDER) && defined(__BIG_ENDIAN)
#    if __BYTE_ORDER == __BIG_ENDIAN
#      define DSFMT_BIG_ENDIAN 1
#    endif
#  elif defined(_BYTE_ORDER) && defined(_BIG_ENDIAN)
#    if _BYTE_ORDER == _BIG_ENDIAN
#      define DSFMT_BIG_ENDIAN 1
#    endif
#  elif defined(__BYTE_ORDER__) && defined(__BIG_ENDIAN__)
#    if __BYTE_ORDER__ == __BIG_ENDIAN__
#      define DSFMT_BIG_ENDIAN 1
#    endif
#  elif defined(BYTE_ORDER) && defined(BIG_ENDIAN)
#    if BYTE_ORDER == BIG_ENDIAN
#      define DSFMT_BIG_ENDIAN 1
#    endif
#  elif defined(__BIG_ENDIAN) || defined(_BIG_ENDIAN) \
    || defined(__BIG_ENDIAN__) || defined(BIG_ENDIAN)
#      define DSFMT_BIG_ENDIAN 1
#  endif
#endif

#if defined(DSFMT_BIG_ENDIAN) && defined(__amd64)
#  undef DSFMT_BIG_ENDIAN
#endif

#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)
#  include <inttypes.h>
#elif defined(_MSC_VER) || defined(__BORLANDC__)
#  if !defined(DSFMT_UINT32_DEFINED) && !defined(SFMT_UINT32_DEFINED)
typedef unsigned int uint32_t;
typedef unsigned __int64 uint64_t;
#    ifndef UINT64_C
#      define UINT64_C(v) (v ## ui64)
#    endif
#    define DSFMT_UINT32_DEFINED
#    if !defined(inline) && !defined(__cplusplus)
#      define inline __inline
#    endif
#  endif
#else
#  include <inttypes.h>
#  if !defined(inline) && !defined(__cplusplus)
#    if defined(__GNUC__)
#      define inline __inline__
#    else
#      define inline
#    endif
#  endif
#endif

#ifndef PRIu64
#  if defined(_MSC_VER) || defined(__BORLANDC__)
#    define PRIu64 "I64u"
#    define PRIx64 "I64x"
#  else
#    define PRIu64 "llu"
#    define PRIx64 "llx"
#  endif
#endif

#ifndef UINT64_C
#  define UINT64_C(v) (v ## ULL)
#endif

/*------------------------------------------
  128-bit SIMD like data type for standard C
  ------------------------------------------*/
#if defined(HAVE_ALTIVEC)
#  if !defined(__APPLE__)
#    include <altivec.h>
#  endif
/** 128-bit data structure */
union W128_T {
    vector unsigned int s;
    uint64_t u[2];
    uint32_t u32[4];
    double d[2];
};

#elif defined(THRILL_HAVE_SSE2)
#  include <emmintrin.h>

/** 128-bit data structure */
union W128_T {
    __m128i si;
    __m128d sd;
    uint64_t u[2];
    uint32_t u32[4];
    double d[2];
};
#else  /* standard C */
/** 128-bit data structure */
union W128_T {
    uint64_t u[2];
    uint32_t u32[4];
    double d[2];
};
#endif

/** 128-bit data type */
typedef union W128_T w128_t;

/** the 128-bit internal state array */
struct DSFMT_T {
    w128_t status[DSFMT_N + 1];
    int idx;
};
typedef struct DSFMT_T dsfmt_t;

/** dsfmt internal state vector */
extern dsfmt_t dsfmt_global_data;
/** dsfmt mexp for check */
extern const int dsfmt_global_mexp;

void dsfmt_gen_rand_all(dsfmt_t *dsfmt);
void dsfmt_fill_array_open_close(dsfmt_t *dsfmt, double array[], int size);
void dsfmt_fill_array_close_open(dsfmt_t *dsfmt, double array[], int size);
void dsfmt_fill_array_open_open(dsfmt_t *dsfmt, double array[], int size);
void dsfmt_fill_array_close1_open2(dsfmt_t *dsfmt, double array[], int size);
void dsfmt_chk_init_gen_rand(dsfmt_t *dsfmt, uint32_t seed, int mexp);
void dsfmt_chk_init_by_array(dsfmt_t *dsfmt, uint32_t init_key[],
                             int key_length, int mexp);
const char *dsfmt_get_idstring(void);
int dsfmt_get_min_array_size(void);

#if defined(__GNUC__)
#  define DSFMT_PRE_INLINE inline static
#  define DSFMT_PST_INLINE __attribute__((always_inline))
#elif defined(_MSC_VER) && _MSC_VER >= 1200
#  define DSFMT_PRE_INLINE __forceinline static
#  define DSFMT_PST_INLINE
#else
#  define DSFMT_PRE_INLINE inline static
#  define DSFMT_PST_INLINE
#endif
DSFMT_PRE_INLINE uint32_t dsfmt_genrand_uint32(dsfmt_t *dsfmt) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_genrand_close1_open2(dsfmt_t *dsfmt)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_genrand_close_open(dsfmt_t *dsfmt)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_genrand_open_close(dsfmt_t *dsfmt)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_genrand_open_open(dsfmt_t *dsfmt)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE uint32_t dsfmt_gv_genrand_uint32(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_gv_genrand_close1_open2(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_gv_genrand_close_open(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_gv_genrand_open_close(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double dsfmt_gv_genrand_open_open(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_gv_fill_array_open_close(double array[], int size)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_gv_fill_array_close_open(double array[], int size)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_gv_fill_array_open_open(double array[], int size)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_gv_fill_array_close1_open2(double array[], int size)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_gv_init_gen_rand(uint32_t seed) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_gv_init_by_array(uint32_t init_key[],
                                             int key_length) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_init_gen_rand(dsfmt_t *dsfmt, uint32_t seed)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void dsfmt_init_by_array(dsfmt_t *dsfmt, uint32_t init_key[],
                                          int key_length) DSFMT_PST_INLINE;

/**
 * This function generates and returns unsigned 32-bit integer.
 * This is slower than SFMT, only for convenience usage.
 * dsfmt_init_gen_rand() or dsfmt_init_by_array() must be called
 * before this function.
 * @param dsfmt dsfmt internal state date
 * @return double precision floating point pseudorandom number
 */
inline static uint32_t dsfmt_genrand_uint32(dsfmt_t *dsfmt) {
    uint32_t r;
    uint64_t *psfmt64 = &dsfmt->status[0].u[0];

    if (dsfmt->idx >= DSFMT_N64) {
        dsfmt_gen_rand_all(dsfmt);
        dsfmt->idx = 0;
    }
    r = psfmt64[dsfmt->idx++] & 0xffffffffU;
    return r;
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range [1, 2).  This is
 * the primitive and faster than generating numbers in other ranges.
 * dsfmt_init_gen_rand() or dsfmt_init_by_array() must be called
 * before this function.
 * @param dsfmt dsfmt internal state date
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_genrand_close1_open2(dsfmt_t *dsfmt) {
    double r;
    double *psfmt64 = &dsfmt->status[0].d[0];

    if (dsfmt->idx >= DSFMT_N64) {
        dsfmt_gen_rand_all(dsfmt);
        dsfmt->idx = 0;
    }
    r = psfmt64[dsfmt->idx++];
    return r;
}

/**
 * This function generates and returns unsigned 32-bit integer.
 * This is slower than SFMT, only for convenience usage.
 * dsfmt_gv_init_gen_rand() or dsfmt_gv_init_by_array() must be called
 * before this function.  This function uses \b global variables.
 * @return double precision floating point pseudorandom number
 */
inline static uint32_t dsfmt_gv_genrand_uint32(void) {
    return dsfmt_genrand_uint32(&dsfmt_global_data);
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range [1, 2).
 * dsfmt_gv_init_gen_rand() or dsfmt_gv_init_by_array() must be called
 * before this function. This function uses \b global variables.
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_gv_genrand_close1_open2(void) {
    return dsfmt_genrand_close1_open2(&dsfmt_global_data);
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range [0, 1).
 * dsfmt_init_gen_rand() or dsfmt_init_by_array() must be called
 * before this function.
 * @param dsfmt dsfmt internal state date
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_genrand_close_open(dsfmt_t *dsfmt) {
    return dsfmt_genrand_close1_open2(dsfmt) - 1.0;
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range [0, 1).
 * dsfmt_gv_init_gen_rand() or dsfmt_gv_init_by_array() must be called
 * before this function. This function uses \b global variables.
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_gv_genrand_close_open(void) {
    return dsfmt_gv_genrand_close1_open2() - 1.0;
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range (0, 1].
 * dsfmt_init_gen_rand() or dsfmt_init_by_array() must be called
 * before this function.
 * @param dsfmt dsfmt internal state date
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_genrand_open_close(dsfmt_t *dsfmt) {
    return 2.0 - dsfmt_genrand_close1_open2(dsfmt);
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range (0, 1].
 * dsfmt_gv_init_gen_rand() or dsfmt_gv_init_by_array() must be called
 * before this function. This function uses \b global variables.
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_gv_genrand_open_close(void) {
    return 2.0 - dsfmt_gv_genrand_close1_open2();
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range (0, 1).
 * dsfmt_init_gen_rand() or dsfmt_init_by_array() must be called
 * before this function.
 * @param dsfmt dsfmt internal state date
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_genrand_open_open(dsfmt_t *dsfmt) {
    double *dsfmt64 = &dsfmt->status[0].d[0];
    union {
        double d;
        uint64_t u;
    } r;

    if (dsfmt->idx >= DSFMT_N64) {
        dsfmt_gen_rand_all(dsfmt);
        dsfmt->idx = 0;
    }
    r.d = dsfmt64[dsfmt->idx++];
    r.u |= 1;
    return r.d - 1.0;
}

/**
 * This function generates and returns double precision pseudorandom
 * number which distributes uniformly in the range (0, 1).
 * dsfmt_gv_init_gen_rand() or dsfmt_gv_init_by_array() must be called
 * before this function. This function uses \b global variables.
 * @return double precision floating point pseudorandom number
 */
inline static double dsfmt_gv_genrand_open_open(void) {
    return dsfmt_genrand_open_open(&dsfmt_global_data);
}

/**
 * This function generates double precision floating point
 * pseudorandom numbers which distribute in the range [1, 2) to the
 * specified array[] by one call. This function is the same as
 * dsfmt_fill_array_close1_open2() except that this function uses
 * \b global variables.
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_fill_array_close1_open2()
 */
inline static void dsfmt_gv_fill_array_close1_open2(double array[], int size) {
    dsfmt_fill_array_close1_open2(&dsfmt_global_data, array, size);
}

/**
 * This function generates double precision floating point
 * pseudorandom numbers which distribute in the range (0, 1] to the
 * specified array[] by one call. This function is the same as
 * dsfmt_gv_fill_array_close1_open2() except the distribution range.
 * This function uses \b global variables.
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_fill_array_close1_open2() and \sa
 * dsfmt_gv_fill_array_close1_open2()
 */
inline static void dsfmt_gv_fill_array_open_close(double array[], int size) {
    dsfmt_fill_array_open_close(&dsfmt_global_data, array, size);
}

/**
 * This function generates double precision floating point
 * pseudorandom numbers which distribute in the range [0, 1) to the
 * specified array[] by one call. This function is the same as
 * dsfmt_gv_fill_array_close1_open2() except the distribution range.
 * This function uses \b global variables.
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_fill_array_close1_open2() \sa
 * dsfmt_gv_fill_array_close1_open2()
 */
inline static void dsfmt_gv_fill_array_close_open(double array[], int size) {
    dsfmt_fill_array_close_open(&dsfmt_global_data, array, size);
}

/**
 * This function generates double precision floating point
 * pseudorandom numbers which distribute in the range (0, 1) to the
 * specified array[] by one call. This function is the same as
 * dsfmt_gv_fill_array_close1_open2() except the distribution range.
 * This function uses \b global variables.
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_fill_array_close1_open2() \sa
 * dsfmt_gv_fill_array_close1_open2()
 */
inline static void dsfmt_gv_fill_array_open_open(double array[], int size) {
    dsfmt_fill_array_open_open(&dsfmt_global_data, array, size);
}

/**
 * This function initializes the internal state array with a 32-bit
 * integer seed.
 * @param dsfmt dsfmt state vector.
 * @param seed a 32-bit integer used as the seed.
 */
inline static void dsfmt_init_gen_rand(dsfmt_t *dsfmt, uint32_t seed) {
    dsfmt_chk_init_gen_rand(dsfmt, seed, DSFMT_MEXP);
}

/**
 * This function initializes the internal state array with a 32-bit
 * integer seed. This function uses \b global variables.
 * @param seed a 32-bit integer used as the seed.
 * see also \sa dsfmt_init_gen_rand()
 */
inline static void dsfmt_gv_init_gen_rand(uint32_t seed) {
    dsfmt_init_gen_rand(&dsfmt_global_data, seed);
}

/**
 * This function initializes the internal state array,
 * with an array of 32-bit integers used as the seeds.
 * @param dsfmt dsfmt state vector
 * @param init_key the array of 32-bit integers, used as a seed.
 * @param key_length the length of init_key.
 */
inline static void dsfmt_init_by_array(dsfmt_t *dsfmt, uint32_t init_key[],
                                       int key_length) {
    dsfmt_chk_init_by_array(dsfmt, init_key, key_length, DSFMT_MEXP);
}

/**
 * This function initializes the internal state array,
 * with an array of 32-bit integers used as the seeds.
 * This function uses \b global variables.
 * @param init_key the array of 32-bit integers, used as a seed.
 * @param key_length the length of init_key.
 * see also \sa dsfmt_init_by_array()
 */
inline static void dsfmt_gv_init_by_array(uint32_t init_key[], int key_length) {
    dsfmt_init_by_array(&dsfmt_global_data, init_key, key_length);
}

#if !defined(DSFMT_DO_NOT_USE_OLD_NAMES)
DSFMT_PRE_INLINE const char *get_idstring(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE int get_min_array_size(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void init_gen_rand(uint32_t seed) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void init_by_array(uint32_t init_key[], int key_length)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double genrand_close1_open2(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double genrand_close_open(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double genrand_open_close(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE double genrand_open_open(void) DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void fill_array_open_close(double array[], int size)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void fill_array_close_open(double array[], int size)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void fill_array_open_open(double array[], int size)
    DSFMT_PST_INLINE;
DSFMT_PRE_INLINE void fill_array_close1_open2(double array[], int size)
    DSFMT_PST_INLINE;

/**
 * This function is just the same as dsfmt_get_idstring().
 * @return id string.
 * see also \sa dsfmt_get_idstring()
 */
inline static const char *get_idstring(void) {
    return dsfmt_get_idstring();
}

/**
 * This function is just the same as dsfmt_get_min_array_size().
 * @return minimum size of array used for fill_array functions.
 * see also \sa dsfmt_get_min_array_size()
 */
inline static int get_min_array_size(void) {
    return dsfmt_get_min_array_size();
}

/**
 * This function is just the same as dsfmt_gv_init_gen_rand().
 * @param seed a 32-bit integer used as the seed.
 * see also \sa dsfmt_gv_init_gen_rand(), \sa dsfmt_init_gen_rand().
 */
inline static void init_gen_rand(uint32_t seed) {
    dsfmt_gv_init_gen_rand(seed);
}

/**
 * This function is just the same as dsfmt_gv_init_by_array().
 * @param init_key the array of 32-bit integers, used as a seed.
 * @param key_length the length of init_key.
 * see also \sa dsfmt_gv_init_by_array(), \sa dsfmt_init_by_array().
 */
inline static void init_by_array(uint32_t init_key[], int key_length) {
    dsfmt_gv_init_by_array(init_key, key_length);
}

/**
 * This function is just the same as dsfmt_gv_genrand_close1_open2().
 * @return double precision floating point number.
 * see also \sa dsfmt_genrand_close1_open2() \sa
 * dsfmt_gv_genrand_close1_open2()
 */
inline static double genrand_close1_open2(void) {
    return dsfmt_gv_genrand_close1_open2();
}

/**
 * This function is just the same as dsfmt_gv_genrand_close_open().
 * @return double precision floating point number.
 * see also \sa dsfmt_genrand_close_open() \sa
 * dsfmt_gv_genrand_close_open()
 */
inline static double genrand_close_open(void) {
    return dsfmt_gv_genrand_close_open();
}

/**
 * This function is just the same as dsfmt_gv_genrand_open_close().
 * @return double precision floating point number.
 * see also \sa dsfmt_genrand_open_close() \sa
 * dsfmt_gv_genrand_open_close()
 */
inline static double genrand_open_close(void) {
    return dsfmt_gv_genrand_open_close();
}

/**
 * This function is just the same as dsfmt_gv_genrand_open_open().
 * @return double precision floating point number.
 * see also \sa dsfmt_genrand_open_open() \sa
 * dsfmt_gv_genrand_open_open()
 */
inline static double genrand_open_open(void) {
    return dsfmt_gv_genrand_open_open();
}

/**
 * This function is juset the same as dsfmt_gv_fill_array_open_close().
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_gv_fill_array_open_close(), \sa
 * dsfmt_fill_array_close1_open2(), \sa
 * dsfmt_gv_fill_array_close1_open2()
 */
inline static void fill_array_open_close(double array[], int size) {
    dsfmt_gv_fill_array_open_close(array, size);
}

/**
 * This function is juset the same as dsfmt_gv_fill_array_close_open().
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_gv_fill_array_close_open(), \sa
 * dsfmt_fill_array_close1_open2(), \sa
 * dsfmt_gv_fill_array_close1_open2()
 */
inline static void fill_array_close_open(double array[], int size) {
    dsfmt_gv_fill_array_close_open(array, size);
}

/**
 * This function is juset the same as dsfmt_gv_fill_array_open_open().
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_gv_fill_array_open_open(), \sa
 * dsfmt_fill_array_close1_open2(), \sa
 * dsfmt_gv_fill_array_close1_open2()
 */
inline static void fill_array_open_open(double array[], int size) {
    dsfmt_gv_fill_array_open_open(array, size);
}

/**
 * This function is juset the same as dsfmt_gv_fill_array_close1_open2().
 * @param array an array where pseudorandom numbers are filled
 * by this function.
 * @param size the number of pseudorandom numbers to be generated.
 * see also \sa dsfmt_fill_array_close1_open2(), \sa
 * dsfmt_gv_fill_array_close1_open2()
 */
inline static void fill_array_close1_open2(double array[], int size) {
    dsfmt_gv_fill_array_close1_open2(array, size);
}
#endif /* DSFMT_DO_NOT_USE_OLD_NAMES */


/*
 * inlined: dSFMT-params.h
 */
#define DSFMT_LOW_MASK  UINT64_C(0x000FFFFFFFFFFFFF)
#define DSFMT_HIGH_CONST UINT64_C(0x3FF0000000000000)
#define DSFMT_SR	12

/* for sse2 */
#if defined(THRILL_HAVE_SSE2)
  #define SSE2_SHUFF 0x1b
#elif defined(HAVE_ALTIVEC)
  #if defined(__APPLE__)  /* For OSX */
    #define ALTI_SR (vector unsigned char)(4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4)
    #define ALTI_SR_PERM \
        (vector unsigned char)(15,0,1,2,3,4,5,6,15,8,9,10,11,12,13,14)
    #define ALTI_SR_MSK \
        (vector unsigned int)(0x000fffffU,0xffffffffU,0x000fffffU,0xffffffffU)
    #define ALTI_PERM \
        (vector unsigned char)(12,13,14,15,8,9,10,11,4,5,6,7,0,1,2,3)
  #else
    #define ALTI_SR      {4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4}
    #define ALTI_SR_PERM {15,0,1,2,3,4,5,6,15,8,9,10,11,12,13,14}
    #define ALTI_SR_MSK  {0x000fffffU,0xffffffffU,0x000fffffU,0xffffffffU}
    #define ALTI_PERM    {12,13,14,15,8,9,10,11,4,5,6,7,0,1,2,3}
  #endif
#endif

/*
 * inlined: dSFMT-params19937.h
 */

/* #define DSFMT_N	191 */
/* #define DSFMT_MAXDEGREE	19992 */
#define DSFMT_POS1	117
#define DSFMT_SL1	19
#define DSFMT_MSK1	UINT64_C(0x000ffafffffffb3f)
#define DSFMT_MSK2	UINT64_C(0x000ffdfffc90fffd)
#define DSFMT_MSK32_1	0x000ffaffU
#define DSFMT_MSK32_2	0xfffffb3fU
#define DSFMT_MSK32_3	0x000ffdffU
#define DSFMT_MSK32_4	0xfc90fffdU
#define DSFMT_FIX1	UINT64_C(0x90014964b32f4329)
#define DSFMT_FIX2	UINT64_C(0x3b8d12ac548a7c7a)
#define DSFMT_PCV1	UINT64_C(0x3d84e1ac0dc82880)
#define DSFMT_PCV2	UINT64_C(0x0000000000000001)
#define DSFMT_IDSTR	"dSFMT2-19937:117-19:ffafffffffb3f-ffdfffc90fffd"


/* PARAMETERS FOR ALTIVEC */
#if defined(__APPLE__)	/* For OSX */
    #define ALTI_SL1 	(vector unsigned char)(3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3)
    #define ALTI_SL1_PERM \
	(vector unsigned char)(2,3,4,5,6,7,30,30,10,11,12,13,14,15,0,1)
    #define ALTI_SL1_MSK \
	(vector unsigned int)(0xffffffffU,0xfff80000U,0xffffffffU,0xfff80000U)
    #define ALTI_MSK	(vector unsigned int)(DSFMT_MSK32_1, \
			DSFMT_MSK32_2, DSFMT_MSK32_3, DSFMT_MSK32_4)
#else	/* For OTHER OSs(Linux?) */
    #define ALTI_SL1 	{3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3}
    #define ALTI_SL1_PERM \
	{2,3,4,5,6,7,30,30,10,11,12,13,14,15,0,1}
    #define ALTI_SL1_MSK \
	{0xffffffffU,0xfff80000U,0xffffffffU,0xfff80000U}
    #define ALTI_MSK \
	{DSFMT_MSK32_1, DSFMT_MSK32_2, DSFMT_MSK32_3, DSFMT_MSK32_4}
#endif

/*
 * inlined: dSFMT-common.h
 */

#if defined(THRILL_HAVE_SSE2)
#  include <emmintrin.h>
union X128I_T {
    uint64_t u[2];
    __m128i  i128;
};
union X128D_T {
    double d[2];
    __m128d d128;
};
/** mask data for sse2 */
static const union X128I_T sse2_param_mask = {{DSFMT_MSK1, DSFMT_MSK2}};
#endif

#if defined(HAVE_ALTIVEC)
inline static void do_recursion(w128_t *r, w128_t *a, w128_t * b,
				w128_t *lung) {
    const vector unsigned char sl1 = ALTI_SL1;
    const vector unsigned char sl1_perm = ALTI_SL1_PERM;
    const vector unsigned int sl1_msk = ALTI_SL1_MSK;
    const vector unsigned char sr1 = ALTI_SR;
    const vector unsigned char sr1_perm = ALTI_SR_PERM;
    const vector unsigned int sr1_msk = ALTI_SR_MSK;
    const vector unsigned char perm = ALTI_PERM;
    const vector unsigned int msk1 = ALTI_MSK;
    vector unsigned int w, x, y, z;

    z = a->s;
    w = lung->s;
    x = vec_perm(w, (vector unsigned int)perm, perm);
    y = vec_perm(z, (vector unsigned int)sl1_perm, sl1_perm);
    y = vec_sll(y, sl1);
    y = vec_and(y, sl1_msk);
    w = vec_xor(x, b->s);
    w = vec_xor(w, y);
    x = vec_perm(w, (vector unsigned int)sr1_perm, sr1_perm);
    x = vec_srl(x, sr1);
    x = vec_and(x, sr1_msk);
    y = vec_and(w, msk1);
    z = vec_xor(z, y);
    r->s = vec_xor(z, x);
    lung->s = w;
}
#elif defined(THRILL_HAVE_SSE2)
/**
 * This function represents the recursion formula.
 * @param r output 128-bit
 * @param a a 128-bit part of the internal state array
 * @param b a 128-bit part of the internal state array
 * @param d a 128-bit part of the internal state array (I/O)
 */
inline static void do_recursion(w128_t *r, w128_t *a, w128_t *b, w128_t *u) {
    __m128i v, w, x, y, z;

    x = a->si;
    z = _mm_slli_epi64(x, DSFMT_SL1);
    y = _mm_shuffle_epi32(u->si, SSE2_SHUFF);
    z = _mm_xor_si128(z, b->si);
    y = _mm_xor_si128(y, z);

    v = _mm_srli_epi64(y, DSFMT_SR);
    w = _mm_and_si128(y, sse2_param_mask.i128);
    v = _mm_xor_si128(v, x);
    v = _mm_xor_si128(v, w);
    r->si = v;
    u->si = y;
}
#else
/**
 * This function represents the recursion formula.
 * @param r output 128-bit
 * @param a a 128-bit part of the internal state array
 * @param b a 128-bit part of the internal state array
 * @param lung a 128-bit part of the internal state array (I/O)
 */
inline static void do_recursion(w128_t *r, w128_t *a, w128_t * b,
				w128_t *lung) {
    uint64_t t0, t1, L0, L1;

    t0 = a->u[0];
    t1 = a->u[1];
    L0 = lung->u[0];
    L1 = lung->u[1];
    lung->u[0] = (t0 << DSFMT_SL1) ^ (L1 >> 32) ^ (L1 << 32) ^ b->u[0];
    lung->u[1] = (t1 << DSFMT_SL1) ^ (L0 >> 32) ^ (L0 << 32) ^ b->u[1];
    r->u[0] = (lung->u[0] >> DSFMT_SR) ^ (lung->u[0] & DSFMT_MSK1) ^ t0;
    r->u[1] = (lung->u[1] >> DSFMT_SR) ^ (lung->u[1] & DSFMT_MSK2) ^ t1;
}
#endif

} // extern "C"

} // namespace _dSFMT


/*!
 * A wrapper around dSFMT
 */
class dSFMT {
public:
    static constexpr bool debug = true;

    dSFMT(size_t seed) {
        _dSFMT::dsfmt_init_gen_rand(&dsfmt_, seed);
    }

    //! Generate `size` [0,1) doubles in `output`
    void generate_block(std::vector<double> &output, size_t size)
    {
        // Ensure minimum block size (normally 382)
        const size_t min_size = _dSFMT::dsfmt_get_min_array_size();
        if (size < min_size) {
            sLOG << "dSFMT: requested fewer than" << min_size
                 << "deviates, namely" << size;
            size = min_size;
        }
        // resize if the output vector is too small
        if (size > output.size()) {
            output.resize(size);
        }
        _dSFMT::dsfmt_fill_array_close_open(&dsfmt_, output.data(), size);
    }

    //! Generate `size` [0,1) doubles in `output`
    void generate_block(double* output, size_t size) {
        // Ensure minimum block size (normally 382)
        const size_t min_size = _dSFMT::dsfmt_get_min_array_size();
        if (size < min_size) {
            sLOG << "dSFMT: requested fewer than" << min_size
                 << "deviates, namely" << size;
            size = min_size;
        }

        _dSFMT::dsfmt_fill_array_close_open(&dsfmt_, output, size);
    }

private:
    _dSFMT::dsfmt_t dsfmt_;
};

} // namespace common
} // namespace thrill
