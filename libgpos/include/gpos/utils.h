//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		utils.h
//
//	@doc:
//		Various utilities which are not necessarily gpos specific
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_utils_H
#define GPOS_utils_H

#include "gpos/error/CException.h"
#include "gpos/io/COstreamBasic.h"
#include "gpos/types.h"

#define ALIGNED_16(x) (((uintptr_t)x >> 1) << 1 == (uintptr_t)x)  // checks 16-bit alignment
#define ALIGNED_32(x) (((uintptr_t)x >> 2) << 2 == (uintptr_t)x)  // checks 32-bit alignment
#define ALIGNED_64(x) (((uintptr_t)x >> 3) << 3 == (uintptr_t)x)  // checks 64-bit alignment

#define MAX_ALIGNED(x) ALIGNED_64(x)

#define ALIGN_STORAGE __attribute__((aligned(8)))

#define GPOS_GET_FRAME_POINTER(x) ((x) = (uintptr_t)__builtin_frame_address(0))

#define GPOS_MSEC_IN_SEC ((uint64_t)1000)
#define GPOS_USEC_IN_MSEC ((uint64_t)1000)
#define GPOS_USEC_IN_SEC (((uint64_t)1000) * 1000)
#define GPOS_NSEC_IN_SEC (((uint64_t)1000) * 1000 * 1000)

namespace gpos {
// print wide-character string to stdout
void Print(wchar_t *wsz);

// generic memory dumper routine
IOstream &HexDump(IOstream &os, const void *pv, uint64_t size);

// generic hash function for byte strings
uint32_t HashByteArray(const uint8_t *, const uint32_t);

// generic hash function; by address
template <class T>
inline uint32_t HashValue(const T *pt) {
  return HashByteArray((uint8_t *)pt, GPOS_SIZEOF(T));
}

// generic hash function for pointer types -- use e.g. when address is ID of object
template <class T>
inline uint32_t HashPtr(const T *pt) {
  return HashByteArray((uint8_t *)&pt, GPOS_SIZEOF(void *));
}

// equality function on pointers
template <class T>
inline bool EqualPtr(const T *pt1, const T *pt2) {
  return pt1 == pt2;
}

// hash function for uintptr_t
inline uint32_t HashULongPtr(const uintptr_t &key) {
  return (uint32_t)key;
}

// combine uint32_t hashes
uint32_t CombineHashes(uint32_t, uint32_t);

// equality function, which uses the equality operator of the arguments type
template <class T>
inline bool Equals(const T *pt1, const T *pt2) {
  return *pt1 == *pt2;
}

// equality function for uintptr_t
inline bool EqualULongPtr(const uintptr_t &key_left, const uintptr_t &key_right) {
  return key_left == key_right;
}

// yield and sleep (time in muSec)
// note that in some platforms the minimum sleep interval is 1ms
void USleep(uint32_t);

// add two unsigned long long values, throw an exception if overflow occurs
uint64_t Add(uint64_t first, uint64_t second);

// multiply two unsigned long long values, throw an exception if overflow occurs
uint64_t Multiply(uint64_t first, uint64_t second);

// extern definitions for standard streams; to be used during
// startup/shutdown when outside of task framework
extern COstreamBasic oswcerr;
extern COstreamBasic oswcout;

}  // namespace gpos

#endif  // !GPOS_utils_H

// EOF
