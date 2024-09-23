//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 VMware, Inc. or its affiliates.
//
//	@filename:
//		clibwrapper.cpp
//
//	@doc:
//		Wrapper for functions in C library
//
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"

#include <cxxabi.h>
#include <dlfcn.h>
#include <errno.h>
#include <fenv.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <wchar.h>

#include "gpos/assert.h"
#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/utils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		clib::USleep
//
//	@doc:
//		Sleep given number of microseconds
//
//---------------------------------------------------------------------------
void gpos::clib::USleep(uint32_t usecs) {
  GPOS_ASSERT(1000000 >= usecs);

  // ignore return value
  (void)usleep(usecs);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strcmp
//
//	@doc:
//		Compare two strings
//
//---------------------------------------------------------------------------
int32_t gpos::clib::Strcmp(const char *left, const char *right) {
  GPOS_ASSERT(nullptr != left);
  GPOS_ASSERT(nullptr != right);

  return strcmp(left, right);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strncmp
//
//	@doc:
//		Compare two strings up to a specified number of characters
//
//---------------------------------------------------------------------------
int32_t gpos::clib::Strncmp(const char *left, const char *right, size_t num_bytes) {
  GPOS_ASSERT(nullptr != left);
  GPOS_ASSERT(nullptr != right);

  return strncmp(left, right, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Memcmp
//
//	@doc:
//		Compare a specified number of bytes of two regions of memory
//---------------------------------------------------------------------------
int32_t gpos::clib::Memcmp(const void *left, const void *right, size_t num_bytes) {
  GPOS_ASSERT(nullptr != left);
  GPOS_ASSERT(nullptr != right);

  return memcmp(left, right, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Wcsncmp
//
//	@doc:
//		Compare two strings up to a specified number of wide characters
//
//---------------------------------------------------------------------------
int32_t gpos::clib::Wcsncmp(const wchar_t *left, const wchar_t *right, size_t num_bytes) {
  GPOS_ASSERT(nullptr != left);
  GPOS_ASSERT(nullptr != right);

  return wcsncmp(left, right, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::WcStrNCpy
//
//	@doc:
//		Copy two strings up to a specified number of wide characters
//
//---------------------------------------------------------------------------
wchar_t *gpos::clib::WcStrNCpy(wchar_t *dest, const wchar_t *src, size_t num_bytes) {
  GPOS_ASSERT(nullptr != dest);
  GPOS_ASSERT(nullptr != src && num_bytes > 0);

  // check for overlap
  GPOS_ASSERT(((src + num_bytes) <= dest) || ((dest + num_bytes) <= src));

  return wcsncpy(dest, src, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Memcpy
//
//	@doc:
//		Copy a specified number of bytes between two memory areas
//
//---------------------------------------------------------------------------
void *gpos::clib::Memcpy(void *dest, const void *src, size_t num_bytes) {
  GPOS_ASSERT(nullptr != dest);

  GPOS_ASSERT(nullptr != src && num_bytes > 0);

#ifdef GPOS_DEBUG
  const uint8_t *src_addr = static_cast<const uint8_t *>(src);
  const uint8_t *dest_addr = static_cast<const uint8_t *>(dest);
#endif  // GPOS_DEBUG

  // check for overlap
  GPOS_ASSERT(((src_addr + num_bytes) <= dest_addr) || ((dest_addr + num_bytes) <= src_addr));

  return memcpy(dest, src, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Wmemcpy
//
//	@doc:
//		Copy a specified number of wide characters
//
//---------------------------------------------------------------------------
wchar_t *gpos::clib::Wmemcpy(wchar_t *dest, const wchar_t *src, size_t num_bytes) {
  GPOS_ASSERT(nullptr != dest);
  GPOS_ASSERT(nullptr != src && num_bytes > 0);

#ifdef GPOS_DEBUG
  const wchar_t *src_addr = static_cast<const wchar_t *>(src);
  const wchar_t *dest_addr = static_cast<wchar_t *>(dest);
#endif

  // check for overlap
  GPOS_ASSERT(((src_addr + num_bytes) <= dest_addr) || ((dest_addr + num_bytes) <= src_addr));

  return wmemcpy(dest, src, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strncpy
//
//	@doc:
//		Copy a specified number of characters
//
//---------------------------------------------------------------------------
char *gpos::clib::Strncpy(char *dest, const char *src, size_t num_bytes) {
  GPOS_ASSERT(nullptr != dest);
  GPOS_ASSERT(nullptr != src && num_bytes > 0);
  GPOS_ASSERT(((src + num_bytes) <= dest) || ((dest + num_bytes) <= src));

  return strncpy(dest, src, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strchr
//
//	@doc:
//		Find the first occurrence of the character c (converted to a char) in
//		the null-terminated string beginning at src. Returns a pointer to the
//		located character, or a null pointer if no match was found
//
//---------------------------------------------------------------------------
char *gpos::clib::Strchr(const char *src, int32_t c) {
  GPOS_ASSERT(nullptr != src);

  return (char *)strchr(src, c);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Memset
//
//	@doc:
//		Set the bytes of a given memory block to a specific value
//
//---------------------------------------------------------------------------
void *gpos::clib::Memset(void *dest, int32_t value, size_t num_bytes) {
  GPOS_ASSERT(nullptr != dest);
  GPOS_ASSERT_IFF(0 <= value, 255 >= value);

  return memset(dest, value, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Qsort
//
//	@doc:
//		Sort a specified number of elements
//
//---------------------------------------------------------------------------
void gpos::clib::Qsort(void *dest, size_t num_bytes, size_t size, Comparator comparator) {
  GPOS_ASSERT(nullptr != dest);

  qsort(dest, num_bytes, size, comparator);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Getopt
//
//	@doc:
//		Parse the command-line arguments
//
//---------------------------------------------------------------------------
int32_t gpos::clib::Getopt(int32_t argc, char *const argv[], const char *opt_string) {
  return getopt(argc, argv, opt_string);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strtol
//
//	@doc:
//		Convert string to long integer
//
//---------------------------------------------------------------------------
int64_t gpos::clib::Strtol(const char *val, char **end, uint32_t base) {
  GPOS_ASSERT(nullptr != val);
  GPOS_ASSERT(0 == base || 2 == base || 10 == base || 16 == base);

  return strtol(val, end, base);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strtoll
//
//	@doc:
//		Convert string to long long integer
//
//---------------------------------------------------------------------------
int64_t gpos::clib::Strtoll(const char *val, char **end, uint32_t base) {
  GPOS_ASSERT(nullptr != val);
  GPOS_ASSERT(0 == base || 2 == base || 10 == base || 16 == base);

  return strtoll(val, end, base);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Rand
//
//	@doc:
//		Return a pseudo-random integer between 0 and RAND_MAX
//
//---------------------------------------------------------------------------
uint32_t gpos::clib::Rand(uint32_t *seed) {
  GPOS_ASSERT(nullptr != seed);

  int32_t res = rand_r(seed);

  GPOS_ASSERT(res >= 0 && res <= RAND_MAX);

  return static_cast<uint32_t>(res);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Vswprintf
//
//	@doc:
//		Format wide character output conversion
//
//---------------------------------------------------------------------------
int32_t gpos::clib::Vswprintf(wchar_t *wcstr, size_t max_len, const wchar_t *format, VA_LIST vaArgs) {
  GPOS_ASSERT(nullptr != wcstr);
  GPOS_ASSERT(nullptr != format);

  int32_t res = vswprintf(wcstr, max_len, format, vaArgs);
  if (-1 == res && EILSEQ == errno) {
    // Invalid multibyte character encountered. This can happen if the byte sequence does not
    // match with the server encoding.
    //
    // Rather than fail/fall-back here, ORCA uses a generic "UNKNOWN"
    // string. During DXL to PlStmt translation this will be translated
    // back using the original query tree (see TranslateDXLProjList)
    res = swprintf(wcstr, max_len, format, "UNKNOWN");
  }

  return res;
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Vsnprintf
//
//	@doc:
//		Format string
//
//---------------------------------------------------------------------------
int32_t gpos::clib::Vsnprintf(char *src, size_t size, const char *format, VA_LIST vaArgs) {
  GPOS_ASSERT(nullptr != src);
  GPOS_ASSERT(nullptr != format);

  return vsnprintf(src, size, format, vaArgs);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strerror_r
//
//	@doc:
//		Return string describing error number
//
//---------------------------------------------------------------------------
void gpos::clib::Strerror_r(int32_t errnum, char *buf, size_t buf_len) {
  GPOS_ASSERT(nullptr != buf);

#ifdef _GNU_SOURCE
  // GNU-specific strerror_r() returns char*.
  char *error_str = strerror_r(errnum, buf, buf_len);
  GPOS_ASSERT(nullptr != error_str);

  // GNU strerror_r() may return a pointer to a static error string.
  // Copy it into 'buf' if that is the case.
  if (error_str != buf) {
    strncpy(buf, error_str, buf_len);
    // Ensure null-terminated.
    buf[buf_len - 1] = '\0';
  }
#else  // !_GNU_SOURCE
       // POSIX.1-2001 standard strerror_r() returns int.
  int32_t str_err_code GPOS_ASSERTS_ONLY = strerror_r(errnum, buf, buf_len);
  GPOS_ASSERT(0 == str_err_code);

#endif
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Wcslen
//
//	@doc:
//		Calculate the length of a wide-character string
//
//---------------------------------------------------------------------------
uint32_t gpos::clib::Wcslen(const wchar_t *dest) {
  GPOS_ASSERT(nullptr != dest);

  return (uint32_t)wcslen(dest);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Localtime_r
//
//	@doc:
//		Convert the calendar time time to broken-time representation;
//		Expressed relative to the user's specified time zone
//
//---------------------------------------------------------------------------
struct tm *gpos::clib::Localtime_r(const TIME_T *time, TIME *result) {
  GPOS_ASSERT(nullptr != time);

  localtime_r(time, result);

  GPOS_ASSERT(nullptr != result);

  return result;
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Malloc
//
//	@doc:
//		Allocate dynamic memory
//
//---------------------------------------------------------------------------
void *gpos::clib::Malloc(size_t size) {
  return malloc(size);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Free
//
//	@doc:
//		Free dynamic memory
//
//---------------------------------------------------------------------------
void gpos::clib::Free(void *src) {
  free(src);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strlen
//
//	@doc:
//		Calculate the length of a string
//
//---------------------------------------------------------------------------
uint32_t gpos::clib::Strlen(const char *buf) {
  GPOS_ASSERT(nullptr != buf);

  return (uint32_t)strlen(buf);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Wctomb
//
//	@doc:
//		Convert a wide character to a multibyte sequence
//
//---------------------------------------------------------------------------
int32_t gpos::clib::Wctomb(char *dest, wchar_t src) {
  return wctomb(dest, src);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Mbstowcs
//
//	@doc:
//		Convert a multibyte sequence to wide character array
//
//---------------------------------------------------------------------------
uint32_t gpos::clib::Mbstowcs(wchar_t *dest, const char *src, size_t len) {
  GPOS_ASSERT(nullptr != dest);
  GPOS_ASSERT(nullptr != src);

  return (uint32_t)mbstowcs(dest, src, len);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Wcstombs
//
//	@doc:
//		Convert a wide-character string to a multi-byte string
//
//---------------------------------------------------------------------------
int64_t gpos::clib::Wcstombs(char *dest, wchar_t *src, uintptr_t dest_size) {
  return wcstombs(dest, src, dest_size);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strtod
//
//	@doc:
//		Convert string to double;
//		if conversion fails, return 0.0
//
//---------------------------------------------------------------------------
double gpos::clib::Strtod(const char *str) {
  return strtod(str, nullptr);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Demangle
//
//	@doc:
//		Return a pointer to the start of the NULL-terminated
//		symbol or NULL if demangling fails
//
//---------------------------------------------------------------------------
char *gpos::clib::Demangle(const char *symbol, char *buf, size_t *len, int32_t *status) {
  GPOS_ASSERT(nullptr != symbol);

  char *res = abi::__cxa_demangle(symbol, buf, len, status);

  GPOS_ASSERT(-3 != *status && "One of the arguments is invalid.");

  return res;
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Dladdr
//
//	@doc:
//		Resolve symbol information from its address
//
//---------------------------------------------------------------------------
void gpos::clib::Dladdr(void *addr, DL_INFO *info) {
  int32_t res GPOS_ASSERTS_ONLY = dladdr(addr, info);

  GPOS_ASSERT(0 != res);
}

// EOF
