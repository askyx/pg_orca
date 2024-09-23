//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 VMware, Inc. or its affiliates.
//
//	@filename:
//	       	clibwrapper.h
//
//	@doc:
//	       	Wrapper for functions in C library
//
//---------------------------------------------------------------------------

#ifndef GPOS_clibwrapper_H
#define GPOS_clibwrapper_H

#define VA_START(vaList, last) va_start(vaList, last);
#define VA_END(vaList) va_end(vaList)
#define VA_ARG(vaList, type) va_arg(vaList, type)

#include <unistd.h>

#include "gpos/attributes.h"
#include "gpos/common/clibtypes.h"
#include "gpos/types.h"

namespace gpos {
namespace clib {
using Comparator = int32_t (*)(const void *, const void *);

// compare a specified number of bytes of two regions of memory
int32_t Memcmp(const void *left, const void *right, size_t num_bytes);

// sleep given number of microseconds
void USleep(uint32_t usecs);

// compare two strings
int32_t Strcmp(const char *left, const char *right);

// compare two strings up to a specified number of characters
int32_t Strncmp(const char *left, const char *right, size_t num_bytes);

// compare two strings up to a specified number of wide characters
int32_t Wcsncmp(const wchar_t *left, const wchar_t *right, size_t num_bytes);

// copy two strings up to a specified number of wide characters
wchar_t *WcStrNCpy(wchar_t *dest, const wchar_t *src, size_t num_bytes);

// copy a specified number of bytes between two memory areas
void *Memcpy(void *dest, const void *src, size_t num_bytes);

// copy a specified number of wide characters
wchar_t *Wmemcpy(wchar_t *dest, const wchar_t *src, size_t num_bytes);

// copy a specified number of characters
char *Strncpy(char *dest, const char *src, size_t num_bytes);

// find the first occurrence of the character c in src
char *Strchr(const char *src, int32_t c);

// set a specified number of bytes to a specified m_bytearray_value
void *Memset(void *dest, int32_t value, size_t num_bytes);

// calculate the length of a wide-character string
uint32_t Wcslen(const wchar_t *dest);

// calculate the length of a string
uint32_t Strlen(const char *buf);

// sort a specified number of elements
void Qsort(void *dest, size_t num_bytes, size_t size, Comparator fnComparator);

// parse command-line options
int32_t Getopt(int32_t argc, char *const argv[], const char *opt_string);

// convert string to long integer
int64_t Strtol(const char *val, char **end, uint32_t base);

// convert string to long long integer
int64_t Strtoll(const char *val, char **end, uint32_t base);

// convert string to double
double Strtod(const char *str);

// return a pseudo-random integer between 0 and RAND_MAX
uint32_t Rand(uint32_t *seed);

// format wide character output conversion
int32_t Vswprintf(wchar_t *wcstr, size_t max_len, const wchar_t *format, VA_LIST vaArgs);

// format string
int32_t Vsnprintf(char *src, size_t size, const char *format, VA_LIST vaArgs) GPOS_ATTRIBUTE_PRINTF(3, 0);

// return string describing error number
void Strerror_r(int32_t errnum, char *buf, size_t buf_len);

// convert the calendar time time to broken-time representation
TIME *Localtime_r(const TIME_T *time, TIME *result);

// allocate dynamic memory
void *Malloc(size_t size);

// free dynamic memory
void Free(void *src);

// convert a wide character to a multibyte sequence
int32_t Wctomb(char *dest, wchar_t src);

// convert a wide-character string to a multi-byte string
int64_t Wcstombs(char *dest, wchar_t *src, uintptr_t dest_size);

// convert a multibyte sequence to wide character array
uint32_t Mbstowcs(wchar_t *dest, const char *src, size_t len);

// return a pointer to the start of the NULL-terminated symbol
char *Demangle(const char *symbol, char *buf, size_t *len, int32_t *status);

// resolve symbol information from its address
void Dladdr(void *addr, DL_INFO *info);

}  // namespace clib
}  // namespace gpos

#endif  // !GPOS_clibwrapper_H

// EOF
