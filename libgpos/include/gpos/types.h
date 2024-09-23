//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		types.h
//
//	@doc:
//		Type definitions for gpos to avoid using native types directly;
//
//		TODO: 03/15/2008; the seletion of basic types which then
//		get mapped to internal types should be done by autoconf or other
//		external tools; for the time being these are hard-coded; cpl asserts
//		are used to make sure things are failing if compiled with a different
//		compiler.
//---------------------------------------------------------------------------
#ifndef GPOS_types_H
#define GPOS_types_H

#include <iostream>
#include <limits.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "gpos/assert.h"

#define GPOS_SIZEOF(x) ((uint32_t)sizeof(x))
#define GPOS_ARRAY_SIZE(x) (GPOS_SIZEOF(x) / GPOS_SIZEOF(x[0]))
#define GPOS_OFFSET(T, M) ((uint32_t)(size_t) & (((T *)0x1)->M) - 1)

/* wide character string literate */
#define GPOS_WSZ_LIT(x) L##x

namespace gpos {

// variadic parameter list type
using VA_LIST = va_list;

// enum for results on OS level (instead of using a global error variable)
enum GPOS_RESULT {
  GPOS_OK = 0,

  GPOS_FAILED = 1,
  GPOS_OOM = 2,
  GPOS_NOT_FOUND = 3,
  GPOS_TIMEOUT = 4
};

// enum for locale encoding
enum ELocale {
  ELocInvalid,  // invalid key for hashtable iteration
  ElocEnUS_Utf8,
  ElocGeDE_Utf8,

  ElocSentinel
};
}  // namespace gpos

#endif  // !GPOS_types_H

// EOF
