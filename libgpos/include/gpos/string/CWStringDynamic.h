//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CWStringDynamic.h
//
//	@doc:
//		Wide character string class with dynamic buffer allocation.
//---------------------------------------------------------------------------
#ifndef GPOS_CWStringDynamic_H
#define GPOS_CWStringDynamic_H

#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/string/CWString.h"

#define GPOS_WSTR_DYNAMIC_CAPACITY_INIT (1 << 7)
#define GPOS_WSTR_DYNAMIC_STATIC_BUFFER (1 << 10)

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CWStringDynamic
//
//	@doc:
//		Implementation of the string interface with dynamic buffer allocation.
//		This CWStringDynamic class dynamically allocates memory when constructing a new
//		string, or when modifying a string. The memory is released at destruction time
//		or when the string is reset.
//
//---------------------------------------------------------------------------
class CWStringDynamic : public CWString {
 private:
  // string memory pool used for allocating new memory for the string
  CMemoryPool *m_mp;

  // string capacity
  uint32_t m_capacity;

  // increase string capacity
  void IncreaseCapacity(uint32_t requested);

  // find capacity that fits requested string size
  static uint32_t Capacity(uint32_t requested);

 protected:
  // appends the contents of a buffer to the current string
  void AppendBuffer(const wchar_t *w_str_buffer) override;

 public:
  CWStringDynamic(const CWStringDynamic &) = delete;

  // ctor
  CWStringDynamic(CMemoryPool *mp);

  // ctor - copies passed string
  CWStringDynamic(CMemoryPool *mp, const wchar_t *w_str_buffer);

  // appends a string and replaces character with string
  void AppendEscape(const CWStringBase *str, wchar_t wc, const wchar_t *w_str_replace) override;

  // appends a formatted string
  void AppendFormat(const wchar_t *format, ...) override;

  // appends a null terminated character array
  void AppendCharArray(const char *sz) override;

  // appends a null terminated wide character array
  void AppendWideCharArray(const wchar_t *w_str) override;

  // dtor
  ~CWStringDynamic() override;

  // resets string
  void Reset() override;
};
}  // namespace gpos

#endif  // !GPOS_CWStringDynamic_H

// EOF
