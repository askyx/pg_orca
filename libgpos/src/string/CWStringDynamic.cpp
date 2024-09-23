//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWStringDynamic.cpp
//
//	@doc:
//		Implementation of the wide character string class
//		with dynamic buffer allocation.
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "gpos/common/CAutoRg.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/string/CStringStatic.h"
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::CWStringDynamic
//
//	@doc:
//		Constructs an empty string
//
//---------------------------------------------------------------------------
CWStringDynamic::CWStringDynamic(CMemoryPool *mp)
    : CWString(0  // length
               ),
      m_mp(mp),
      m_capacity(0) {
  Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::CWStringDynamic
//
//	@doc:
//		Constructs a new string and initializes it with the given buffer
//
//---------------------------------------------------------------------------
CWStringDynamic::CWStringDynamic(CMemoryPool *mp, const wchar_t *w_str_buffer)
    : CWString(GPOS_WSZ_LENGTH(w_str_buffer)), m_mp(mp), m_capacity(0) {
  GPOS_ASSERT(nullptr != w_str_buffer);

  Reset();
  AppendBuffer(w_str_buffer);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::~CWStringDynamic
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CWStringDynamic::~CWStringDynamic() {
  Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CWString::Reset
//
//	@doc:
//		Resets string
//
//---------------------------------------------------------------------------
void CWStringDynamic::Reset() {
  if (nullptr != m_w_str_buffer && &m_empty_wcstr != m_w_str_buffer) {
    GPOS_DELETE_ARRAY(m_w_str_buffer);
  }

  m_w_str_buffer = const_cast<wchar_t *>(&m_empty_wcstr);
  m_length = 0;
  m_capacity = 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendBuffer
//
//	@doc:
//		Appends the contents of a buffer to the current string
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendBuffer(const wchar_t *w_str) {
  GPOS_ASSERT(nullptr != w_str);
  uint32_t length = GPOS_WSZ_LENGTH(w_str);
  if (0 == length) {
    return;
  }

  // expand buffer if needed
  uint32_t new_length = m_length + length;
  if (new_length + 1 > m_capacity) {
    IncreaseCapacity(new_length);
  }

  clib::WcStrNCpy(m_w_str_buffer + m_length, w_str, length + 1);
  m_length = new_length;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendWideCharArray
//
//	@doc:
//		Appends a a null terminated wide character array
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendWideCharArray(const wchar_t *w_str) {
  AppendBuffer(w_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendCharArray
//
//	@doc:
//		Appends a a null terminated character array
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendCharArray(const char *sz) {
  GPOS_ASSERT(nullptr != sz);

  // expand buffer if needed
  const uint32_t length = GPOS_SZ_LENGTH(sz);
  uint32_t new_length = m_length + length;
  if (new_length + 1 > m_capacity) {
    IncreaseCapacity(new_length);
  }
  wchar_t *w_str_buffer = GPOS_NEW_ARRAY(m_mp, wchar_t, length + 1);

  // convert input string to wide character buffer
  uint32_t wide_length GPOS_ASSERTS_ONLY = clib::Mbstowcs(w_str_buffer, sz, length);
  GPOS_ASSERT(wide_length == length);

  // append input string to current end of buffer
  (void)clib::Wmemcpy(m_w_str_buffer + m_length, w_str_buffer, length + 1);
  GPOS_DELETE_ARRAY(w_str_buffer);

  m_w_str_buffer[new_length] = WCHAR_EOS;
  m_length = new_length;

  GPOS_ASSERT(IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendFormat
//
//	@doc:
//		Appends a formatted string
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendFormat(const wchar_t *format, ...) {
  GPOS_ASSERT(nullptr != format);
  using clib::Vswprintf;

  VA_LIST va_args;

  // determine length of format string after expansion
  int32_t res = -1;

  // attempt to fit the formatted string in a static array
  wchar_t w_str_buf_static[GPOS_WSTR_DYNAMIC_STATIC_BUFFER];

  // get arguments
  VA_START(va_args, format);

  // try expanding the formatted string in the buffer
  res = Vswprintf(w_str_buf_static, GPOS_ARRAY_SIZE(w_str_buf_static), format, va_args);

  // reset arguments
  VA_END(va_args);
  GPOS_ASSERT(-1 <= res);

  // estimated number of characters in expanded format string
  uint32_t size = std::max(GPOS_WSZ_LENGTH(format), GPOS_ARRAY_SIZE(w_str_buf_static));

  // if the static buffer is too small, find the formatted string
  // length by trying to store it in a buffer of increasing size
  while (-1 == res) {
    // try with a bigger buffer this time
    size *= 2;
    CAutoRg<wchar_t> a_w_str_buff;
    a_w_str_buff = GPOS_NEW_ARRAY(m_mp, wchar_t, size + 1);

    // get arguments
    VA_START(va_args, format);

    // try expanding the formatted string in the buffer
    res = Vswprintf(a_w_str_buff.Rgt(), size, format, va_args);

    // reset arguments
    VA_END(va_args);

    GPOS_ASSERT(-1 <= res);
  }
  // verify required buffer was not bigger than allowed
  GPOS_ASSERT(res >= 0);

  // expand buffer if needed
  uint32_t new_length = m_length + uint32_t(res);
  if (new_length + 1 > m_capacity) {
    IncreaseCapacity(new_length);
  }

  // get arguments
  VA_START(va_args, format);

  // print va_args to string
  Vswprintf(m_w_str_buffer + m_length, res + 1, format, va_args);

  // reset arguments
  VA_END(va_args);

  m_length = new_length;
  GPOS_ASSERT(IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendEscape
//
//	@doc:
//		Appends a string and replaces character with string
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendEscape(const CWStringBase *str, wchar_t wc, const wchar_t *w_str_replace) {
  GPOS_ASSERT(nullptr != str);

  if (str->IsEmpty()) {
    return;
  }

  // count how many times the character to be escaped appears in the string
  uint32_t occurrences = str->CountOccurrencesOf(wc);
  if (0 == occurrences) {
    Append(str);
    return;
  }

  uint32_t length = str->Length();
  const wchar_t *w_str = str->GetBuffer();

  uint32_t length_replace = GPOS_WSZ_LENGTH(w_str_replace);
  uint32_t new_length = m_length + length + (length_replace - 1) * occurrences;
  if (new_length + 1 > m_capacity) {
    IncreaseCapacity(new_length);
  }

  // append new contents while replacing character with escaping string
  for (uint32_t i = 0, j = m_length; i < length; i++) {
    if (wc == w_str[i] && !str->HasEscapedCharAt(i)) {
      clib::WcStrNCpy(m_w_str_buffer + j, w_str_replace, length_replace);
      j += length_replace;
    } else {
      m_w_str_buffer[j++] = w_str[i];
    }
  }

  // terminate string
  m_w_str_buffer[new_length] = WCHAR_EOS;
  m_length = new_length;

  GPOS_ASSERT(IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::IncreaseCapacity
//
//	@doc:
//		Increase string capacity
//
//---------------------------------------------------------------------------
void CWStringDynamic::IncreaseCapacity(uint32_t requested) {
  GPOS_ASSERT(requested + 1 > m_capacity);

  uint32_t capacity = Capacity(requested + 1);
  GPOS_ASSERT(capacity > requested + 1);
  GPOS_ASSERT(capacity >= (m_capacity << 1));

  CAutoRg<wchar_t> a_w_str_new_buff;
  a_w_str_new_buff = GPOS_NEW_ARRAY(m_mp, wchar_t, capacity);
  if (0 < m_length) {
    // current string is not empty: copy it to the resulting string
    a_w_str_new_buff = clib::WcStrNCpy(a_w_str_new_buff.Rgt(), m_w_str_buffer, m_length);
  }

  // release old buffer
  if (m_w_str_buffer != &m_empty_wcstr) {
    GPOS_DELETE_ARRAY(m_w_str_buffer);
  }
  m_w_str_buffer = a_w_str_new_buff.RgtReset();
  m_capacity = capacity;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::Capacity
//
//	@doc:
//		Find capacity that fits requested string size
//
//---------------------------------------------------------------------------
uint32_t CWStringDynamic::Capacity(uint32_t requested) {
  uint32_t capacity = GPOS_WSTR_DYNAMIC_CAPACITY_INIT;
  while (capacity <= requested + 1) {
    capacity = capacity << 1;
  }

  return capacity;
}

// EOF
