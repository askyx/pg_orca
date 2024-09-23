//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010, Greenplum, Inc.
//
//	@filename:
//		CMessageTable.h
//
//	@doc:
//		Error message table;
//---------------------------------------------------------------------------
#ifndef GPOS_CMessageTable_H
#define GPOS_CMessageTable_H

#include "gpos/error/CMessage.h"

#define GPOS_MSGTAB_SIZE 4096

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CMessageTable
//
//	@doc:
//		Maintains error messages for a given locale
//
//---------------------------------------------------------------------------
class CMessageTable {
  // short hand for message tables
  using MessageTable = CSyncHashtable<CMessage, CException>;

  // short hand for message table accessor
  using MTAccessor = CSyncHashtableAccessByKey<CMessage, CException>;

  // message hashtable
  MessageTable m_hash_table;

 public:
  CMessageTable(const CMessageTable &) = delete;

  // ctor
  CMessageTable(CMemoryPool *mp, uint32_t size, ELocale locale);

  // lookup message by error/local
  CMessage *LookupMessage(CException exc);

  // insert message
  void AddMessage(CMessage *msg);

  // simple comparison
  bool operator==(const CMessageTable &mt) const { return m_locale == mt.m_locale; }

  // equality function -- needed for hashtable
  static bool Equals(const ELocale &locale, const ELocale &other_locale) { return locale == other_locale; }

  // basic hash function
  static uint32_t HashValue(const ELocale &locale) { return (uint32_t)locale; }

  // link object
  SLink m_link;

  // locale
  ELocale m_locale;

  // invalid locale
  static const ELocale m_invalid_locale;

};  // class CMessageTable
}  // namespace gpos

#endif  // !GPOS_CMessageTable_H

// EOF
