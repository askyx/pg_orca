//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTaskId.h
//
//	@doc:
//		Abstraction of task identification
//---------------------------------------------------------------------------

#ifndef GPOS_CTaskId_H
#define GPOS_CTaskId_H

#include "gpos/types.h"
#include "gpos/utils.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CTaskId
//
//	@doc:
//		Identification class; uses a serial number.
//
//---------------------------------------------------------------------------
class CTaskId {
 private:
  // task id
  uintptr_t m_task_id;

  // atomic counter
  static uintptr_t m_counter;

 public:
  // ctor
  CTaskId() : m_task_id(m_counter++) {}

  // simple comparison
  bool Equals(const CTaskId &tid) const { return m_task_id == tid.m_task_id; }

  // comparison operator
  inline bool operator==(const CTaskId &tid) const { return this->Equals(tid); }

  // comparison function; used in hashtables
  static bool Equals(const CTaskId &tid, const CTaskId &other) { return tid == other; }

  // primitive hash function
  static uint32_t HashValue(const CTaskId &tid) { return gpos::HashValue<uintptr_t>(&tid.m_task_id); }

  // invalid id
  static const CTaskId m_invalid_tid;

};  // class CTaskId

}  // namespace gpos

#endif  // !GPOS_CTaskId_H

// EOF
