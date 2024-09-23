//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CMDIndexInfo.cpp
//
//	@doc:
//		Implementation of the class for representing indexinfo
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIndexInfo.h"

using namespace gpdxl;
using namespace gpmd;

// ctor
CMDIndexInfo::CMDIndexInfo(IMDId *mdid, bool is_partial) : m_mdid(mdid), m_is_partial(is_partial) {
  GPOS_ASSERT(mdid->IsValid());
}

// dtor
CMDIndexInfo::~CMDIndexInfo() {
  m_mdid->Release();
}

// returns the metadata id of this index
IMDId *CMDIndexInfo::MDId() const {
  return m_mdid;
}

// is the index partial
bool CMDIndexInfo::IsPartial() const {
  return m_is_partial;
}

#ifdef GPOS_DEBUG
// prints a indexinfo to the provided output
void CMDIndexInfo::DebugPrint(IOstream &os) const {
  os << "Index id: ";
  MDId()->OsPrint(os);
  os << std::endl;
  os << "Is partial index: " << m_is_partial << std::endl;
}

#endif  // GPOS_DEBUG
