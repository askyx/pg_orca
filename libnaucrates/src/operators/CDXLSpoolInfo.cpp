//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLSpoolInfo.cpp
//
//	@doc:
//		Implementation of DXL sorting columns for sort and motion operator nodes
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLSpoolInfo.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::CDXLSpoolInfo
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLSpoolInfo::CDXLSpoolInfo(uint32_t ulSpoolId, Edxlspooltype edxlspstype, bool fMultiSlice, int32_t iExecutorSlice)
    : m_spool_id(ulSpoolId),
      m_spool_type(edxlspstype),
      m_is_multi_slice_shared(fMultiSlice),
      m_executor_slice_id(iExecutorSlice) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::GetSpoolId
//
//	@doc:
//		Spool id
//
//---------------------------------------------------------------------------
uint32_t CDXLSpoolInfo::GetSpoolId() const {
  return m_spool_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::GetSpoolType
//
//	@doc:
//		Type of the underlying operator (Materialize or Sort)
//
//---------------------------------------------------------------------------
Edxlspooltype CDXLSpoolInfo::GetSpoolType() const {
  return m_spool_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::IsMultiSlice
//
//	@doc:
//		Is the spool used across slices
//
//---------------------------------------------------------------------------
bool CDXLSpoolInfo::IsMultiSlice() const {
  return m_is_multi_slice_shared;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::GetExecutorSliceId
//
//	@doc:
//		Id of slice executing the underlying operation
//
//---------------------------------------------------------------------------
int32_t CDXLSpoolInfo::GetExecutorSliceId() const {
  return m_executor_slice_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::GetSpoolTypeName
//
//	@doc:
//		Id of slice executing the underlying operation
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLSpoolInfo::GetSpoolTypeName() const {
  GPOS_ASSERT(EdxlspoolMaterialize == m_spool_type || EdxlspoolSort == m_spool_type);

  switch (m_spool_type) {
    case EdxlspoolMaterialize:
      return CDXLTokens::GetDXLTokenStr(EdxltokenSpoolMaterialize);
    case EdxlspoolSort:
      return CDXLTokens::GetDXLTokenStr(EdxltokenSpoolSort);
    default:
      return CDXLTokens::GetDXLTokenStr(EdxltokenUnknown);
  }
}
