//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLSpoolInfo.h
//
//	@doc:
//		Class for representing spooling info in shared scan nodes, and nodes that
//		allow sharing (currently Materialize and Sort).
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLSpoolInfo_H
#define GPDXL_CDXLSpoolInfo_H

#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl {
// fwd decl

enum Edxlspooltype { EdxlspoolNone, EdxlspoolMaterialize, EdxlspoolSort, EdxlspoolSentinel };

//---------------------------------------------------------------------------
//	@class:
//		CDXLSpoolInfo
//
//	@doc:
//		Class for representing spooling info in shared scan nodes, and nodes that
//		allow sharing (currently Materialize and Sort).
//
//---------------------------------------------------------------------------
class CDXLSpoolInfo {
 private:
  // id of the spooling operator
  uint32_t m_spool_id;

  // type of the underlying spool
  Edxlspooltype m_spool_type;

  // is the spool shared across multiple slices
  bool m_is_multi_slice_shared;

  // slice executing the underlying sort or materialize
  int32_t m_executor_slice_id;

  // spool type name
  const CWStringConst *GetSpoolTypeName() const;

 public:
  CDXLSpoolInfo(CDXLSpoolInfo &) = delete;

  // ctor/dtor
  CDXLSpoolInfo(uint32_t ulSpoolId, Edxlspooltype edxlspstype, bool fMultiSlice, int32_t iExecutorSlice);

  // accessors

  // spool id
  uint32_t GetSpoolId() const;

  // spool type (sort or materialize)
  Edxlspooltype GetSpoolType() const;

  // is spool shared across multiple slices
  bool IsMultiSlice() const;

  // id of slice executing the underlying operation
  int32_t GetExecutorSliceId() const;

  // serialize operator in DXL format
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLSpoolInfo_H

// EOF
