//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEConsumer.h
//
//	@doc:
//		Class for representing DXL logical CTE Consumer operators
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEConsumer_H
#define GPDXL_CDXLLogicalCTEConsumer_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl {
//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalCTEConsumer
//
//	@doc:
//		Class for representing DXL logical CTE Consumers
//
//---------------------------------------------------------------------------
class CDXLLogicalCTEConsumer : public CDXLLogical {
 private:
  // cte id
  uint32_t m_id;

  // output column ids
  ULongPtrArray *m_output_colids_array;

 public:
  CDXLLogicalCTEConsumer(CDXLLogicalCTEConsumer &) = delete;

  // ctor
  CDXLLogicalCTEConsumer(CMemoryPool *mp, uint32_t id, ULongPtrArray *output_colids_array);

  // dtor
  ~CDXLLogicalCTEConsumer() override;

  // operator type
  Edxlopid GetDXLOperator() const override;

  // operator name
  const CWStringConst *GetOpNameStr() const override;

  // cte identifier
  uint32_t Id() const { return m_id; }

  ULongPtrArray *GetOutputColIdsArray() const { return m_output_colids_array; }

  // serialize operator in DXL format

  // check if given column is defined by operator
  bool IsColDefined(uint32_t colid) const override;

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *, bool validate_children) const override;
#endif  // GPOS_DEBUG

  // conversion function
  static CDXLLogicalCTEConsumer *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopLogicalCTEConsumer == dxl_op->GetDXLOperator());
    return dynamic_cast<CDXLLogicalCTEConsumer *>(dxl_op);
  }
};
}  // namespace gpdxl
#endif  // !GPDXL_CDXLLogicalCTEConsumer_H

// EOF
