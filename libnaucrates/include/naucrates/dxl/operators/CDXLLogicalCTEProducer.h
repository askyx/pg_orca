//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEProducer.h
//
//	@doc:
//		Class for representing DXL logical CTE producer operators
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEProducer_H
#define GPDXL_CDXLLogicalCTEProducer_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl {
//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalCTEProducer
//
//	@doc:
//		Class for representing DXL logical CTE producers
//
//---------------------------------------------------------------------------
class CDXLLogicalCTEProducer : public CDXLLogical {
 private:
  // cte id
  uint32_t m_id;

  // output column ids
  ULongPtrArray *m_output_colids_array;

 public:
  CDXLLogicalCTEProducer(CDXLLogicalCTEProducer &) = delete;

  // ctor
  CDXLLogicalCTEProducer(CMemoryPool *mp, uint32_t id, ULongPtrArray *output_colids_array);

  // dtor
  ~CDXLLogicalCTEProducer() override;

  // operator type
  Edxlopid GetDXLOperator() const override;

  // operator name
  const CWStringConst *GetOpNameStr() const override;

  // cte identifier
  uint32_t Id() const { return m_id; }

  ULongPtrArray *GetOutputColIdsArray() const { return m_output_colids_array; }

  // serialize operator in DXL format

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *, bool validate_children) const override;
#endif  // GPOS_DEBUG

  // conversion function
  static CDXLLogicalCTEProducer *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopLogicalCTEProducer == dxl_op->GetDXLOperator());
    return dynamic_cast<CDXLLogicalCTEProducer *>(dxl_op);
  }
};
}  // namespace gpdxl
#endif  // !GPDXL_CDXLLogicalCTEProducer_H

// EOF
