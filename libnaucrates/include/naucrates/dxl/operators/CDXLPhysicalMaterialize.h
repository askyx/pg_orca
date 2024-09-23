//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalMaterialize.h
//
//	@doc:
//		Class for representing DXL physical materialize operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalMaterialize_H
#define GPDXL_CDXLPhysicalMaterialize_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLSpoolInfo.h"

namespace gpdxl {
// indices of materialize elements in the children array
enum Edxlmaterialize { EdxlmatIndexProjList = 0, EdxlmatIndexFilter, EdxlmatIndexChild, EdxlmatIndexSentinel };

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalMaterialize
//
//	@doc:
//		Class for representing DXL materialize operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalMaterialize : public CDXLPhysical {
 private:
  // eager materialization
  bool m_is_eager;

  // spool info
  // id of the spooling operator
  uint32_t m_spooling_op_id;

  // type of the underlying spool
  Edxlspooltype m_spool_type;

  // slice executing the underlying sort or materialize
  int32_t m_executor_slice;

  // number of consumers in case the materialize is a spooling operator
  uint32_t m_num_consumer_slices;

 public:
  CDXLPhysicalMaterialize(CDXLPhysicalMaterialize &) = delete;

  // ctor/dtor
  CDXLPhysicalMaterialize(CMemoryPool *mp, bool is_eager);

  CDXLPhysicalMaterialize(CMemoryPool *mp, bool is_eager, uint32_t spooling_op_id, int32_t executor_slice,
                          uint32_t num_consumer_slices);

  // accessors
  Edxlopid GetDXLOperator() const override;
  const CWStringConst *GetOpNameStr() const override;
  uint32_t GetSpoolingOpId() const;
  int32_t GetExecutorSlice() const;
  uint32_t GetNumConsumerSlices() const;

  // is the operator spooling to other operators
  bool IsSpooling() const;

  // does the operator do eager materialization
  bool IsEager() const;

  // serialize operator in DXL format

  // conversion function
  static CDXLPhysicalMaterialize *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopPhysicalMaterialize == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLPhysicalMaterialize *>(dxl_op);
  }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *, bool validate_children) const override;
#endif  // GPOS_DEBUG
};
}  // namespace gpdxl
#endif  // !GPDXL_CDXLPhysicalMaterialize_H

// EOF
