//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDML.h
//
//	@doc:
//		Class for representing physical DML operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalDML_H
#define GPDXL_CDXLPhysicalDML_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl {
// fwd decl
class CDXLTableDescr;

enum EdxlDmlType { Edxldmlinsert, Edxldmldelete, Edxldmlupdate, EdxldmlSentinel };

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalDML
//
//	@doc:
//		Class for representing physical DML operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalDML : public CDXLPhysical {
 private:
  // operator type
  const EdxlDmlType m_dxl_dml_type;

  // target table descriptor
  CDXLTableDescr *m_dxl_table_descr;

  // list of source column ids
  ULongPtrArray *m_src_colids_array;

  // action column id
  uint32_t m_action_colid;

  // ctid column id
  uint32_t m_ctid_colid;

  // segmentid column id
  uint32_t m_segid_colid;

  // Is Split Update
  bool m_fSplit;

 public:
  CDXLPhysicalDML(const CDXLPhysicalDML &) = delete;

  // ctor
  CDXLPhysicalDML(CMemoryPool *mp, const EdxlDmlType dxl_dml_type, CDXLTableDescr *table_descr,
                  ULongPtrArray *src_colids_array, uint32_t action_colid, uint32_t ctid_colid, uint32_t segid_colid);

  // dtor
  ~CDXLPhysicalDML() override;

  // operator type
  Edxlopid GetDXLOperator() const override;

  // operator name
  const CWStringConst *GetOpNameStr() const override;

  // DML operator type
  EdxlDmlType GetDmlOpType() const { return m_dxl_dml_type; }

  // target table descriptor
  CDXLTableDescr *GetDXLTableDescr() const { return m_dxl_table_descr; }

  // source column ids
  ULongPtrArray *GetSrcColIdsArray() const { return m_src_colids_array; }

  // action column id
  uint32_t ActionColId() const { return m_action_colid; }

  // ctid column id
  uint32_t GetCtIdColId() const { return m_ctid_colid; }

  // segmentid column id
  uint32_t GetSegmentIdColId() const { return m_segid_colid; }

  // Is update using split
  bool FSplit() const { return m_fSplit; }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *node, bool validate_children) const override;
#endif  // GPOS_DEBUG

  // serialize operator in DXL format

  // conversion function
  static CDXLPhysicalDML *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopPhysicalDML == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLPhysicalDML *>(dxl_op);
  }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLPhysicalDML_H

// EOF
