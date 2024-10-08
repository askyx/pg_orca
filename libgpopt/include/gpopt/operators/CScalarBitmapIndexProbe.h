//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CScalarBitmapIndexProbe.h
//
//	@doc:
//		Bitmap index probe scalar operator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CScalarBitmapIndexProbe_H
#define GPOPT_CScalarBitmapIndexProbe_H

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declarations
class CColRefSet;
class CIndexDescriptor;

//---------------------------------------------------------------------------
//	@class:
//		CScalarBitmapIndexProbe
//
//	@doc:
//		Bitmap index probe scalar operator
//
//---------------------------------------------------------------------------
class CScalarBitmapIndexProbe : public CScalar {
 private:
  // index descriptor
  CIndexDescriptor *m_pindexdesc;

  // table descriptor
  CTableDescriptor *m_ptabdesc;

  // bitmap type id
  IMDId *m_pmdidBitmapType;

  // private copy ctor
  CScalarBitmapIndexProbe(const CScalarBitmapIndexProbe &);

 public:
  // ctor
  CScalarBitmapIndexProbe(CMemoryPool *mp, CIndexDescriptor *pindexdesc, CTableDescriptor *ptabdesc,
                          IMDId *pmdidBitmapType);

  // ctor
  // only for transforms
  explicit CScalarBitmapIndexProbe(CMemoryPool *mp);

  // dtor
  ~CScalarBitmapIndexProbe() override;

  // index descriptor
  CIndexDescriptor *Pindexdesc() const { return m_pindexdesc; }

  // return table's descriptor
  CTableDescriptor *Ptabdesc() const { return m_ptabdesc; }

  // bitmap type id
  IMDId *MdidType() const override { return m_pmdidBitmapType; }

  // identifier
  EOperatorId Eopid() const override { return EopScalarBitmapIndexProbe; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarBitmapIndexProbe"; }

  // operator specific hash function
  uint32_t HashValue() const override;

  // match function
  bool Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return false; }

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *,       // mp,
                                        UlongToColRefMap *,  // colref_mapping,
                                        bool                 // must_exist
                                        ) override {
    return PopCopyDefault();
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

  // conversion
  static CScalarBitmapIndexProbe *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarBitmapIndexProbe == pop->Eopid());

    return dynamic_cast<CScalarBitmapIndexProbe *>(pop);
  }

};  // class CScalarBitmapIndexProbe
}  // namespace gpopt

#endif  // !GPOPT_CScalarBitmapIndexProbe_H

// EOF
