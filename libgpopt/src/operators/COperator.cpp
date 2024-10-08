//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		COperator.cpp
//
//	@doc:
//		Implementation of operator base class
//---------------------------------------------------------------------------

#include "gpopt/operators/COperator.h"

#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

// generate unique operator ids
uint32_t COperator::m_aulOpIdCounter(0);

//---------------------------------------------------------------------------
//	@function:
//		COperator::COperator
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COperator::COperator(CMemoryPool *mp) : m_ulOpId(m_aulOpIdCounter++), m_mp(mp), m_fPattern(false) {
  GPOS_ASSERT(nullptr != mp);
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::HashValue
//
//	@doc:
//		default hash function based on operator ID
//
//---------------------------------------------------------------------------
uint32_t COperator::HashValue() const {
  uint32_t ulEopid = (uint32_t)Eopid();

  return gpos::HashValue<uint32_t>(&ulEopid);
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &COperator::OsPrint(IOstream &os) const {
  os << this->SzId();
  return os;
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::EfsDeriveFromChildren
//
//	@doc:
//		Derive stability function property from child expressions
//
//---------------------------------------------------------------------------
IMDFunction::EFuncStbl COperator::EfsDeriveFromChildren(CExpressionHandle &exprhdl, IMDFunction::EFuncStbl efsDefault) {
  IMDFunction::EFuncStbl efs = efsDefault;

  const uint32_t arity = exprhdl.Arity();
  for (uint32_t ul = 0; ul < arity; ul++) {
    IMDFunction::EFuncStbl efsChild = exprhdl.PfpChild(ul)->Efs();
    if (efsChild > efs) {
      efs = efsChild;
    }
  }

  return efs;
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::PfpDeriveFromChildren
//
//	@doc:
//		Derive function properties from child expressions
//
//---------------------------------------------------------------------------
CFunctionProp *COperator::PfpDeriveFromChildren(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                IMDFunction::EFuncStbl efsDefault, bool fHasVolatileFunctionScan,
                                                bool fScan) {
  IMDFunction::EFuncStbl efs = EfsDeriveFromChildren(exprhdl, efsDefault);

  return GPOS_NEW(mp) CFunctionProp(efs, fHasVolatileFunctionScan || exprhdl.FChildrenHaveVolatileFuncScan(), fScan);
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::DeriveTableDescriptor
//
//	@doc:
//		Derive table descriptor for tables used by operator
//
//---------------------------------------------------------------------------
CTableDescriptorHashSet *COperator::DeriveTableDescriptor(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  CTableDescriptorHashSet *table_descriptor_set = GPOS_NEW(mp) CTableDescriptorHashSet(mp);

  for (uint32_t ul = 0; ul < exprhdl.Arity(); ul++) {
    CTableDescriptorHashSetIter hsiter(exprhdl.DeriveTableDescriptor(ul));
    while (hsiter.Advance()) {
      CTableDescriptor *ptabdesc = const_cast<CTableDescriptor *>(hsiter.Get());
      if (table_descriptor_set->Insert(ptabdesc)) {
        ptabdesc->AddRef();
      }
    }
  }
  return table_descriptor_set;
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::PopCopyDefault
//
//	@doc:
//		Return an addref'ed copy of the operator
//
//---------------------------------------------------------------------------
COperator *COperator::PopCopyDefault() {
  this->AddRef();
  return this;
}

// EOF
