//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CPhysicalTVF.cpp
//
//	@doc:
//		Implementation of table-valued functions
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalTVF.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::CPhysicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalTVF::CPhysicalTVF(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type, CWStringConst *str,
                           CColumnDescriptorArray *pdrgpcoldesc, CColRefSet *pcrsOutput)
    : CPhysical(mp),
      m_func_mdid(mdid_func),
      m_return_type_mdid(mdid_return_type),
      m_pstr(str),
      m_pdrgpcoldesc(pdrgpcoldesc),
      m_pcrsOutput(pcrsOutput) {
  GPOS_ASSERT(m_return_type_mdid->IsValid());
  GPOS_ASSERT(nullptr != m_pstr);
  GPOS_ASSERT(nullptr != m_pdrgpcoldesc);
  GPOS_ASSERT(nullptr != m_pcrsOutput);

  CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
  if (m_func_mdid->IsValid()) {
    m_pmdfunc = md_accessor->RetrieveFunc(m_func_mdid);
  } else {
    m_pmdfunc = nullptr;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::~CPhysicalTVF
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalTVF::~CPhysicalTVF() {
  m_func_mdid->Release();
  m_return_type_mdid->Release();
  m_pdrgpcoldesc->Release();
  m_pcrsOutput->Release();
  GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
bool CPhysicalTVF::Matches(COperator *pop) const {
  if (pop->Eopid() == Eopid()) {
    CPhysicalTVF *popTVF = CPhysicalTVF::PopConvert(pop);

    return m_func_mdid->Equals(popTVF->FuncMdId()) && m_return_type_mdid->Equals(popTVF->ReturnTypeMdId()) &&
           m_pdrgpcoldesc == popTVF->Pdrgpcoldesc() && m_pcrsOutput->Equals(popTVF->DeriveOutputColumns());
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to input order
//
//---------------------------------------------------------------------------
bool CPhysicalTVF::FInputOrderSensitive() const {
  return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalTVF::PcrsRequired(CMemoryPool *,        // mp,
                                       CExpressionHandle &,  // exprhdl,
                                       CColRefSet *,         // pcrsRequired,
                                       uint32_t,             // child_index,
                                       CDrvdPropArray *,     // pdrgpdpCtxt
                                       uint32_t              // ulOptReq
) {
  GPOS_ASSERT(!"CPhysicalTVF has no relational children");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalTVF::PosRequired(CMemoryPool *,        // mp,
                                      CExpressionHandle &,  // exprhdl,
                                      COrderSpec *,         // posRequired,
                                      uint32_t,             // child_index,
                                      CDrvdPropArray *,     // pdrgpdpCtxt
                                      uint32_t              // ulOptReq
) const {
  GPOS_ASSERT(!"CPhysicalTVF has no relational children");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalTVF::PcteRequired(CMemoryPool *,        // mp,
                                    CExpressionHandle &,  // exprhdl,
                                    CCTEReq *,            // pcter,
                                    uint32_t,             // child_index,
                                    CDrvdPropArray *,     // pdrgpdpCtxt,
                                    uint32_t              // ulOptReq
) const {
  GPOS_ASSERT(!"CPhysicalTVF has no relational children");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalTVF::FProvidesReqdCols(CExpressionHandle &,  // exprhdl,
                                     CColRefSet *pcrsRequired,
                                     uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);

  return m_pcrsOutput->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalTVF::PosDerive(CMemoryPool *mp,
                                    CExpressionHandle &  // exprhdl
) const {
  return GPOS_NEW(mp) COrderSpec(mp);
}

// derive partition propagation
CPartitionPropagationSpec *CPhysicalTVF::PppsDerive(CMemoryPool *mp, CExpressionHandle &) const {
  return GPOS_NEW(mp) CPartitionPropagationSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PcmDerive
//
//	@doc:
//		Common case of combining cte maps of all logical children
//
//---------------------------------------------------------------------------
CCTEMap *CPhysicalTVF::PcmDerive(CMemoryPool *mp,
                                 CExpressionHandle &  // exprhdl
) const {
  return GPOS_NEW(mp) CCTEMap(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalTVF::EpetOrder(CExpressionHandle &,  // exprhdl
                                                      const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                          peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  return CEnfdProp::EpetRequired;
}
