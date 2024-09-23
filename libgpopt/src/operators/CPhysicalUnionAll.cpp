#include "gpopt/operators/CPhysicalUnionAll.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/error/CAutoTrace.h"

using namespace gpopt;

// sensitivity to order of inputs
bool CPhysicalUnionAll::FInputOrderSensitive() const {
  return false;
}

CPhysicalUnionAll::CPhysicalUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput, CColRef2dArray *pdrgpdrgpcrInput)
    : CPhysical(mp), m_pdrgpcrOutput(pdrgpcrOutput), m_pdrgpdrgpcrInput(pdrgpdrgpcrInput), m_pdrgpcrsInput(nullptr) {
  GPOS_ASSERT(nullptr != pdrgpcrOutput);
  GPOS_ASSERT(nullptr != pdrgpdrgpcrInput);

  // build set representation of input columns
  m_pdrgpcrsInput = GPOS_NEW(mp) CColRefSetArray(mp);
  const uint32_t arity = m_pdrgpdrgpcrInput->Size();
  for (uint32_t ulChild = 0; ulChild < arity; ulChild++) {
    CColRefArray *colref_array = (*m_pdrgpdrgpcrInput)[ulChild];
    m_pdrgpcrsInput->Append(GPOS_NEW(mp) CColRefSet(mp, colref_array));
  }
}

CPhysicalUnionAll::~CPhysicalUnionAll() {
  m_pdrgpcrOutput->Release();
  m_pdrgpdrgpcrInput->Release();
  m_pdrgpcrsInput->Release();
}

// accessor of output column array
CColRefArray *CPhysicalUnionAll::PdrgpcrOutput() const {
  return m_pdrgpcrOutput;
}

// accessor of input column array
CColRef2dArray *CPhysicalUnionAll::PdrgpdrgpcrInput() const {
  return m_pdrgpdrgpcrInput;
}

CPhysicalUnionAll *CPhysicalUnionAll::PopConvert(COperator *pop) {
  GPOS_ASSERT(nullptr != pop);

  CPhysicalUnionAll *popPhysicalUnionAll = dynamic_cast<CPhysicalUnionAll *>(pop);
  GPOS_ASSERT(nullptr != popPhysicalUnionAll);

  return popPhysicalUnionAll;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
bool CPhysicalUnionAll::Matches(COperator *pop) const {
  if (Eopid() == pop->Eopid()) {
    CPhysicalUnionAll *popUnionAll = CPhysicalUnionAll::PopConvert(pop);

    return PdrgpcrOutput()->Equals(popUnionAll->PdrgpcrOutput());
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalUnionAll::PcrsRequired(CMemoryPool *mp,
                                            CExpressionHandle &,  // exprhdl,
                                            CColRefSet *pcrsRequired, uint32_t child_index,
                                            CDrvdPropArray *,  // pdrgpdpCtxt
                                            uint32_t           // ulOptReq
) {
  return MapOutputColRefsToInput(mp, pcrsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalUnionAll::PosRequired(CMemoryPool *mp,
                                           CExpressionHandle &,  // exprhdl,
                                           COrderSpec *,         // posRequired,
                                           uint32_t
#ifdef GPOS_DEBUG
                                               child_index
#endif  // GPOS_DEBUG
                                           ,
                                           CDrvdPropArray *,  // pdrgpdpCtxt
                                           uint32_t           // ulOptReq
) const {
  GPOS_ASSERT(PdrgpdrgpcrInput()->Size() > child_index);

  // no order required from child expression
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalUnionAll::PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter,
                                         uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt,
                                         uint32_t  // ulOptReq
) const {
  return PcterNAry(mp, exprhdl, pcter, child_index, pdrgpdpCtxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalUnionAll::FProvidesReqdCols(CExpressionHandle &
#ifdef GPOS_DEBUG
                                              exprhdl
#endif  // GPOS_DEBUG
                                          ,
                                          CColRefSet *pcrsRequired,
                                          uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);
  GPOS_ASSERT(PdrgpdrgpcrInput()->Size() == exprhdl.Arity());

  CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);

  // include output columns
  pcrs->Include(PdrgpcrOutput());
  bool fProvidesCols = pcrs->ContainsAll(pcrsRequired);
  pcrs->Release();

  return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalUnionAll::PosDerive(CMemoryPool *mp,
                                         CExpressionHandle &  // exprhdl
) const {
  // return empty sort order
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalUnionAll::EpetOrder(CExpressionHandle &,  // exprhdl
                                                           const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                               peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  return CEnfdProp::EpetRequired;
}

bool CPhysicalUnionAll::FPassThruStats() const {
  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PdrgpulMap
//
//	@doc:
//		Map given array of scalar identifier expressions to positions of
//		UnionAll input columns in the given child;
//		the function returns NULL if no mapping could be constructed
//
//---------------------------------------------------------------------------
ULongPtrArray *CPhysicalUnionAll::PdrgpulMap(CMemoryPool *mp, CExpressionArray *pdrgpexpr, uint32_t child_index) const {
  GPOS_ASSERT(nullptr != pdrgpexpr);

  CColRefArray *colref_array = (*PdrgpdrgpcrInput())[child_index];
  const uint32_t ulExprs = pdrgpexpr->Size();
  const uint32_t num_cols = colref_array->Size();
  ULongPtrArray *pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
  for (uint32_t ulExpr = 0; ulExpr < ulExprs; ulExpr++) {
    CExpression *pexpr = (*pdrgpexpr)[ulExpr];
    if (COperator::EopScalarIdent != pexpr->Pop()->Eopid()) {
      continue;
    }
    const CColRef *colref = CScalarIdent::PopConvert(pexpr->Pop())->Pcr();
    for (uint32_t ulCol = 0; ulCol < num_cols; ulCol++) {
      if ((*colref_array)[ulCol] == colref) {
        pdrgpul->Append(GPOS_NEW(mp) uint32_t(ulCol));
      }
    }
  }

  if (0 == pdrgpul->Size()) {
    // mapping failed
    pdrgpul->Release();
    pdrgpul = nullptr;
  }

  return pdrgpul;
}

CColRefSet *CPhysicalUnionAll::MapOutputColRefsToInput(CMemoryPool *mp, CColRefSet *out_col_refs,
                                                       uint32_t child_index) {
  CColRefSet *result = GPOS_NEW(mp) CColRefSet(mp);
  CColRefArray *all_outcols = m_pdrgpcrOutput;
  uint32_t total_num_cols = all_outcols->Size();
  CColRefArray *in_colref_array = (*PdrgpdrgpcrInput())[child_index];
  CColRefSetIter iter(*out_col_refs);
  while (iter.Advance()) {
    bool found = false;
    // find the index in the complete list of output columns
    for (uint32_t i = 0; i < total_num_cols && !found; i++) {
      if (iter.Bit() == (*all_outcols)[i]->Id()) {
        // the input colref will have the same index, but in the list of input cols
        result->Include((*in_colref_array)[i]);
        found = true;
      }
    }
    GPOS_ASSERT(found);
  }
  return result;
}
