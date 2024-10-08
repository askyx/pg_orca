//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintInterval.cpp
//
//	@doc:
//		Implementation of interval constraints
//---------------------------------------------------------------------------

#include "gpopt/base/CConstraintInterval.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CConstraintDisjunction.h"
#include "gpopt/base/CDatumSortedSet.h"
#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarArray.h"
#include "gpopt/operators/CScalarBooleanTest.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarIsDistinctFrom.h"
#include "gpos/base.h"
#include "gpos/common/CAutoRef.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::CConstraintInterval
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintInterval::CConstraintInterval(CMemoryPool *mp, const CColRef *colref, CRangeArray *pdrgprng,
                                         bool fIncludesNull)
    : CConstraint(mp, GPOS_NEW(mp) CColRefSet(mp)),
      m_pcr(colref),
      m_pdrgprng(pdrgprng),
      m_fIncludesNull(fIncludesNull) {
  GPOS_ASSERT(nullptr != colref);
  GPOS_ASSERT(nullptr != pdrgprng);
  m_pcrsUsed->Include(colref);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::~CConstraintInterval
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintInterval::~CConstraintInterval() {
  m_pdrgprng->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FContradiction
//
//	@doc:
//		Check if this constraint is a contradiction. An interval is a contradiction if
//		it has no ranges and the null flag is not set
//
//---------------------------------------------------------------------------
bool CConstraintInterval::FContradiction() const {
  if (!m_fIncludesNull && 0 == m_pdrgprng->Size()) {
    return true;
  }

  // Constraint on boolean column is special case because only 2 values exist
  // in the domain space [0,1]. If both ends are exclude then the constraint
  // is a contradiction.
  if (m_pcr->RetrieveType()->GetDatumType() == IMDType::EtiBool && m_pdrgprng->Size() == 1 && !m_fIncludesNull) {
    if ((*m_pdrgprng)[0]->EriLeft() == CRange::EriExcluded && (*m_pdrgprng)[0]->EriRight() == CRange::EriExcluded &&
        (*m_pdrgprng)[0]->PdatumLeft() != nullptr && (*m_pdrgprng)[0]->PdatumRight() != nullptr) {
      return true;
    }
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::IsConstraintUnbounded
//
//	@doc:
//		Check if this interval is unbounded. An interval is unbounded if
//		it has a (-inf, inf) range and the null flag is set
//
//---------------------------------------------------------------------------
bool CConstraintInterval::IsConstraintUnbounded() const {
  return (m_fIncludesNull && 1 == m_pdrgprng->Size() && (*m_pdrgprng)[0]->IsConstraintUnbounded());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns
//
//---------------------------------------------------------------------------
CConstraint *CConstraintInterval::PcnstrCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping,
                                                                bool must_exist) {
  CColRef *colref = CUtils::PcrRemap(m_pcr, colref_mapping, must_exist);
  return PcnstrRemapForColumn(mp, colref);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::GetConstraintOnSegmentId
//
//	@doc:
//		Returns the constraint for system column gp_segment_id
//
//---------------------------------------------------------------------------

CConstraint *CConstraintInterval::GetConstraintOnSegmentId() const {
  if (FConstraintOnSegmentId()) {
    return (CConstraint *)this;
  }

  return nullptr;
}
//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarExpr
//
//	@doc:
//		Create interval from scalar expression
//
//		For a given expression pexpr on colref colref, return the CConstraintInterval
//		for which pexpr
//		- evaluates to true (if infer_null_as is false).
//		  This is used for WHERE predicates, which return a row only if the predicate is true.
//		- evaluates to true or null (if infer_null_as is set to true).
//		  This is used for constraints, which are satisfied if the predicate is true or null.
//
//		Let's call the function result r(pexpr) when infer_null_as is set to false,
//		and r'(pexpr) when infer_null_as is set to true. The table below shows how we
//		calculate the intervals for boolean operations AND, OR and NOT:
//
//		Range of a			Equivalent				Comment
//		Boolean expression	expression
//		------------------	---------------------	--------------------------------------------------------
//		r(x and y)			r(x) intersect r(y)		Both x and y must be true for a value of
// c to qualify 		r(x or y)			r(x) union r(y)			One of x or y must be
// true for a value of c to qualify 		r(not x)			complement(r’(x))		x must
// be false 		r’(x and y) r’(x) intersect r’(y)	Both x and y must not be false 		r’(x or y)
// r'(x) union r'(y) At least one of x and y must not be false 		r’(not x)			complement
// (r(x))		x must not be true
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarExpr(CMemoryPool *mp, CExpression *pexpr,
                                                                    CColRef *colref, bool infer_nulls_as,
                                                                    IMDIndex::EmdindexType access_method) {
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(pexpr->Pop()->FScalar());

  // expression must use at most one column
  GPOS_ASSERT(1 >= pexpr->DeriveUsedColumns()->Size());
  CConstraintInterval *pci = nullptr;
  switch (pexpr->Pop()->Eopid()) {
    case COperator::EopScalarNullTest:
      pci = PciIntervalFromScalarNullAndUnknownTest(mp, pexpr, colref);
      break;
    case COperator::EopScalarBooleanTest:
      pci = PciIntervalFromScalarBooleanTest(mp, pexpr, colref);
      break;
    case COperator::EopScalarBoolOp:
      pci = PciIntervalFromScalarBoolOp(mp, pexpr, colref, infer_nulls_as, access_method);
      break;
    case COperator::EopScalarIdent:
      pci = PciIntervalFromScalarIdent(mp, colref, infer_nulls_as);
      break;
    case COperator::EopScalarCmp:
      pci = PciIntervalFromScalarCmp(mp, pexpr, colref, infer_nulls_as, access_method);
      break;
    case COperator::EopScalarIsDistinctFrom:
      pci = PciIntervalFromScalarIDF(mp, pexpr, colref);
      break;
    case COperator::EopScalarConst: {
      if (CUtils::FScalarConstTrue(pexpr)) {
        pci = CConstraintInterval::PciUnbounded(mp, colref, true /*fIncludesNull*/);
      } else {
        pci = GPOS_NEW(mp) CConstraintInterval(mp, colref, GPOS_NEW(mp) CRangeArray(mp), false /*fIncludesNull*/);
      }
    } break;
    case COperator::EopScalarArrayCmp:
      if (GPOS_FTRACE(EopttraceArrayConstraints)) {
        pci = CConstraintInterval::PcnstrIntervalFromScalarArrayCmp(mp, pexpr, colref, infer_nulls_as);
      }
      break;
    default:
      pci = nullptr;
  }

  return pci;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrFromScalarArrayCmp
//
//	@doc:
//		Create constraint from scalar array comparison expression. Returns
//		NULL if a constraint interval cannot be created. Has side effect of
//		removing duplicates
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PcnstrIntervalFromScalarArrayCmp(CMemoryPool *mp, CExpression *pexpr,
                                                                           CColRef *colref, bool infer_nulls_as) {
  if (!(CPredicateUtils::FCompareIdentToConstArray(pexpr) || CPredicateUtils::FCompareCastIdentToConstArray(pexpr))) {
    return nullptr;
  }
#ifdef GPOS_DEBUG
  else {
    // verify column in expr is the same as column which was passed in
    CScalarIdent *popScId = nullptr;
    if (CUtils::FScalarIdent((*pexpr)[0])) {
      popScId = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
    } else {
      GPOS_ASSERT(CScalarIdent::FCastedScId((*pexpr)[0]));
      popScId = CScalarIdent::PopConvert((*(*pexpr)[0])[0]->Pop());
    }
    GPOS_ASSERT(colref == (CColRef *)popScId->Pcr());
  }
#endif  // GPOS_DEBUG

  CScalarArrayCmp *popScArrayCmp = CScalarArrayCmp::PopConvert(pexpr->Pop());
  IMDType::ECmpType cmp_type = CUtils::ParseCmpType(popScArrayCmp->MdIdOp());

  CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
  const uint32_t ulArrayExprArity = CUtils::UlScalarArrayArity(pexprArray);
  if (0 == ulArrayExprArity) {
    return nullptr;
  }

  const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();
  gpos::CAutoRef<CDatumSortedSet> apdatumsortedset(GPOS_NEW(mp) CDatumSortedSet(mp, pexprArray, pcomp));
  // construct ranges representing IN or NOT IN
  CRangeArray *prgrng = GPOS_NEW(mp) CRangeArray(mp);

  switch (cmp_type) {
    case IMDType::EcmptEq: {
      // IN case, create ranges [X, X] [Y, Y] [Z, Z]
      for (uint32_t ul = 0; ul < apdatumsortedset->Size(); ul++) {
        (*apdatumsortedset)[ul]->AddRef();
        CRange *prng = GPOS_NEW(mp) CRange(pcomp, IMDType::EcmptEq, (*apdatumsortedset)[ul]);
        prgrng->Append(prng);
      }
      break;
    }
    case IMDType::EcmptNEq: {
      // NOT IN case, create ranges: (-inf, X) (X, Y) (Y, Z) (Z, inf)
      IDatum *pprevdatum = nullptr;
      IDatum *datum = nullptr;

      for (uint32_t ul = 0; ul < apdatumsortedset->Size(); ul++) {
        if (0 != ul) {
          pprevdatum->AddRef();
        }

        datum = (*apdatumsortedset)[ul];
        datum->AddRef();

        IMDId *mdid = datum->MDId();
        mdid->AddRef();

        CRange *prng = GPOS_NEW(mp) CRange(mdid, pcomp, pprevdatum, CRange::EriExcluded, datum, CRange::EriExcluded);
        prgrng->Append(prng);

        pprevdatum = datum;
      }

      // add the last datum, making range (last, inf)
      IMDId *mdid = pprevdatum->MDId();
      pprevdatum->AddRef();
      mdid->AddRef();
      CRange *prng = GPOS_NEW(mp) CRange(mdid, pcomp, pprevdatum, CRange::EriExcluded, nullptr, CRange::EriExcluded);
      prgrng->Append(prng);
      break;
    }
    default: {
      // does not handle IS DISTINCT FROM
      prgrng->Release();
      return nullptr;
    }
  }

  return GPOS_NEW(mp) CConstraintInterval(mp, colref, prgrng, infer_nulls_as);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromConstraint
//
//	@doc:
//		Create interval from any general constraint that references only one column
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromConstraint(CMemoryPool *mp, CConstraint *pcnstr,
                                                                    CColRef *colref) {
  if (nullptr == pcnstr) {
    GPOS_ASSERT(nullptr != colref && "Must provide valid column reference to construct unbounded interval");
    return PciUnbounded(mp, colref, true /*fIncludesNull*/);
  }

  if (CConstraint::EctInterval == pcnstr->Ect()) {
    pcnstr->AddRef();
    return dynamic_cast<CConstraintInterval *>(pcnstr);
  }

  CColRefSet *pcrsUsed = pcnstr->PcrsUsed();
  GPOS_ASSERT(1 == pcrsUsed->Size());

  CColRef *pcrFirst = pcrsUsed->PcrFirst();
  GPOS_ASSERT_IMP(nullptr != colref, pcrFirst == colref);

  CExpression *pexprScalar = pcnstr->PexprScalar(mp);

  return PciIntervalFromScalarExpr(mp, pexprScalar, pcrFirst);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarNullAndUnknownTest
//
//	@doc:
//		Create interval from scalar null test & unknown test
//
//		Returns an empty interval includes null, appropriate for
//		"is null" and "is unknown"
//
//		For "is not null",
//		PciComplement will be invoked by PciIntervalFromScalarBoolOp
//		For "is not unknown",
//		PciComplement will be invoked by PciIntervalFromScalarBooleanTest
//
//		Notice: this function only supports cases where the child is an
//		identifier now, otherwise, it will return nullptr
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarNullAndUnknownTest(CMemoryPool *mp, CExpression *pexpr,
                                                                                  CColRef *colref) {
  GPOS_ASSERT(nullptr != pexpr);
  // let (is null & is not null) and (is unknown & is not unknown) in
  GPOS_ASSERT(CUtils::FScalarNullTest(pexpr) ||
              (CUtils::FScalarBooleanTest(pexpr) &&
               (CScalarBooleanTest::PopConvert(pexpr->Pop())->Ebt() == CScalarBooleanTest::EbtIsUnknown ||
                CScalarBooleanTest::PopConvert(pexpr->Pop())->Ebt() == CScalarBooleanTest::EbtIsNotUnknown)));

  // child of comparison operator
  CExpression *pexprChild = (*pexpr)[0];

  // TODO:  - May 28, 2012; add support for other expression forms
  // besides (ident is null)
  // think about how to differ ((ident is true) is null) from (ident is null)
  // when ident is null

  if (CUtils::FScalarIdent(pexprChild)) {
#ifdef GPOS_DEBUG
    CScalarIdent *popScId = CScalarIdent::PopConvert(pexprChild->Pop());
    GPOS_ASSERT(colref == (CColRef *)popScId->Pcr());
#endif  // GPOS_DEBUG
    return GPOS_NEW(mp) CConstraintInterval(mp, colref, GPOS_NEW(mp) CRangeArray(mp), true /*fIncludesNull*/);
  }

  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBooleanTest
//
//	@doc:
//		Create interval from scalar null test
//
//		This is the table about results of boolean tests &
//		some other boolean operators
//
//		Parameter		T	F	NULL
//		----------------------------
//		is true			T	F	F
//		is not false	T	F	T
//		is false		F	T	F
//		is not true		F	T	T
//		is unknown		F	F	T
//		is not unknown	T	T	F
//		----------------------------
//		expr			T	F	infer_nulls_as
//		not				F	T	infer_nulls_as
//		is null			F	F	T
//		is not null		T	T	F
//
//		So, we treat
//		(is true / is not false)	as	expr with infer_nulls_as
//		(is false / is not true)	as	not with infer_nulls_as
//		is unknown 					as	is null
//		is not unknown 				as	is not null
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarBooleanTest(CMemoryPool *mp, CExpression *pexpr,
                                                                           CColRef *colref) {
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(CUtils::FScalarBooleanTest(pexpr));

  CScalarBooleanTest *pop = CScalarBooleanTest::PopConvert(pexpr->Pop());
  GPOS_ASSERT(nullptr != pop);

  bool fIncludesNull = false;
  switch (pop->Ebt()) {
    case CScalarBooleanTest::EbtIsTrue:
    case CScalarBooleanTest::EbtIsNotFalse: {
      if (pop->Ebt() == CScalarBooleanTest::EbtIsNotFalse) {
        fIncludesNull = true;
      }

      return PciIntervalFromScalarExpr(mp, (*pexpr)[0], colref, fIncludesNull);
    }
    case CScalarBooleanTest::EbtIsNotTrue:
    case CScalarBooleanTest::EbtIsFalse: {
      if (pop->Ebt() == CScalarBooleanTest::EbtIsNotTrue) {
        fIncludesNull = true;
      }

      CConstraintInterval *pciChild = PciIntervalFromScalarExpr(mp, (*pexpr)[0], colref, !fIncludesNull);
      if (nullptr == pciChild) {
        return nullptr;
      }

      CConstraintInterval *pciNot = pciChild->PciComplement(mp);
      pciChild->Release();
      return pciNot;
    }
    case CScalarBooleanTest::EbtIsUnknown: {
      return PciIntervalFromScalarNullAndUnknownTest(mp, pexpr, colref);
    }
    case CScalarBooleanTest::EbtIsNotUnknown: {
      CConstraintInterval *pciNullTest = PciIntervalFromScalarNullAndUnknownTest(mp, pexpr, colref);
      if (nullptr == pciNullTest) {
        return nullptr;
      }

      CConstraintInterval *pciNot = pciNullTest->PciComplement(mp);
      pciNullTest->Release();
      return pciNot;
    }
    default: {
      GPOS_ASSERT(false && "Unknown boolean test type");
      return nullptr;
    }
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarCmp
//
//	@doc:
//		Helper for create interval from comparison between a column and
//		a constant
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromColConstCmp(CMemoryPool *mp, CColRef *colref,
                                                                     IMDType::ECmpType cmp_type,
                                                                     CScalarConst *popScConst, bool infer_nulls_as) {
  CConstraintInterval *pcri = nullptr;
  CRangeArray *pdrngprng = PciRangeFromColConstCmp(mp, cmp_type, popScConst);

  if (nullptr != pdrngprng) {
    // (col = const) usually implies (col IS NOT NULL) for these ops since
    // NULLs are inferred as false.  But, if asked to infer NULLS as true (e.g
    // in table constraints), include NULL in the final interval.
    pcri = GPOS_NEW(mp) CConstraintInterval(mp, colref, pdrngprng, infer_nulls_as);
  }
  return pcri;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarCmp
//
//	@doc:
//		Create interval from scalar comparison expression
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarCmp(CMemoryPool *mp, CExpression *pexpr, CColRef *colref,
                                                                   bool infer_nulls_as,
                                                                   IMDIndex::EmdindexType access_method) {
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(CUtils::FScalarCmp(pexpr) || CUtils::FScalarArrayCmp(pexpr));

  // TODO:  - May 28, 2012; add support for other expression forms
  // besides (column relop const)
  if (CPredicateUtils::FCompareIdentToConst(pexpr)) {
    // column
#ifdef GPOS_DEBUG
    CScalarIdent *popScId;
    CExpression *pexprLeft = (*pexpr)[0];
    if (CUtils::FScalarIdent((*pexpr)[0])) {
      popScId = CScalarIdent::PopConvert(pexprLeft->Pop());
    } else {
      GPOS_ASSERT(CCastUtils::FBinaryCoercibleCastedScId(pexprLeft));
      popScId = CScalarIdent::PopConvert((*pexprLeft)[0]->Pop());
    }
    GPOS_ASSERT(colref == (CColRef *)popScId->Pcr());
#endif  // GPOS_DEBUG

    // constant
    CExpression *pexprRight = (*pexpr)[1];
    CScalarConst *popScConst;
    if (CUtils::FScalarConst(pexprRight)) {
      popScConst = CScalarConst::PopConvert(pexprRight->Pop());
    } else {
      GPOS_ASSERT(CCastUtils::FBinaryCoercibleCastedConst(pexprRight));
      popScConst = CScalarConst::PopConvert((*pexprRight)[0]->Pop());
    }
    CScalarCmp *popScCmp = CScalarCmp::PopConvert(pexpr->Pop());

    return PciIntervalFromColConstCmp(mp, colref, popScCmp->ParseCmpType(), popScConst, infer_nulls_as);
  }

  return nullptr;
}

CConstraintInterval *CConstraintInterval::PciIntervalFromScalarIDF(CMemoryPool *mp, CExpression *pexpr,
                                                                   CColRef *colref) {
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(CPredicateUtils::FIDF(pexpr));

  if (CPredicateUtils::FIdentIDFConst(pexpr)) {
    // column
#ifdef GPOS_DEBUG
    CScalarIdent *popScId = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
    GPOS_ASSERT(colref == (CColRef *)popScId->Pcr());
#endif  // GPOS_DEBUG

    // constant
    CScalarConst *popScConst = CScalarConst::PopConvert((*pexpr)[1]->Pop());
    // operator
    CScalarIsDistinctFrom *popScCmp = CScalarIsDistinctFrom::PopConvert(pexpr->Pop());

    GPOS_ASSERT(CScalar::EopScalarConst == popScConst->Eopid());
    GPOS_ASSERT(IMDType::EcmptIDF == popScCmp->ParseCmpType());

    IDatum *datum = popScConst->GetDatum();
    CConstraintInterval *pcri = nullptr;

    if (datum->IsNull()) {
      // col IS DISTINCT FROM NULL
      CConstraintInterval *pcriChild =
          GPOS_NEW(mp) CConstraintInterval(mp, colref, GPOS_NEW(mp) CRangeArray(mp), true /*fIncludesNull*/);
      pcri = pcriChild->PciComplement(mp);
      pcriChild->Release();
    } else {
      // col IS DISTINCT FROM const
      CRangeArray *pdrgprng = PciRangeFromColConstCmp(mp, popScCmp->ParseCmpType(), popScConst);
      if (nullptr != pdrgprng) {
        pcri = GPOS_NEW(mp) CConstraintInterval(mp, colref, pdrgprng, true /*fIncludesNull*/);
      }
    }

    return pcri;
  }
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolOp
//
//	@doc:
//		Create interval from scalar boolean: AND, OR, NOT
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarBoolOp(CMemoryPool *mp, CExpression *pexpr,
                                                                      CColRef *colref, bool infer_nulls_as,
                                                                      IMDIndex::EmdindexType access_method) {
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));

  CScalarBoolOp *popScBool = CScalarBoolOp::PopConvert(pexpr->Pop());
  CScalarBoolOp::EBoolOperator eboolop = popScBool->Eboolop();

  switch (eboolop) {
    case CScalarBoolOp::EboolopAnd:
      return PciIntervalFromScalarBoolAnd(mp, pexpr, colref, infer_nulls_as, access_method);

    case CScalarBoolOp::EboolopOr:
      return PciIntervalFromScalarBoolOr(mp, pexpr, colref, infer_nulls_as, access_method);

    case CScalarBoolOp::EboolopNot: {
      CConstraintInterval *pciChild = PciIntervalFromScalarExpr(mp, (*pexpr)[0], colref, !infer_nulls_as);
      if (nullptr == pciChild) {
        return nullptr;
      }

      CConstraintInterval *pciNot = pciChild->PciComplement(mp);
      pciChild->Release();
      return pciNot;
    }
    default:
      return nullptr;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarIdent
//
//	@doc:
//		Create interval from boolean scalar ident
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarIdent(CMemoryPool *mp, CColRef *colref,
                                                                     bool infer_nulls_as) {
  GPOS_ASSERT(colref->RetrieveType()->GetDatumType() == IMDType::EtiBool);

  CRangeArray *pdrngprng = GPOS_NEW(mp) CRangeArray(mp);
  CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
  const IMDTypeBool *pmdtypebool = md_accessor->PtMDType<IMDTypeBool>();
  IDatumBool *datum = pmdtypebool->CreateBoolDatum(mp, true, false /*is_null*/);
  pdrngprng->Append(GPOS_NEW(mp) CRange(COptCtxt::PoctxtFromTLS()->Pcomp(), IMDType::EcmptEq, datum));

  return GPOS_NEW(mp) CConstraintInterval(mp, colref, pdrngprng, infer_nulls_as /*fIncludesNull*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolOr
//
//	@doc:
//		Create interval from scalar boolean OR
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarBoolOr(CMemoryPool *mp, CExpression *pexpr,
                                                                      CColRef *colref, bool infer_nulls_as,
                                                                      IMDIndex::EmdindexType access_method) {
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
  GPOS_ASSERT(CScalarBoolOp::EboolopOr == CScalarBoolOp::PopConvert(pexpr->Pop())->Eboolop());

  const uint32_t arity = pexpr->Arity();
  GPOS_ASSERT(0 < arity);

  CConstraintIntervalArray *child_constraints = GPOS_NEW(mp) CConstraintIntervalArray(mp);
  for (uint32_t ul = 0; ul < arity; ul++) {
    CConstraintInterval *pciChild = PciIntervalFromScalarExpr(mp, (*pexpr)[ul], colref, infer_nulls_as, access_method);

    if (nullptr == pciChild) {
      child_constraints->Release();
      return nullptr;
    }

    child_constraints->Append(pciChild);
  }

  CConstraintIntervalArray *constraints;

  // PciUnion each interval in pairs. Given intervals I1,I2.., I5, perform the unions as follows:
  // iteration 1: I1 U I2, I3 U I4, I5
  // iteration 2: I12 U I34, I5
  // iteration 3: I1234 U I5
  while (child_constraints->Size() > 1) {
    constraints = GPOS_NEW(mp) CConstraintIntervalArray(mp);

    uint32_t length = child_constraints->Size();
    uint32_t ul;

    for (ul = 0; ul < length - 1; ul += 2) {
      CConstraintInterval *pci1 = (*child_constraints)[ul];
      CConstraintInterval *pci2 = (*child_constraints)[ul + 1];

      CConstraintInterval *pciOr = pci1->PciUnion(mp, pci2);
      constraints->Append(pciOr);
    }

    if (ul < length) {
      // append the odd one at the end
      CConstraintInterval *pciChild = (*child_constraints)[ul];
      pciChild->AddRef();
      constraints->Append(pciChild);
    }
    child_constraints->Release();
    child_constraints = constraints;
  }
  GPOS_ASSERT(child_constraints->Size() == 1);
  CConstraintInterval *dest = (*child_constraints)[0];
  dest->AddRef();
  child_constraints->Release();

  return dest;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolAnd
//
//	@doc:
//		Create interval from scalar boolean AND
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntervalFromScalarBoolAnd(CMemoryPool *mp, CExpression *pexpr,
                                                                       CColRef *colref, bool infer_nulls_as,
                                                                       IMDIndex::EmdindexType access_method) {
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
  GPOS_ASSERT(CScalarBoolOp::EboolopAnd == CScalarBoolOp::PopConvert(pexpr->Pop())->Eboolop());

  const uint32_t arity = pexpr->Arity();
  GPOS_ASSERT(0 < arity);

  CConstraintInterval *pci = PciIntervalFromScalarExpr(mp, (*pexpr)[0], colref, infer_nulls_as, access_method);
  for (uint32_t ul = 1; ul < arity; ul++) {
    CConstraintInterval *pciChild = PciIntervalFromScalarExpr(mp, (*pexpr)[ul], colref, infer_nulls_as, access_method);
    // here is where we will return a NULL child from not being able to create a
    // CConstraint interval from the ScalarExpr
    if (nullptr != pciChild && nullptr != pci) {
      CConstraintInterval *pciAnd = pci->PciIntersect(mp, pciChild);
      pci->Release();
      pciChild->Release();
      pci = pciAnd;
    } else if (nullptr != pciChild) {
      pci = pciChild;
    }
  }

  return pci;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprScalar
//
//	@doc:
//		Return scalar expression
//
//---------------------------------------------------------------------------
CExpression *CConstraintInterval::PexprScalar(CMemoryPool *mp) {
  if (nullptr == m_pexprScalar) {
    m_pexprScalar = PexprConstructScalar(mp);
  }

  return m_pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructScalar
//
//	@doc:
//		Construct scalar expression
//
//---------------------------------------------------------------------------
CExpression *CConstraintInterval::PexprConstructScalar(CMemoryPool *mp) const {
  if (FContradiction()) {
    return CUtils::PexprScalarConstBool(mp, false /*fval*/, false /*is_null*/);
  }

  if (GPOS_FTRACE(EopttraceArrayConstraints)) {
    // try creating an array IN/NOT IN expression
    CExpression *pexpr = PexprConstructArrayScalar(mp);
    if (pexpr != nullptr) {
      return pexpr;
    }
  }

  // otherwise, we generate a disjunction of ranges
  return PexprConstructDisjunctionScalar(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructDisjunctionScalar
//
//	@doc:
//		Returns a disjunction of several equality or inequality expressions
//		describing this interval. Or, returns a singular expression if the
//		interval can be represented as such.
//		For example an interval containing ranges like
//			[1,1],(7,inf)
//		converts to an expression like
//			x = 1 OR x > 7
//		but an interval containing the range
//			(-inf, inf)
//		converts to a scalar true
//
//---------------------------------------------------------------------------
CExpression *CConstraintInterval::PexprConstructDisjunctionScalar(CMemoryPool *mp) const {
  CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

  const uint32_t length = m_pdrgprng->Size();
  for (uint32_t ul = 0; ul < length; ul++) {
    CRange *prange = (*m_pdrgprng)[ul];
    CExpression *pexprChild = prange->PexprScalar(mp, m_pcr);
    pdrgpexpr->Append(pexprChild);
  }

  if (1 == pdrgpexpr->Size() && CUtils::FScalarConstTrue((*pdrgpexpr)[0])) {
    // so far, interval covers all the not null values
    pdrgpexpr->Release();

    if (m_fIncludesNull) {
      return CUtils::PexprScalarConstBool(mp, true /*fval*/, false /*is_null*/);
    }

    return CUtils::PexprIsNotNull(mp, CUtils::PexprScalarIdent(mp, m_pcr));
  }

  if (m_fIncludesNull) {
    CExpression *pexprIsNull = CUtils::PexprIsNull(mp, CUtils::PexprScalarIdent(mp, m_pcr));
    pdrgpexpr->Append(pexprIsNull);
  }

  return CPredicateUtils::PexprDisjunction(mp, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FConvertsToIn
//
//	@doc:
//		Looks for a specific pattern within the array of ranges to determine
//		if this interval can be converted into an array IN statement. The
//		pattern is like [[n,n], [m,m]] is an IN
//
//---------------------------------------------------------------------------
bool CConstraintInterval::FConvertsToIn() const {
  if (1 >= m_pdrgprng->Size()) {
    return false;
  }

  bool isIN = true;
  const uint32_t length = m_pdrgprng->Size();
  for (uint32_t ul = 0; ul < length && isIN; ul++) {
    isIN &= (*m_pdrgprng)[ul]->FPoint();
  }
  return isIN;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FConvertsToNotIn
//
//	@doc:
//		Looks for a specific pattern within the array of ranges to determine
//		if this interval can be converted into an array NOT IN statement. The
//		pattern is like [(-inf, m), (m, n), (n, inf)]
//
//---------------------------------------------------------------------------
bool CConstraintInterval::FConvertsToNotIn() const {
  if (1 >= m_pdrgprng->Size()) {
    return false;
  }

  // for this to be a NOT IN, its edges must be unbounded
  if ((*m_pdrgprng)[0]->PdatumLeft() != nullptr || (*m_pdrgprng)[m_pdrgprng->Size() - 1]->PdatumRight() != nullptr) {
    return false;
  }

  // check that each range is exclusive and that the inner values are equal
  bool isNotIn = true;
  CRange *pLeftRng = (*m_pdrgprng)[0];
  CRange *pRightRng = nullptr;
  const uint32_t length = m_pdrgprng->Size();
  for (uint32_t ul = 1; ul < length && isNotIn; ul++) {
    pRightRng = (*m_pdrgprng)[ul];
    isNotIn &= pLeftRng->EriRight() == CRange::EriExcluded;
    isNotIn &= pRightRng->EriLeft() == CRange::EriExcluded;
    isNotIn &= pLeftRng->FUpperBoundEqualsLowerBound(pRightRng);

    pLeftRng = pRightRng;
  }

  return isNotIn;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructArrayScalar
//
//	@doc:
//		Constructs an array expression from the ranges stored in this interval.
//		It is a mistake to call this method without first detecting if the
//		stored ranges can be converted to an IN or NOT in statement. The param
//		'fIn' refers to the statement being an IN statement, and if set to false,
// 		it is considered a NOT IN statement
//
//---------------------------------------------------------------------------
CExpression *CConstraintInterval::PexprConstructArrayScalar(CMemoryPool *mp, bool fIn) const {
  GPOS_ASSERT(FConvertsToIn() || FConvertsToNotIn());

  uint32_t ulRngs = m_pdrgprng->Size();
  IMDType::ECmpType ecmptype = IMDType::EcmptEq;
  CScalarArrayCmp::EArrCmpType earraycmptype = CScalarArrayCmp::EarrcmpAny;

  if (!fIn) {
    ecmptype = IMDType::EcmptNEq;
    earraycmptype = CScalarArrayCmp::EarrcmpAll;

    // if NOT IN, we skip the last range, as the right datum will be null
    ulRngs -= 1;
  }

  // loop through all of the constants in the ranges, creating an array of CScalarConst Expressions
  CExpressionArray *prngexpr = GPOS_NEW(mp) CExpressionArray(mp);

  // this method assumes IN or NOT IN which means that the ranges stored will look like either
  // [x,x], ... ,[y,y] or the NOT IN case (-inf, x),(x,y), ... ,(z,inf).
  for (uint32_t ul = 0; ul < ulRngs; ul++) {
    IDatum *datum = (*m_pdrgprng)[ul]->PdatumRight();
    datum->AddRef();
    CScalarConst *popScConst = GPOS_NEW(mp) CScalarConst(mp, datum);
    CExpression *pexpr = GPOS_NEW(mp) CExpression(mp, popScConst);
    prngexpr->Append(pexpr);
  }

  CExpression *pexpr = CUtils::PexprScalarArrayCmp(mp, earraycmptype, ecmptype, prngexpr, m_pcr);

  if (m_fIncludesNull) {
    CExpression *pexprIsNull = CUtils::PexprIsNull(mp, CUtils::PexprScalarIdent(mp, m_pcr));
    CExpression *pexprDisjuction = CPredicateUtils::PexprDisjunction(mp, pexpr, pexprIsNull);
    pexpr->Release();
    pexprIsNull->Release();
    pexpr = pexprDisjuction;
  }

  return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructScalar
//
//	@doc:
//		Constructs an array expression if the interval can be converted into
//		an array expression. Returns null if an array scalar cannot be
//		constructed
//
//---------------------------------------------------------------------------
CExpression *CConstraintInterval::PexprConstructArrayScalar(CMemoryPool *mp) const {
  if (1 >= m_pdrgprng->Size()) {
    return nullptr;
  }

  if (FConvertsToIn()) {
    return PexprConstructArrayScalar(mp, true);
  } else if (FConvertsToNotIn()) {
    return PexprConstructArrayScalar(mp, false);
  } else {
    // Does not convert to either IN or NOT IN
    return nullptr;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint *CConstraintInterval::Pcnstr(CMemoryPool *,  // mp,
                                         const CColRef *colref) {
  if (m_pcr == colref) {
    this->AddRef();
    return this;
  }

  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint *CConstraintInterval::Pcnstr(CMemoryPool *,  // mp,
                                         CColRefSet *pcrs) {
  if (pcrs->FMember(m_pcr)) {
    this->AddRef();
    return this;
  }

  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint *CConstraintInterval::PcnstrRemapForColumn(CMemoryPool *mp, CColRef *colref) const {
  GPOS_ASSERT(nullptr != colref);
  m_pdrgprng->AddRef();
  return GPOS_NEW(mp) CConstraintInterval(mp, colref, m_pdrgprng, m_fIncludesNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntersect
//
//	@doc:
//		Intersection with another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciIntersect(CMemoryPool *mp, CConstraintInterval *pci) {
  GPOS_ASSERT(nullptr != pci);
  GPOS_ASSERT(m_pcr == pci->Pcr());

  CRangeArray *pdrgprngOther = pci->Pdrgprng();

  CRangeArray *pdrgprngNew = GPOS_NEW(mp) CRangeArray(mp);

  uint32_t ulFst = 0;
  uint32_t ulSnd = 0;
  const uint32_t ulNumRangesFst = m_pdrgprng->Size();
  const uint32_t ulNumRangesSnd = pdrgprngOther->Size();
  while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd) {
    CRange *prangeThis = (*m_pdrgprng)[ulFst];
    CRange *prangeOther = (*pdrgprngOther)[ulSnd];

    CRange *prangeNew = nullptr;
    if (prangeOther->FEndsAfter(prangeThis)) {
      prangeNew = prangeThis->PrngIntersect(mp, prangeOther);
      ulFst++;
    } else {
      prangeNew = prangeOther->PrngIntersect(mp, prangeThis);
      ulSnd++;
    }

    if (nullptr != prangeNew) {
      pdrgprngNew->Append(prangeNew);
    }
  }

  return GPOS_NEW(mp) CConstraintInterval(mp, m_pcr, pdrgprngNew, m_fIncludesNull && pci->FIncludesNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnion
//
//	@doc:
//		Union with another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciUnion(CMemoryPool *mp, CConstraintInterval *pci) {
  GPOS_ASSERT(nullptr != pci);
  GPOS_ASSERT(m_pcr == pci->Pcr());

  CRangeArray *pdrgprngOther = pci->Pdrgprng();

  CRangeArray *pdrgprngNew = GPOS_NEW(mp) CRangeArray(mp);

  uint32_t ulFst = 0;
  uint32_t ulSnd = 0;
  const uint32_t ulNumRangesFst = m_pdrgprng->Size();
  const uint32_t ulNumRangesSnd = pdrgprngOther->Size();
  while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd) {
    CRange *prangeThis = (*m_pdrgprng)[ulFst];
    CRange *prangeOther = (*pdrgprngOther)[ulSnd];

    CRange *prangeNew = nullptr;
    if (prangeOther->FEndsAfter(prangeThis)) {
      prangeNew = prangeThis->PrngDifferenceLeft(mp, prangeOther);
      ulFst++;
    } else {
      prangeNew = prangeOther->PrngDifferenceLeft(mp, prangeThis);
      ulSnd++;
    }

    AppendOrExtend(mp, pdrgprngNew, prangeNew);
  }

  AddRemainingRanges(mp, m_pdrgprng, ulFst, pdrgprngNew);
  AddRemainingRanges(mp, pdrgprngOther, ulSnd, pdrgprngNew);

  return GPOS_NEW(mp) CConstraintInterval(mp, m_pcr, pdrgprngNew, m_fIncludesNull || pci->FIncludesNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciDifference
//
//	@doc:
//		Difference between this interval and another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciDifference(CMemoryPool *mp, CConstraintInterval *pci) {
  GPOS_ASSERT(nullptr != pci);
  GPOS_ASSERT(m_pcr == pci->Pcr());

  CRangeArray *pdrgprngOther = pci->Pdrgprng();

  CRangeArray *pdrgprngNew = GPOS_NEW(mp) CRangeArray(mp);

  uint32_t ulFst = 0;
  uint32_t ulSnd = 0;
  CRangeArray *pdrgprngResidual = GPOS_NEW(mp) CRangeArray(mp);
  CRange *prangeResidual = nullptr;
  const uint32_t ulNumRangesFst = m_pdrgprng->Size();
  const uint32_t ulNumRangesSnd = pdrgprngOther->Size();
  while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd) {
    // if there is a residual range from previous iteration then use it
    CRange *prangeThis = (nullptr == prangeResidual ? (*m_pdrgprng)[ulFst] : prangeResidual);
    CRange *prangeOther = (*pdrgprngOther)[ulSnd];

    CRange *prangeNew = nullptr;
    prangeResidual = nullptr;

    if (prangeOther->FEndsWithOrAfter(prangeThis)) {
      prangeNew = prangeThis->PrngDifferenceLeft(mp, prangeOther);
      ulFst++;
    } else {
      prangeNew = PrangeDiffWithRightResidual(mp, prangeThis, prangeOther, &prangeResidual, pdrgprngResidual);
      ulSnd++;
    }

    AppendOrExtend(mp, pdrgprngNew, prangeNew);
  }

  if (nullptr != prangeResidual) {
    ulFst++;
    prangeResidual->AddRef();
  }

  AppendOrExtend(mp, pdrgprngNew, prangeResidual);
  pdrgprngResidual->Release();
  AddRemainingRanges(mp, m_pdrgprng, ulFst, pdrgprngNew);

  return GPOS_NEW(mp) CConstraintInterval(mp, m_pcr, pdrgprngNew, m_fIncludesNull && !pci->FIncludesNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FContainsInterval
//
//	@doc:
//		Does the current interval contain the given interval?
//
//---------------------------------------------------------------------------
bool CConstraintInterval::FContainsInterval(CMemoryPool *mp, CConstraintInterval *pci) {
  GPOS_ASSERT(nullptr != pci);
  GPOS_ASSERT(m_pcr == pci->Pcr());

  if (IsConstraintUnbounded()) {
    return true;
  }

  if (nullptr == pci || pci->IsConstraintUnbounded() || (!FIncludesNull() && pci->FIncludesNull())) {
    return false;
  }

  CConstraintInterval *pciDiff = pci->PciDifference(mp, this);

  // if the difference is empty, then this interval contains the given one
  bool fContains = pciDiff->FContradiction();
  pciDiff->Release();

  return fContains;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnbounded
//
//	@doc:
//		Create an unbounded interval
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciUnbounded(CMemoryPool *mp, const CColRef *colref, bool fIncludesNull) {
  IMDId *mdid = colref->RetrieveType()->MDId();
  if (!CUtils::FConstrainableType(mdid)) {
    return nullptr;
  }

  mdid->AddRef();

  CRangeArray *pdrgprng = GPOS_NEW(mp) CRangeArray(mp);
  if (colref->RetrieveType()->GetDatumType() == IMDType::EtiBool) {
    // valid boolean constraint values must map in the range [0, 1]
    CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
    const IMDTypeBool *pmdtypebool = md_accessor->PtMDType<IMDTypeBool>();
    CRange *prange = GPOS_NEW(mp)
        CRange(mdid, COptCtxt::PoctxtFromTLS()->Pcomp(),
               pmdtypebool->CreateBoolDatum(mp, false, false /*is_null*/) /*ppointLeft*/, CRange::EriIncluded,
               pmdtypebool->CreateBoolDatum(mp, true, false /*is_null*/) /*ppointRight*/, CRange::EriIncluded);
    pdrgprng->Append(prange);
  } else {
    CRange *prange = GPOS_NEW(mp) CRange(mdid, COptCtxt::PoctxtFromTLS()->Pcomp(), nullptr /*ppointLeft*/,
                                         CRange::EriExcluded, nullptr /*ppointRight*/, CRange::EriExcluded);
    pdrgprng->Append(prange);
  }

  return GPOS_NEW(mp) CConstraintInterval(mp, colref, pdrgprng, fIncludesNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnbounded
//
//	@doc:
//		Create an unbounded interval on any column from the given set
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciUnbounded(CMemoryPool *mp, const CColRefSet *pcrs, bool fIncludesNull) {
  // find the first constrainable column
  CColRefSetIter crsi(*pcrs);
  while (crsi.Advance()) {
    CColRef *colref = crsi.Pcr();
    CConstraintInterval *pci = PciUnbounded(mp, colref, fIncludesNull);
    if (nullptr != pci) {
      return pci;
    }
  }

  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::MdidType
//
//	@doc:
//		Type of this interval
//
//---------------------------------------------------------------------------
IMDId *CConstraintInterval::MdidType() {
  // if there is at least one range, return range type
  if (0 < m_pdrgprng->Size()) {
    CRange *prange = (*m_pdrgprng)[0];
    return prange->MDId();
  }

  // otherwise return type of column ref
  return m_pcr->RetrieveType()->MDId();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciComplement
//
//	@doc:
//		Complement of this interval
//
//---------------------------------------------------------------------------
CConstraintInterval *CConstraintInterval::PciComplement(CMemoryPool *mp) {
  // create an unbounded interval
  CConstraintInterval *pciUniversal = PciUnbounded(mp, m_pcr, true /*fIncludesNull*/);

  CConstraintInterval *pciComp = pciUniversal->PciDifference(mp, this);
  pciUniversal->Release();

  return pciComp;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PrangeDiffWithRightResidual
//
//	@doc:
//		Difference between two ranges on the left side only -
//		Any difference on the right side is reported as residual range
//
//		this    |----------------------|
//		prange         |-----------|
//		result  |------|
//		residual                   |---|
//---------------------------------------------------------------------------
CRange *CConstraintInterval::PrangeDiffWithRightResidual(CMemoryPool *mp, CRange *prangeFirst, CRange *prangeSecond,
                                                         CRange **pprangeResidual, CRangeArray *pdrgprngResidual) {
  if (prangeSecond->FDisjointLeft(prangeFirst)) {
    return nullptr;
  }

  CRange *prangeRet = nullptr;

  if (prangeFirst->Contains(prangeSecond)) {
    prangeRet = prangeFirst->PrngDifferenceLeft(mp, prangeSecond);
  }

  // the part of prangeFirst that goes beyond prangeSecond
  *pprangeResidual = prangeFirst->PrngDifferenceRight(mp, prangeSecond);
  // add it to array so we can release it later on
  if (nullptr != *pprangeResidual) {
    pdrgprngResidual->Append(*pprangeResidual);
  }

  return prangeRet;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::AddRemainingRanges
//
//	@doc:
//		Add ranges from a source array to a destination array, starting at the
//		range with the given index
//
//---------------------------------------------------------------------------
void CConstraintInterval::AddRemainingRanges(CMemoryPool *mp, CRangeArray *pdrgprngSrc, uint32_t ulStart,
                                             CRangeArray *pdrgprngDest) {
  const uint32_t length = pdrgprngSrc->Size();
  for (uint32_t ul = ulStart; ul < length; ul++) {
    CRange *prange = (*pdrgprngSrc)[ul];
    prange->AddRef();
    AppendOrExtend(mp, pdrgprngDest, prange);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::AppendOrExtend
//
//	@doc:
//		Append the given range to the array or extend the last range in that
//		array
//
//---------------------------------------------------------------------------
void CConstraintInterval::AppendOrExtend(CMemoryPool *mp, CRangeArray *pdrgprng, CRange *prange) {
  if (nullptr == prange) {
    return;
  }

  GPOS_ASSERT(nullptr != pdrgprng);

  const uint32_t length = pdrgprng->Size();
  if (0 == length) {
    pdrgprng->Append(prange);
    return;
  }

  CRange *prangeLast = (*pdrgprng)[length - 1];
  CRange *prangeNew = prangeLast->PrngExtend(mp, prange);
  if (nullptr == prangeNew) {
    pdrgprng->Append(prange);
  } else {
    pdrgprng->Replace(length - 1, prangeNew);
    prange->Release();
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::OsPrint
//
//	@doc:
//		Debug print interval
//
//---------------------------------------------------------------------------
IOstream &CConstraintInterval::OsPrint(IOstream &os) const {
  os << "{";
  m_pcr->OsPrint(os);
  const uint32_t length = m_pdrgprng->Size();
  os << ", ranges: ";
  for (uint32_t ul = 0; ul < length; ul++) {
    CRange *prange = (*m_pdrgprng)[ul];
    os << *prange << " ";
  }

  if (m_fIncludesNull) {
    os << "[NULL] ";
  }

  os << "}";

  return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CRange::PciRangeFromColConstCmp
//
//	@doc:
//		Creates an array of 1 or 2 ranges which represent the comparison to
//		a scalar.
//
//---------------------------------------------------------------------------
CRangeArray *CConstraintInterval::PciRangeFromColConstCmp(CMemoryPool *mp, IMDType::ECmpType cmp_type,
                                                          const CScalarConst *popsccnst) {
  GPOS_ASSERT(CScalar::EopScalarConst == popsccnst->Eopid());

  // comparison operator
  if (IMDType::EcmptOther == cmp_type) {
    return nullptr;
  }

  IDatum *datum = popsccnst->GetDatum();
  CRangeArray *pdrgprng = GPOS_NEW(mp) CRangeArray(mp);

  const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();
  if (IMDType::EcmptNEq == cmp_type || IMDType::EcmptIDF == cmp_type) {
    // need an interval with 2 ranges
    datum->AddRef();
    pdrgprng->Append(GPOS_NEW(mp) CRange(pcomp, IMDType::EcmptL, datum));
    datum->AddRef();
    pdrgprng->Append(GPOS_NEW(mp) CRange(pcomp, IMDType::EcmptG, datum));
  } else {
    datum->AddRef();
    pdrgprng->Append(GPOS_NEW(mp) CRange(pcomp, cmp_type, datum));
  }

  return pdrgprng;
}

// EOF
