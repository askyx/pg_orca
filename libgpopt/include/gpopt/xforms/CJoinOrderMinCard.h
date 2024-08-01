//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CJoinOrderMinCard.h
//
//	@doc:
//		Cardinality-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderMinCard_H
#define GPOPT_CJoinOrderMinCard_H

#include "gpopt/xforms/CJoinOrder.h"
#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CRefCount.h"
#include "gpos/io/IOstream.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJoinOrderMinCard
//
//	@doc:
//		Helper class for creating join orders based on cardinality of results
//
//---------------------------------------------------------------------------
class CJoinOrderMinCard : public CJoinOrder {
 private:
  // result component
  SComponent *m_pcompResult;

 public:
  // ctor
  CJoinOrderMinCard(CMemoryPool *mp, CExpressionArray *pdrgpexprComponents, CExpressionArray *pdrgpexprConjuncts);

  // dtor
  ~CJoinOrderMinCard() override;

  // main handler
  virtual CExpression *PexprExpand();

  // print function
  IOstream &OsPrint(IOstream &) const;

  CXform::EXformId EOriginXForm() const override { return CXform::ExfExpandNAryJoinMinCard; }

};  // class CJoinOrderMinCard

}  // namespace gpopt

#endif  // !GPOPT_CJoinOrderMinCard_H

// EOF
