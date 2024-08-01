//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2023 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CHintUtils.h
//---------------------------------------------------------------------------
#ifndef GPOS_CHintUtils_H
#define GPOS_CHintUtils_H

#include "gpopt/hints/CPlanHint.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalIndexGet.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CScalarBitmapIndexProbe.h"
#include "gpos/base.h"

namespace gpopt {
class CHintUtils {
 public:
  // Check if CLogicalGet operator satisfies plan hints
  static BOOL SatisfiesPlanHints(CLogicalGet *pop, CPlanHint *plan_hint);

  // Check if CLogicalIndexGet operator satisfies plan hints
  static BOOL SatisfiesPlanHints(CLogicalIndexGet *pop, CPlanHint *plan_hint);

  // Check if CScalarBitmapIndexProbe operator satisfies plan hints
  static BOOL SatisfiesPlanHints(CScalarBitmapIndexProbe *pop, CPlanHint *plan_hint);

  // Check if CExpression satisfies join type hints
  static BOOL SatisfiesJoinTypeHints(CMemoryPool *mp, CExpression *pexpr, CPlanHint *plan_hint);

  static const WCHAR *ScanHintEnumToString(CScanHint::EType type);

  static CScanHint::EType ScanHintStringToEnum(const WCHAR *type);

  static const WCHAR *JoinTypeHintEnumToString(CJoinTypeHint::JoinType type);

  static CJoinTypeHint::JoinType JoinTypeHintStringToEnum(const WCHAR *type);

  // Get set of aliases from table descriptor set
  static StringPtrArray *GetAliasesFromTableDescriptors(CMemoryPool *mp, CTableDescriptorHashSet *ptabs);

  // Get set of aliases from join pair
  static StringPtrArray *GetAliasesFromHint(CMemoryPool *mp, const CJoinHint::JoinNode *joinnode);
};

}  // namespace gpopt

#endif  // !GPOS_CHintUtils_H

// EOF
