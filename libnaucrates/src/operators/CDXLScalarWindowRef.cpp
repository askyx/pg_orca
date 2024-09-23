//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarWindowRef.cpp
//
//	@doc:
//		Implementation of DXL WindowRef
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarWindowRef.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::CDXLScalarWindowRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarWindowRef::CDXLScalarWindowRef(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type, bool is_distinct,
                                         bool is_star_arg, bool is_simple_agg, EdxlWinStage dxl_win_stage,
                                         uint32_t ulWinspecPosition)
    : CDXLScalar(mp),
      m_func_mdid(mdid_func),
      m_return_type_mdid(mdid_return_type),
      m_is_distinct(is_distinct),
      m_is_star_arg(is_star_arg),
      m_is_simple_agg(is_simple_agg),
      m_dxl_win_stage(dxl_win_stage),
      m_win_spec_pos(ulWinspecPosition) {
  GPOS_ASSERT(m_func_mdid->IsValid());
  GPOS_ASSERT(m_return_type_mdid->IsValid());
  GPOS_ASSERT(EdxlwinstageSentinel != m_dxl_win_stage);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::~CDXLScalarWindowRef
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarWindowRef::~CDXLScalarWindowRef() {
  m_func_mdid->Release();
  m_return_type_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarWindowRef::GetDXLOperator() const {
  return EdxlopScalarWindowRef;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::GetWindStageStr
//
//	@doc:
//		Return window stage
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarWindowRef::GetWindStageStr() const {
  GPOS_ASSERT(EdxlwinstageSentinel > m_dxl_win_stage);
  uint32_t win_stage_token_mapping[][2] = {{EdxlwinstageImmediate, EdxltokenWindowrefStageImmediate},
                                           {EdxlwinstagePreliminary, EdxltokenWindowrefStagePreliminary},
                                           {EdxlwinstageRowKey, EdxltokenWindowrefStageRowKey}};

  const uint32_t arity = GPOS_ARRAY_SIZE(win_stage_token_mapping);
  for (uint32_t ul = 0; ul < arity; ul++) {
    uint32_t *element = win_stage_token_mapping[ul];
    if ((uint32_t)m_dxl_win_stage == element[0]) {
      Edxltoken dxl_token = (Edxltoken)element[1];
      return CDXLTokens::GetDXLTokenStr(dxl_token);
      break;
    }
  }

  GPOS_ASSERT(!"Unrecognized window stage");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarWindowRef::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarWindowref);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::HasBoolResult
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
bool CDXLScalarWindowRef::HasBoolResult(CMDAccessor *md_accessor) const {
  IMDId *mdid = md_accessor->RetrieveFunc(m_func_mdid)->GetResultTypeMdid();
  return (IMDType::EtiBool == md_accessor->RetrieveType(mdid)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarWindowRef::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  EdxlWinStage edxlwinrefstage = ((CDXLScalarWindowRef *)dxlnode->GetOperator())->GetDxlWinStage();

  GPOS_ASSERT((EdxlwinstageSentinel >= edxlwinrefstage));

  const uint32_t arity = dxlnode->Arity();
  for (uint32_t ul = 0; ul < arity; ++ul) {
    CDXLNode *dxlnode_winref_arg = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypeScalar == dxlnode_winref_arg->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      dxlnode_winref_arg->GetOperator()->AssertValid(dxlnode_winref_arg, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
