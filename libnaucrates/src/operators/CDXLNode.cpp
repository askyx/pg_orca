//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLNode.cpp
//
//	@doc:
//		Implementation of DXL nodes
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/operators/CDXLOperator.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Constructs a DXL node with unspecified operator
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode(CMemoryPool *mp) : m_dxl_op(nullptr), m_dxl_properties(nullptr), m_direct_dispatch_info(nullptr) {
  m_dxl_array = GPOS_NEW(mp) CDXLNodeArray(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Constructs a DXL node with given operator
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op)
    : m_dxl_op(dxl_op), m_dxl_properties(nullptr), m_direct_dispatch_info(nullptr) {
  GPOS_ASSERT(nullptr != dxl_op);
  m_dxl_array = GPOS_NEW(mp) CDXLNodeArray(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op, CDXLNode *child_dxlnode)
    : m_dxl_op(dxl_op), m_dxl_properties(nullptr), m_dxl_array(nullptr), m_direct_dispatch_info(nullptr) {
  GPOS_ASSERT(nullptr != dxl_op);
  GPOS_ASSERT(nullptr != child_dxlnode);

  m_dxl_array = GPOS_NEW(mp) CDXLNodeArray(mp);
  m_dxl_array->Append(child_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op, CDXLNode *first_child_dxlnode, CDXLNode *second_child_dxlnode)
    : m_dxl_op(dxl_op), m_dxl_properties(nullptr), m_dxl_array(nullptr), m_direct_dispatch_info(nullptr) {
  GPOS_ASSERT(nullptr != dxl_op);
  GPOS_ASSERT(nullptr != first_child_dxlnode);
  GPOS_ASSERT(nullptr != second_child_dxlnode);

  m_dxl_array = GPOS_NEW(mp) CDXLNodeArray(mp);
  m_dxl_array->Append(first_child_dxlnode);
  m_dxl_array->Append(second_child_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op, CDXLNode *first_child_dxlnode, CDXLNode *second_child_dxlnode,
                   CDXLNode *third_child_dxlnode)
    : m_dxl_op(dxl_op), m_dxl_properties(nullptr), m_dxl_array(nullptr), m_direct_dispatch_info(nullptr) {
  GPOS_ASSERT(nullptr != dxl_op);
  GPOS_ASSERT(nullptr != first_child_dxlnode);
  GPOS_ASSERT(nullptr != second_child_dxlnode);
  GPOS_ASSERT(nullptr != third_child_dxlnode);

  m_dxl_array = GPOS_NEW(mp) CDXLNodeArray(mp);
  m_dxl_array->Append(first_child_dxlnode);
  m_dxl_array->Append(second_child_dxlnode);
  m_dxl_array->Append(third_child_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode(CDXLOperator *dxl_op, CDXLNodeArray *dxl_array)
    : m_dxl_op(dxl_op), m_dxl_properties(nullptr), m_dxl_array(dxl_array), m_direct_dispatch_info(nullptr) {
  GPOS_ASSERT(nullptr != dxl_op);
  GPOS_ASSERT(nullptr != dxl_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::~CDXLNode
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLNode::~CDXLNode() {
  m_dxl_array->Release();
  CRefCount::SafeRelease(m_dxl_op);
  CRefCount::SafeRelease(m_dxl_properties);
  CRefCount::SafeRelease(m_direct_dispatch_info);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::AddChild
//
//	@doc:
//		Adds a child to the DXL node's list of children
//
//---------------------------------------------------------------------------
void CDXLNode::AddChild(CDXLNode *child_dxlnode) {
  GPOS_ASSERT(nullptr != m_dxl_array);
  GPOS_ASSERT(nullptr != child_dxlnode);

  m_dxl_array->Append(child_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::ReplaceChild
//
//	@doc:
//		Replaces a child of the DXL node with a new one
//
//---------------------------------------------------------------------------
void CDXLNode::ReplaceChild(ULONG pos, CDXLNode *child_dxlnode) {
  GPOS_ASSERT(nullptr != m_dxl_array);
  GPOS_ASSERT(nullptr != child_dxlnode);

  m_dxl_array->Replace(pos, child_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SetOperator
//
//	@doc:
//		Sets the operator at that DXL node
//
//---------------------------------------------------------------------------
void CDXLNode::SetOperator(CDXLOperator *dxl_op) {
  GPOS_ASSERT(nullptr == m_dxl_op);
  m_dxl_op = dxl_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SetProperties
//
//	@doc:
//		Set operator properties
//
//---------------------------------------------------------------------------
void CDXLNode::SetProperties(CDXLProperties *dxl_properties) {
  // allow setting properties only once
  GPOS_ASSERT(nullptr == m_dxl_properties);
  m_dxl_properties = dxl_properties;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::AssertValid
//
//	@doc:
//		Checks whether node is well-structured
//
//---------------------------------------------------------------------------
void CDXLNode::AssertValid(BOOL validate_children) const {
  if (!validate_children) {
    return;
  }

  const ULONG arity = Arity();
  for (ULONG idx = 0; idx < arity; idx++) {
    CDXLNode *child_dxlnode = (*this)[idx];
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}
#endif  // GPOS_DEBUG

// EOF
