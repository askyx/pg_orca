//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDXLNode.h
//
//	@doc:
//		Basic tree/DAG-based representation for DXL tree nodes
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLNode_H
#define GPDXL_CDXLNode_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLProperties.h"
#include "naucrates/dxl/xml/dxltokens.h"
namespace gpdxl {
using namespace gpos;

// fwd decl
class CDXLNode;
class CDXLOperator;

using CDXLNodeArray = CDynamicPtrArray<CDXLNode, CleanupRelease>;

// arrays of OID
using OidArray = CDynamicPtrArray<OID, CleanupDelete>;

using IdToCDXLNodeMap = CHashMap<uint32_t, CDXLNode, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                 CleanupDelete<uint32_t>, CleanupRelease<CDXLNode>>;

//---------------------------------------------------------------------------
//	@class:
//		CDXLNode
//
//	@doc:
//		Class for representing nodes in a DXL tree.
//		Each node specifies an operator at that node, and has an array of children nodes.
//
//---------------------------------------------------------------------------
class CDXLNode : public CRefCount {
 private:
  // dxl tree operator class
  CDXLOperator *m_dxl_op;

  // properties of the operator
  CDXLProperties *m_dxl_properties;

  // array of children
  CDXLNodeArray *m_dxl_array;

 public:
  CDXLNode(const CDXLNode &) = delete;

  // ctors

  explicit CDXLNode(CMemoryPool *mp);
  CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op);
  CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op, CDXLNode *child_dxlnode);
  CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op, CDXLNode *first_child_dxlnode, CDXLNode *second_child_dxlnode);
  CDXLNode(CMemoryPool *mp, CDXLOperator *dxl_op, CDXLNode *first_child_dxlnode, CDXLNode *second_child_dxlnode,
           CDXLNode *third_child_dxlnode);
  CDXLNode(CDXLOperator *dxl_op, CDXLNodeArray *dxl_array);

  // dtor
  ~CDXLNode() override;

  // shorthand to access children
  inline CDXLNode *operator[](uint32_t idx) const {
    GPOS_ASSERT(nullptr != m_dxl_array);
    CDXLNode *dxl_node = (*m_dxl_array)[idx];
    GPOS_ASSERT(nullptr != dxl_node);
    return dxl_node;
  };

  // arity function, returns the number of children this node has
  inline uint32_t Arity() const { return (m_dxl_array == nullptr) ? 0 : m_dxl_array->Size(); }

  // accessor for operator
  inline CDXLOperator *GetOperator() const { return m_dxl_op; }

  // return properties
  CDXLProperties *GetProperties() const { return m_dxl_properties; }

  // accessor for children nodes
  const CDXLNodeArray *GetChildDXLNodeArray() const { return m_dxl_array; }

  // setters
  void AddChild(CDXLNode *child_dxlnode);

  void SetOperator(CDXLOperator *dxl_op);

  // replace a given child of this DXL node with the given node
  void ReplaceChild(uint32_t idx, CDXLNode *child_dxlnode);

  // setter
  void SetProperties(CDXLProperties *dxl_properties);

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(bool validate_children) const;
#endif  // GPOS_DEBUG

};  // class CDXLNode
}  // namespace gpdxl

#endif  // !GPDXL_CDXLNode_H

// EOF
