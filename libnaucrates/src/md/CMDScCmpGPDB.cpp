//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDScCmpGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific comparisons
//		in the MD cache
//---------------------------------------------------------------------------

#include "naucrates/md/CMDScCmpGPDB.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::CMDScCmpGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDScCmpGPDB::CMDScCmpGPDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, IMDId *left_mdid, IMDId *right_mdid,
                           IMDType::ECmpType cmp_type, IMDId *mdid_op)
    : m_mp(mp),
      m_mdid(mdid),
      m_mdname(mdname),
      m_mdid_left(left_mdid),
      m_mdid_right(right_mdid),
      m_comparision_type(cmp_type),
      m_mdid_op(mdid_op) {
  GPOS_ASSERT(m_mdid->IsValid());
  GPOS_ASSERT(m_mdid_left->IsValid());
  GPOS_ASSERT(m_mdid_right->IsValid());
  GPOS_ASSERT(m_mdid_op->IsValid());
  GPOS_ASSERT(IMDType::EcmptOther != m_comparision_type);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::~CMDScCmpGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDScCmpGPDB::~CMDScCmpGPDB() {
  m_mdid->Release();
  m_mdid_left->Release();
  m_mdid_right->Release();
  m_mdid_op->Release();
  GPOS_DELETE(m_mdname);
  if (nullptr != m_dxl_str) {
    GPOS_DELETE(m_dxl_str);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::MDId
//
//	@doc:
//		Mdid of comparison object
//
//---------------------------------------------------------------------------
IMDId *CMDScCmpGPDB::MDId() const {
  return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::Mdname
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
CMDName CMDScCmpGPDB::Mdname() const {
  return *m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::GetLeftMdid
//
//	@doc:
//		Left type id
//
//---------------------------------------------------------------------------
IMDId *CMDScCmpGPDB::GetLeftMdid() const {
  return m_mdid_left;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::GetRightMdid
//
//	@doc:
//		Destination type id
//
//---------------------------------------------------------------------------
IMDId *CMDScCmpGPDB::GetRightMdid() const {
  return m_mdid_right;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::MdIdOp
//
//	@doc:
//		Cast function id
//
//---------------------------------------------------------------------------
IMDId *CMDScCmpGPDB::MdIdOp() const {
  return m_mdid_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::IsBinaryCoercible
//
//	@doc:
//		Returns whether this is a cast between binary coercible types, i.e. the
//		types are binary compatible
//
//---------------------------------------------------------------------------
IMDType::ECmpType CMDScCmpGPDB::ParseCmpType() const {
  return m_comparision_type;
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void CMDScCmpGPDB::DebugPrint(IOstream &os) const {
  os << "ComparisonOp ";
  GetLeftMdid()->OsPrint(os);
  os << (Mdname()).GetMDName()->GetBuffer() << "(";
  MdIdOp()->OsPrint(os);
  os << ") ";
  GetLeftMdid()->OsPrint(os);

  os << ", type: " << IMDType::GetCmpTypeStr(m_comparision_type);

  os << std::endl;
}

#endif  // GPOS_DEBUG

// EOF
