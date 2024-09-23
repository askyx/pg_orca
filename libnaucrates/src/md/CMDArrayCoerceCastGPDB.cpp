//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDArrayCoerceCastGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific array coerce
//		casts in the MD cache
//---------------------------------------------------------------------------

#include "naucrates/md/CMDArrayCoerceCastGPDB.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

// ctor
CMDArrayCoerceCastGPDB::CMDArrayCoerceCastGPDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, IMDId *mdid_src,
                                               IMDId *mdid_dest, bool is_binary_coercible, IMDId *mdid_cast_func,
                                               EmdCoercepathType path_type, int32_t type_modifier, bool is_explicit,
                                               EdxlCoercionForm dxl_coerce_format, int32_t location,
                                               IMDId *mdid_src_elemtype)
    : CMDCastGPDB(mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible, mdid_cast_func, path_type),
      m_type_modifier(type_modifier),
      m_is_explicit(is_explicit),
      m_dxl_coerce_format(dxl_coerce_format),
      m_location(location),
      m_mdid_src_elemtype(mdid_src_elemtype) {}

// dtor
CMDArrayCoerceCastGPDB::~CMDArrayCoerceCastGPDB() {
  if (nullptr != m_dxl_str) {
    GPOS_DELETE(m_dxl_str);
  }
  m_mdid_src_elemtype->Release();
}

// return type modifier
int32_t CMDArrayCoerceCastGPDB::TypeModifier() const {
  return m_type_modifier;
}

// return is explicit cast
bool CMDArrayCoerceCastGPDB::IsExplicit() const {
  return m_is_explicit;
}

// return coercion form
EdxlCoercionForm CMDArrayCoerceCastGPDB::GetCoercionForm() const {
  return m_dxl_coerce_format;
}

// return token location
int32_t CMDArrayCoerceCastGPDB::Location() const {
  return m_location;
}

// return src basetype mdid
IMDId *CMDArrayCoerceCastGPDB::GetSrcElemTypeMdId() const {
  return m_mdid_src_elemtype;
}

#ifdef GPOS_DEBUG

// prints a metadata cache relation to the provided output
void CMDArrayCoerceCastGPDB::DebugPrint(IOstream &os) const {
  CMDCastGPDB::DebugPrint(os);
  os << ", Result Type Mod: ";
  os << m_type_modifier;
  os << ", isExplicit: ";
  os << m_is_explicit;
  os << ", coercion form: ";
  os << m_dxl_coerce_format;

  os << std::endl;
}

#endif  // GPOS_DEBUG

// EOF
