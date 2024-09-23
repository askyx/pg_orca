//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatum.h
//
//	@doc:
//		Class for representing DXL datums
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatum_H
#define GPDXL_CDXLDatum_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/string/CWStringConst.h"
#include "naucrates/md/IMDId.h"
namespace gpdxl {
using namespace gpos;
using namespace gpmd;
// fwd decl

//---------------------------------------------------------------------------
//	@class:
//		CDXLDatum
//
//	@doc:
//		Class for representing DXL datums
//
//---------------------------------------------------------------------------
class CDXLDatum : public CRefCount {
 private:
 protected:
  // memory pool
  CMemoryPool *m_mp;

  // mdid of the datum's type
  IMDId *m_mdid_type;

  const int32_t m_type_modifier;

  // is the datum NULL
  bool m_is_null;

  // length
  const uint32_t m_length;

 public:
  CDXLDatum(const CDXLDatum &) = delete;

  // datum types
  enum EdxldatumType {
    EdxldatumInt2,
    EdxldatumInt4,
    EdxldatumInt8,
    EdxldatumBool,
    EdxldatumGeneric,
    EdxldatumStatsDoubleMappable,
    EdxldatumStatsLintMappable,
    EdxldatumOid,
    EdxldatumSentinel
  };
  // ctor
  CDXLDatum(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, bool is_null, uint32_t length);

  // dtor
  ~CDXLDatum() override { m_mdid_type->Release(); }

  // mdid type of the datum
  virtual IMDId *MDId() const { return m_mdid_type; }

  int32_t TypeModifier() const;

  // is datum NULL
  virtual bool IsNull() const;

  // byte array length
  virtual uint32_t Length() const;

  // ident accessors
  virtual EdxldatumType GetDatumType() const = 0;
};

// array of datums
using CDXLDatumArray = CDynamicPtrArray<CDXLDatum, CleanupRelease>;

// dynamic array of datum arrays -- array owns elements
using CDXLDatum2dArray = CDynamicPtrArray<CDXLDatumArray, CleanupRelease>;
}  // namespace gpdxl

#endif  // !GPDXL_CDXLDatum_H

// EOF
