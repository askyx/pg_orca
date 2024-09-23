//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLRelStats.h
//
//	@doc:
//		Class representing relation stats
//---------------------------------------------------------------------------

#ifndef GPMD_CDXLRelStats_H
#define GPMD_CDXLRelStats_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/IMDRelStats.h"

namespace gpdxl {}

namespace gpmd {
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@class:
//		CDXLRelStats
//
//	@doc:
//		Class representing relation stats
//
//---------------------------------------------------------------------------
class CDXLRelStats : public IMDRelStats {
 private:
  // memory pool
  CMemoryPool *m_mp;

  // metadata id of the object
  CMDIdRelStats *m_rel_stats_mdid;

  // table name
  CMDName *m_mdname;

  // number of rows
  CDouble m_rows;

  // flag to indicate if input relation is empty
  bool m_empty;

  // DXL string for object
  CWStringDynamic *m_dxl_str = nullptr;

  // number of blocks (not always up to-to-date)
  uint32_t m_relpages;

  // number of all-visible blocks (not always up-to-date)
  uint32_t m_relallvisible;

 public:
  CDXLRelStats(const CDXLRelStats &) = delete;

  CDXLRelStats(CMemoryPool *mp, CMDIdRelStats *rel_stats_mdid, CMDName *mdname, CDouble rows, bool is_empty,
               uint32_t relpages, uint32_t relallvisible);

  ~CDXLRelStats() override;

  // the metadata id
  IMDId *MDId() const override;

  // relation name
  CMDName Mdname() const override;

  // DXL string representation of cache object

  // number of rows
  CDouble Rows() const override;

  // number of blocks (not always up to-to-date)
  uint32_t RelPages() const override { return m_relpages; }

  // number of all-visible blocks (not always up-to-date)
  uint32_t RelAllVisible() const override { return m_relallvisible; }

  // is statistics on an empty input
  bool IsEmpty() const override { return m_empty; }

  // serialize relation stats in DXL format given a serializer object

#ifdef GPOS_DEBUG
  // debug print of the metadata relation
  void DebugPrint(IOstream &os) const override;
#endif

  // dummy relstats
  static CDXLRelStats *CreateDXLDummyRelStats(CMemoryPool *mp, IMDId *mdid);
};

}  // namespace gpmd

#endif  // !GPMD_CDXLRelStats_H

// EOF
