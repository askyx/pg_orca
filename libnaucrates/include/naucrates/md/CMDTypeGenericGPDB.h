//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDTypeGenericGPDB.h
//
//	@doc:
//		Class representing GPDB generic types
//---------------------------------------------------------------------------

#ifndef GPMD_CMDTypeGenericGPDB_H
#define GPMD_CMDTypeGenericGPDB_H

#include "gpos/base.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/md/CGPDBTypeHelper.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDTypeGeneric.h"

// some metadata ids for types that don't have their specific header files (yet)
// keep this in sync with Postgres file pg_operator.h
#define GPDB_TEXT_EQ_OP OID(98)
#define GPDB_TEXT_NEQ_OP OID(531)
#define GPDB_TEXT_LT_OP OID(664)
#define GPDB_TEXT_LEQ_OP OID(665)
#define GPDB_TEXT_GT_OP OID(666)
#define GPDB_TEXT_GEQ_OP OID(667)

// fwd decl
namespace gpdxl {}

namespace gpmd {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMDTypeGenericGPDB
//
//	@doc:
//		Class representing GPDB generic types
//
//---------------------------------------------------------------------------
class CMDTypeGenericGPDB : public IMDTypeGeneric {
  friend class CGPDBTypeHelper<CMDTypeGenericGPDB>;

 private:
  // memory pool
  CMemoryPool *m_mp;

  // DXL for object
  const CWStringDynamic *m_dxl_str = nullptr;

  // metadata id
  IMDId *m_mdid;

  // type name
  CMDName *m_mdname;

  // is this a fixed-length type
  bool m_is_fixed_length;

  // FIXME: we seem to only use m_gpdb_length here, why?
#ifdef GPOS_DEBUG
  // type length in number of bytes for fixed-length types, 0 otherwise
  uint32_t m_length;
#endif

  // is type passed by value or by reference
  bool m_is_passed_by_value;

  IMDId *m_part_opfamily;

  // id of equality operator for type
  IMDId *m_mdid_op_eq;

  // id of inequality operator for type
  IMDId *m_mdid_op_neq;

  // id of less than operator for type
  IMDId *m_mdid_op_lt;

  // id of less than equals operator for type
  IMDId *m_mdid_op_leq;

  // id of greater than operator for type
  IMDId *m_mdid_op_gt;

  // id of greater than equals operator for type
  IMDId *m_mdid_op_geq;

  // id of comparison operator for type used in btree lookups
  IMDId *m_mdid_op_cmp;

  // min aggregate
  IMDId *m_mdid_min;

  // max aggregate
  IMDId *m_mdid_max;

  // avg aggregate
  IMDId *m_mdid_avg;

  // sum aggregate
  IMDId *m_mdid_sum;

  // count aggregate
  IMDId *m_mdid_count;

  // is type hashable
  bool m_is_hashable;

  // is type merge joinable using '='
  bool m_is_merge_joinable;

  // is type composite
  bool m_is_composite_type;

  // is type text related
  bool m_is_text_related;

  // id of the relation corresponding to a composite type
  IMDId *m_mdid_base_relation;

  // id of array type for type
  IMDId *m_mdid_type_array;

  // GPDB specific length
  int32_t m_gpdb_length;

  // a null datum of this type (used for statistics comparison)
  IDatum *m_datum_null;

 public:
  CMDTypeGenericGPDB(const CMDTypeGenericGPDB &) = delete;

  // ctor
  CMDTypeGenericGPDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, bool is_fixed_length, uint32_t length,
                     bool is_passed_by_value, IMDId *mdid_part_opfamily, IMDId *mdid_op_eq, IMDId *mdid_op_neq,
                     IMDId *mdid_op_lt, IMDId *mdid_op_leq, IMDId *mdid_op_gt, IMDId *mdid_op_geq, IMDId *mdid_op_cmp,
                     IMDId *pmdidMin, IMDId *pmdidMax, IMDId *pmdidAvg, IMDId *pmdidSum, IMDId *pmdidCount,
                     bool is_hashable, bool is_merge_joinable, bool is_composite_type, bool is_text_related,
                     IMDId *mdid_base_relation, IMDId *mdid_type_array, int32_t gpdb_length);

  // dtor
  ~CMDTypeGenericGPDB() override;

  // accessors

  IMDId *MDId() const override;

  CMDName Mdname() const override;

  bool IsFixedLength() const override { return m_is_fixed_length; }

  // is type composite
  bool IsComposite() const override { return m_is_composite_type; }

  uint32_t Length() const override { return m_gpdb_length; }

  bool IsPassedByValue() const override { return m_is_passed_by_value; }

  // id of specified comparison operator type
  IMDId *GetMdidForCmpType(ECmpType ecmpt) const override;

  // id of specified specified aggregate type
  IMDId *GetMdidForAggType(EAggType agg_type) const override;

  const IMDId *CmpOpMdid() const override { return m_mdid_op_cmp; }

  // is type hashable
  bool IsHashable() const override { return m_is_hashable; }

  bool IsTextRelated() const override { return m_is_text_related; }

  // is type merge joinable on '='
  bool IsMergeJoinable() const override { return m_is_merge_joinable; }

  // id of the relation corresponding to a composite type
  IMDId *GetBaseRelMdid() const override { return m_mdid_base_relation; }

  IMDId *GetArrayTypeMdid() const override { return m_mdid_type_array; }

  IMDId *GetPartOpfamilyMdid() const override;

  // serialize object in DXL format

  // factory method for generating generic datum from CDXLScalarConstValue
  IDatum *GetDatumForDXLConstVal(const CDXLScalarConstValue *dxl_op) const override;

  // create typed datum from DXL datum
  IDatum *GetDatumForDXLDatum(CMemoryPool *mp, const CDXLDatum *dxl_datum) const override;

  // return the GPDB length
  virtual int32_t GetGPDBLength() const { return m_gpdb_length; }

  // return the null constant for this type
  IDatum *DatumNull() const override { return m_datum_null; }

  // generate the DXL datum from IDatum
  CDXLDatum *GetDatumVal(CMemoryPool *mp, IDatum *datum) const override;

  // generate the DXL datum representing null value
  CDXLDatum *GetDXLDatumNull(CMemoryPool *mp) const override;

  // generate the DXL scalar constant from IDatum
  CDXLScalarConstValue *GetDXLOpScConst(CMemoryPool *mp, IDatum *datum) const override;

#ifdef GPOS_DEBUG
  // debug print of the type in the provided stream
  void DebugPrint(IOstream &os) const override;
#endif

  // is type an ambiguous one? e.g., AnyElement in GPDB
  bool IsAmbiguous() const override;

  // create a dxl datum
  static CDXLDatum *CreateDXLDatumVal(CMemoryPool *mp, IMDId *mdid, const IMDType *md_type, int32_t type_modifier,
                                      bool is_null, uint8_t *byte_array, uint32_t length, int64_t lint_Value,
                                      CDouble double_Value);

  // create a dxl datum of types having double mapping
  static CDXLDatum *CreateDXLDatumStatsDoubleMappable(CMemoryPool *mp, IMDId *mdid, int32_t type_modifier, bool is_null,
                                                      uint8_t *byte_array, uint32_t length, int64_t lint_Value,
                                                      CDouble double_Value);

  // create a dxl datum of types having lint mapping
  static CDXLDatum *CreateDXLDatumStatsIntMappable(CMemoryPool *mp, IMDId *mdid, int32_t type_modifier, bool is_null,
                                                   uint8_t *byte_array, uint32_t length, int64_t lint_Value,
                                                   CDouble double_Value);

  // create a NULL constant for this type
  IDatum *CreateGenericNullDatum(CMemoryPool *mp, int32_t type_modifier) const override;

  // does a datum of this type need bytea to Lint mapping for statistics computation
  static bool HasByte2IntMapping(const IMDType *mdtype);

  // does a datum of this type need bytea to double mapping for statistics computation
  static bool HasByte2DoubleMapping(const IMDId *mdid);

  // is this a time-related type
  static bool IsTimeRelatedType(const IMDId *mdid);

  // is this a network-related type
  static bool IsNetworkRelatedType(const IMDId *mdid);
};
}  // namespace gpmd

#endif  // !GPMD_CMDTypeGenericGPDB_H

// EOF
