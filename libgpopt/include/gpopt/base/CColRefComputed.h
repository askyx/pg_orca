//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CColRefComputed.h
//
//	@doc:
//		Column reference implementation for computed columns
//---------------------------------------------------------------------------
#ifndef GPOS_CColRefComputed_H
#define GPOS_CColRefComputed_H

#include "gpopt/base/CColRef.h"
#include "gpopt/metadata/CName.h"
#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CList.h"
#include "naucrates/md/IMDType.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CColRefComputed
//
//	@doc:
//
//---------------------------------------------------------------------------
class CColRefComputed : public CColRef {
 private:
 public:
  CColRefComputed(const CColRefComputed &) = delete;

  // ctor
  CColRefComputed(const IMDType *pmdtype, int32_t type_modifier, uint32_t id, const CName *pname);

  // dtor
  ~CColRefComputed() override;

  CColRef::Ecolreftype Ecrt() const override { return CColRef::EcrtComputed; }

  // is column a system column?
  bool IsSystemCol() const override {
    // we cannot introduce system columns as computed column
    return false;
  }

  // is column a distribution column?
  bool IsDistCol() const override {
    // we cannot introduce distribution columns as computed column
    return false;
  };

  // is column a partition column?
  bool IsPartCol() const override {
    // we cannot introduce partition columns as computed column
    return false;
  };

};  // class CColRefComputed

}  // namespace gpopt

#endif  // !GPOS_CColRefComputed_H

// EOF
