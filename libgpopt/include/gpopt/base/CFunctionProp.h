//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CFunctionProp.h
//
//	@doc:
//		Representation of function properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CFunctionProp_H
#define GPOPT_CFunctionProp_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/IMDFunction.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CFunctionProp
//
//	@doc:
//		Representation of function properties
//
//---------------------------------------------------------------------------
class CFunctionProp : public CRefCount {
 private:
  // function stability
  IMDFunction::EFuncStbl m_efs;

  // does this expression have a volatile Function Scan
  bool m_fHasVolatileFunctionScan;

  // is this function used as a scan operator
  bool m_fScan;

 public:
  CFunctionProp(const CFunctionProp &) = delete;

  // ctor
  CFunctionProp(IMDFunction::EFuncStbl func_stability, bool fHasVolatileFunctionScan, bool fScan);

  // dtor
  ~CFunctionProp() override;

  // function stability
  IMDFunction::EFuncStbl Efs() const { return m_efs; }

  // does this expression have a volatile Function Scan
  virtual bool FHasVolatileFunctionScan() const { return m_fHasVolatileFunctionScan; }

  // check if must execute on a single host
  bool NeedsSingletonExecution() const;

  // print
  IOstream &OsPrint(IOstream &os) const;

};  // class CFunctionProp

// shorthand for printing
inline IOstream &operator<<(IOstream &os, CFunctionProp &fp) {
  return fp.OsPrint(os);
}
}  // namespace gpopt

#endif  // !GPOPT_CFunctionProp_H

// EOF
