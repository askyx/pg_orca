//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdProp.cpp
//
//	@doc:
//		Implementation of derived properties
//---------------------------------------------------------------------------

#include "gpopt/base/CDrvdProp.h"

#include "gpopt/operators/COperator.h"
#include "gpos/base.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif  // GPOS_DEBUG

namespace gpopt {
CDrvdProp::CDrvdProp() = default;

IOstream &operator<<(IOstream &os, const CDrvdProp &drvdprop) {
  return drvdprop.OsPrint(os);
}

}  // namespace gpopt

// EOF
