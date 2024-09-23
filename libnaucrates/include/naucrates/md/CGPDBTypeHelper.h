//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CGPDBTypeHelper.h
//
//	@doc:
//		Helper class that provides implementation for common functions across
//		different GPDB types (CMDTypeInt4GPDB, CMDTypeBoolGPDB, and CMDTypeGenericGPDB)
//---------------------------------------------------------------------------
#ifndef GPMD_CGPDBHelper_H
#define GPMD_CGPDBHelper_H

#include "gpos/base.h"
#include "naucrates/dxl/CDXLUtils.h"

namespace gpmd {
using namespace gpos;
using namespace gpdxl;

template <class T>
class CGPDBTypeHelper {
 public:
#ifdef GPOS_DEBUG
  // debug print of the type in the provided stream
  static void DebugPrint(IOstream &os, const T *mdtype) {
    os << "Type id: ";
    mdtype->MDId()->OsPrint(os);
    os << std::endl;

    os << "Type name: " << mdtype->Mdname().GetMDName()->GetBuffer() << std::endl;

    const CWStringConst *fixed_len_str = mdtype->IsFixedLength() ? CDXLTokens::GetDXLTokenStr(EdxltokenTrue)
                                                                 : CDXLTokens::GetDXLTokenStr(EdxltokenFalse);

    os << "Fixed length: " << fixed_len_str->GetBuffer() << std::endl;

    if (mdtype->IsFixedLength()) {
      os << "Type length: " << mdtype->Length() << std::endl;
    }

    const CWStringConst *passed_by_val_str = mdtype->IsPassedByValue() ? CDXLTokens::GetDXLTokenStr(EdxltokenTrue)
                                                                       : CDXLTokens::GetDXLTokenStr(EdxltokenFalse);

    os << "Pass by value: " << passed_by_val_str->GetBuffer() << std::endl;

    os << "Equality operator id: ";
    mdtype->GetMdidForCmpType(IMDType::EcmptEq)->OsPrint(os);
    os << std::endl;

    os << "Less-than operator id: ";
    mdtype->GetMdidForCmpType(IMDType::EcmptL)->OsPrint(os);
    os << std::endl;

    os << "Greater-than operator id: ";
    mdtype->GetMdidForCmpType(IMDType::EcmptG)->OsPrint(os);
    os << std::endl;

    os << "Comparison operator id: ";
    mdtype->CmpOpMdid()->OsPrint(os);
    os << std::endl;

    os << std::endl;
  }
#endif  // GPOS_DEBUG
};
}  // namespace gpmd

#endif  // !CGPMD_GPDBHelper_H

// EOF
