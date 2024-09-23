//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC CORP.
//
//	@filename:
//		CFunctionalDependency.h
//
//	@doc:
//		Functional dependency representation
//---------------------------------------------------------------------------
#ifndef GPOPT_CFunctionalDependency_H
#define GPOPT_CFunctionalDependency_H

#include "gpopt/base/CColRefSet.h"
#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

namespace gpopt {
// fwd declarations
class CFunctionalDependency;

// definition of array of functional dependencies
using CFunctionalDependencyArray = CDynamicPtrArray<CFunctionalDependency, CleanupRelease>;

using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CFunctionalDependency
//
//	@doc:
//		Functional dependency representation
//
//---------------------------------------------------------------------------
class CFunctionalDependency : public CRefCount {
 private:
  // the left hand side of the FD
  CColRefSet *m_pcrsKey;

  // the right hand side of the FD
  CColRefSet *m_pcrsDetermined;

 public:
  CFunctionalDependency(const CFunctionalDependency &) = delete;

  // ctor
  CFunctionalDependency(CColRefSet *pcrsKey, CColRefSet *pcrsDetermined);

  // dtor
  ~CFunctionalDependency() override;

  // key set accessor
  CColRefSet *PcrsKey() const { return m_pcrsKey; }

  // determined set accessor
  CColRefSet *PcrsDetermined() const { return m_pcrsDetermined; }

  // determine if all FD columns are included in the given column set
  bool FIncluded(CColRefSet *pcrs) const;

  // hash function
  virtual uint32_t HashValue() const;

  // equality function
  bool Equals(const CFunctionalDependency *pfd) const;

  // do the given arguments form a functional dependency
  bool FFunctionallyDependent(CColRefSet *pcrsKey, CColRef *colref) {
    GPOS_ASSERT(nullptr != pcrsKey);
    GPOS_ASSERT(nullptr != colref);

    return m_pcrsKey->Equals(pcrsKey) && m_pcrsDetermined->FMember(colref);
  }

  // print
  IOstream &OsPrint(IOstream &os) const;

  // hash function
  static uint32_t HashValue(const CFunctionalDependencyArray *pdrgpfd);

  // equality function
  static bool Equals(const CFunctionalDependencyArray *pdrgpfdFst, const CFunctionalDependencyArray *pdrgpfdSnd);

  // create a set of all keys in the passed FD's array
  static CColRefSet *PcrsKeys(CMemoryPool *mp, const CFunctionalDependencyArray *pdrgpfd);

  // create an array of all keys in the passed FD's array
  static CColRefArray *PdrgpcrKeys(CMemoryPool *mp, const CFunctionalDependencyArray *pdrgpfd);

};  // class CFunctionalDependency

// shorthand for printing
inline IOstream &operator<<(IOstream &os, CFunctionalDependency &fd) {
  return fd.OsPrint(os);
}

}  // namespace gpopt

#endif  // !GPOPT_CFunctionalDependency_H

// EOF
