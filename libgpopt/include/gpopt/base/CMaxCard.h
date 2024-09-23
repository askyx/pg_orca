//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CMaxCard.h
//
//	@doc:
//		Data type to carry maximal card information;
//
//		This numeric is NOT an estimate but a guaranteed upper bound
//		on the cardinality, ie., plans can be pruned based on this information
//---------------------------------------------------------------------------
#ifndef GPOPT_CMaxCard_H
#define GPOPT_CMaxCard_H

#include "gpos/base.h"

// use UINT32_MAX instead of UINT64_MAX to make sure arithmetic does not cause overflows
#define GPOPT_MAX_CARD ((uint64_t)(UINT32_MAX))

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMaxCard
//
//	@doc:
//		Maximum Cardinality, including basic operators
//
//---------------------------------------------------------------------------
class CMaxCard {
  // friends that enforce 'literal == variable' order
  friend bool operator==(uint64_t, const CMaxCard &);
  friend bool operator==(const CMaxCard &, const CMaxCard &);

 private:
  // actual (cropped) value
  uint64_t m_ull;

  // equality
  bool operator==(uint64_t ull) const {
    GPOS_ASSERT(ull <= GPOPT_MAX_CARD);
    return m_ull == ull;
  }

 public:
  // ctor
  explicit CMaxCard(uint64_t ull = GPOPT_MAX_CARD) : m_ull(ull) {
    GPOS_ASSERT(m_ull <= GPOPT_MAX_CARD);
    m_ull = std::min(m_ull, GPOPT_MAX_CARD);
  }

  // copy ctor
  CMaxCard(const CMaxCard &mc) { *this = mc; }

  // accessor
  uint64_t Ull() const { return m_ull; }

  // assignment
  void operator=(const CMaxCard &mc) { m_ull = mc.m_ull; }

  // arithmetic operators
  void operator*=(const CMaxCard &mc) { m_ull = std::min(mc.m_ull * m_ull, GPOPT_MAX_CARD); }

  // arithmetic operators
  void operator+=(const CMaxCard &mc) { m_ull = std::min(mc.m_ull + m_ull, GPOPT_MAX_CARD); }

  // print
  IOstream &OsPrint(IOstream &os) const {
    if (GPOPT_MAX_CARD == m_ull) {
      return os << "unbounded";
    }

    return os << m_ull;
  }
};  // class CMaxCard

// shorthand for printing
inline IOstream &operator<<(IOstream &os, const CMaxCard &mc) {
  return mc.OsPrint(os);
}

// shorthand for less-than equal
inline bool operator<=(const CMaxCard &mcLHS, const CMaxCard &mcRHS) {
  if (mcLHS.Ull() <= mcRHS.Ull()) {
    return true;
  }

  return false;
}

// shorthand for equality
inline bool operator==(const CMaxCard &mcLHS, const CMaxCard &mcRHS) {
  return mcLHS.operator==(mcRHS.Ull());
}

// shorthand for equality
inline bool operator==(uint64_t ull, const CMaxCard &mc) {
  return mc.operator==(ull);
}

}  // namespace gpopt

#endif  // !GPOPT_CMaxCard_H

// EOF
