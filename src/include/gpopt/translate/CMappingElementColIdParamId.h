#pragma once

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl {
using namespace gpos;
using namespace gpmd;

struct CMappingElementColIdParamId {
  // column identifier that is used as the key
  uint32_t m_colid;

  // param identifier
  uint32_t m_paramid;

  // param type
  IMDId *m_mdid;

  int32_t m_type_modifier;

  CMappingElementColIdParamId(uint32_t colid, uint32_t paramid, IMDId *mdid, int32_t type_modifier)
      : m_colid(colid), m_paramid(paramid), m_mdid(mdid), m_type_modifier(type_modifier) {}

  auto *Copy() const { return new CMappingElementColIdParamId(m_colid, m_paramid, m_mdid, m_type_modifier); }
};
}  // namespace gpdxl
