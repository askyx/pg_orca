//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//---------------------------------------------------------------------------
#ifndef GPDB_RelationWrapper_H
#define GPDB_RelationWrapper_H

#include <cstddef>

using Relation = struct RelationData *;

namespace gpdb {

class RelationWrapper {
 public:
  RelationWrapper(RelationWrapper const &) = delete;
  RelationWrapper(RelationWrapper &&r) : m_relation(r.m_relation) { r.m_relation = nullptr; };

  explicit RelationWrapper(Relation relation) : m_relation(relation) {}

  /// allows use in typical conditionals of the form
  ///
  /// \code if (rel) { do_stuff(rel); } \endcode or
  /// \code if (!rel) return; \endcode
  explicit operator bool() const { return m_relation != nullptr; }

  // behave like a raw pointer on arrow
  Relation operator->() const { return m_relation; }

  // get the raw pointer, behaves like std::unique_ptr::get()
  Relation get() const { return m_relation; }

  /// Explicitly close the underlying relation early. This is not usually
  /// necessary unless there is significant amount of time between the point
  /// of close and the end-of-scope
  void Close();

  ~RelationWrapper() noexcept(false);

 private:
  Relation m_relation = nullptr;
};
}  // namespace gpdb
#endif  // GPDB_RelationWrapper_H
