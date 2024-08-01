//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CErrorContext.h
//
//	@doc:
//		Error context to record error message, stack, etc.
//---------------------------------------------------------------------------
#ifndef GPOS_CErrorContext_H
#define GPOS_CErrorContext_H

#include "gpos/common/CStackDescriptor.h"
#include "gpos/error/CException.h"
#include "gpos/error/CSerializable.h"
#include "gpos/error/IErrorContext.h"
#include "gpos/io/ioutils.h"
#include "gpos/string/CWStringStatic.h"

#define GPOS_ERROR_MESSAGE_BUFFER_SIZE (4 * 1024)

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CErrorContext
//
//	@doc:
//		Context object, owned by Task
//
//---------------------------------------------------------------------------
class CErrorContext : public IErrorContext {
 private:
  // copy of original exception
  CException m_exception;

  // exception severity
  ULONG m_severity{CException::ExsevError};

  // flag to indicate if handled yet
  BOOL m_pending{false};

  // flag to indicate if handled yet
  BOOL m_rethrown{false};

  // flag to indicate that we are currently serializing this.
  BOOL m_serializing{false};

  // error message buffer
  WCHAR m_error_msg[GPOS_ERROR_MESSAGE_BUFFER_SIZE];

  // system error message buffer
  CHAR m_system_error_msg[GPOS_ERROR_MESSAGE_BUFFER_SIZE];

  // string with static buffer allocation
  CWStringStatic m_static_buffer;

  // stack descriptor to store error stack info
  CStackDescriptor m_stack_descriptor;

  // list of objects to serialize on exception
  CList<CSerializable> m_serializable_objects_list;

 public:
  CErrorContext(const CErrorContext &) = delete;

  // ctor
  explicit CErrorContext();

  // dtor
  ~CErrorContext() override;

  // reset context, clear out handled error
  void Reset() override;

  // record error context
  void Record(CException &exc, VA_LIST) override;

  // accessors
  CException GetException() const override { return m_exception; }

  const WCHAR *GetErrorMsg() const override { return m_error_msg; }

  CStackDescriptor *GetStackDescriptor() { return &m_stack_descriptor; }

  // register object to serialize
  void Register(CSerializable *serializable_obj) { m_serializable_objects_list.Append(serializable_obj); }

  // unregister object to serialize
  void Unregister(CSerializable *serializable_obj) { m_serializable_objects_list.Remove(serializable_obj); }

  // copy necessary info for error propagation
  void CopyPropErrCtxt(const IErrorContext *perrctxt) override;

  // severity accessor
  ULONG
  GetSeverity() const override { return m_severity; }

  // set severity
  void SetSev(ULONG severity) override { m_severity = severity; }

  // print error stack trace
  void AppendStackTrace() override {
    m_static_buffer.AppendFormat(GPOS_WSZ_LIT("\nStack trace:\n"));
    m_stack_descriptor.AppendTrace(&m_static_buffer);
  }

  // print errno message
  void AppendErrnoMsg() override;

  BOOL IsPending() const override { return m_pending; }

  BOOL IsRethrown() const override { return m_rethrown; }

  void SetRethrow() override { m_rethrown = true; }

};  // class CErrorContext
}  // namespace gpos

#endif  // !GPOS_CErrorContext_H

// EOF
