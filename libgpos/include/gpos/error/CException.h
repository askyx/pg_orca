//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CException.h
//
//	@doc:
//		Implements basic exception handling. All excpetion handling related
//		functionality is wrapped in macros to facilitate easy modifications
//		at a later point in time.
//---------------------------------------------------------------------------
#ifndef GPOS_CException_H
#define GPOS_CException_H

#include "gpos/types.h"

// SQL state code length
#define GPOS_SQLSTATE_LENGTH 5

// standard way to raise an exception
#define GPOS_RAISE(...) gpos::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)

// raises GPOS exception,
// these exceptions can later be translated to GPDB log severity levels
// so that they can be written in GPDB with appropriate severity level.
#define GPOS_THROW_EXCEPTION(...) gpos::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)

// helper to match a caught exception
#define GPOS_MATCH_EX(ex, major, minor) (major == ex.Major() && minor == ex.Minor())

// being of a try block w/o explicit handler
#define GPOS_TRY                     \
  do {                               \
    CErrorHandler *err_hdl__ = NULL; \
    try {
// begin of a try block
#define GPOS_TRY_HDL(perrhdl)           \
  do {                                  \
    CErrorHandler *err_hdl__ = perrhdl; \
    try {
// begin of a catch block
#define GPOS_CATCH_EX(exc)         \
  }                                \
  catch (gpos::CException & exc) { \
    {                              \
      if (NULL != err_hdl__)       \
        err_hdl__->Process(exc);   \
    }

// end of a catch block
#define GPOS_CATCH_END \
  }                    \
  }                    \
  while (0)

// to be used inside a catch block
#define GPOS_RESET_EX ITask::Self()->GetErrCtxt()->Reset()
#define GPOS_RETHROW(exc) gpos::CException::Reraise(exc)

// short hands for frequently used exceptions
#define GPOS_ABORT GPOS_RAISE(CException::ExmaSystem, CException::ExmiAbort)
#define GPOS_OOM_CHECK(x)                                      \
  do {                                                         \
    if (NULL == (void *)x) {                                   \
      GPOS_RAISE(CException::ExmaSystem, CException::ExmiOOM); \
    }                                                          \
  } while (0)

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CException
//
//	@doc:
//		Basic exception class -- used for "throw by value/catch by reference"
//		Contains only a category (= major) and a error (= minor).
//
//---------------------------------------------------------------------------
class CException {
 public:
  // majors - reserve range 0-99
  enum ExMajor {
    ExmaInvalid = 0,
    ExmaSystem = 1,

    ExmaSQL = 2,

    ExmaUnhandled = 3,

    ExmaSentinel
  };

  // minors
  enum ExMinor {
    // system errors
    ExmiInvalid = 0,
    ExmiAbort,
    ExmiAssert,
    ExmiOOM,
    ExmiOutOfStack,
    ExmiAbortTimeout,
    ExmiIOError,
    ExmiOverflow,
    ExmiInvalidDeletion,

    // sql exceptions
    ExmiSQLDefault,
    ExmiSQLNotNullViolation,
    ExmiSQLCheckConstraintViolation,
    ExmiSQLTest,

    // warnings
    ExmiDummyWarning,

    // unknown exception
    ExmiUnhandled,

    // ORCA in an invalid state
    ExmiORCAInvalidState,

    ExmiSentinel
  };

  // structure for mapping exceptions to SQLerror codes
 private:
  struct ErrCodeElem {
    // exception number
    uint32_t m_exception_num;

    // SQL standard error code
    const char *m_sql_state;
  };

  // error range
  uint32_t m_major;

  // error number
  uint32_t m_minor;

  // SQL state error code
  const char *m_sql_state;

  // filename
  char *m_filename;

  // line in file
  uint32_t m_line;

  // sql state error codes
  static const ErrCodeElem m_errcode[ExmiSQLTest - ExmiSQLDefault + 1];

  // internal raise API
  static void Raise(CException exc) __attribute__((__noreturn__));

  // get sql error code for given exception
  static const char *GetSQLState(uint32_t major, uint32_t minor);

 public:
  // severity levels
  enum ExSeverity {
    ExsevInvalid = 0,
    ExsevError,
    ExsevWarning,
    ExsevNotice,
    ExsevTrace,

    ExsevSentinel
  };

  // severity levels
  static const char *m_severity[ExsevSentinel];

  // ctor
  CException(uint32_t major, uint32_t minor);
  CException(uint32_t major, uint32_t minor, const char *filename, uint32_t line);

  // accessors
  uint32_t Major() const { return m_major; }

  uint32_t Minor() const { return m_minor; }

  const char *Filename() const { return m_filename; }

  uint32_t Line() const { return m_line; }

  const char *GetSQLState() const { return m_sql_state; }

  // simple equality
  bool operator==(const CException &exc) const { return m_major == exc.m_major && m_minor == exc.m_minor; }

  // simple inequality
  bool operator!=(const CException &exc) const { return !(*this == exc); }

  // equality function -- needed for hashtable
  static bool Equals(const CException &exc, const CException &excOther) { return exc == excOther; }

  // basic hash function
  static uint32_t HashValue(const CException &exc) { return exc.m_major ^ exc.m_minor; }

  // wrapper around throw
  static void Raise(const char *filename, uint32_t line, uint32_t major, uint32_t minor, ...)
      __attribute__((__noreturn__));

  // rethrow wrapper
  static void Reraise(CException exc, bool propagate = false) __attribute__((__noreturn__));

  // invalid exception
  static const CException m_invalid_exception;

};  // class CException
}  // namespace gpos

#endif  // !GPOS_CException_H

// EOF
