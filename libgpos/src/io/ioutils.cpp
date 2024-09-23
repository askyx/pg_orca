//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ioutils.cpp
//
//	@doc:
//		Implementation of I/O utilities
//---------------------------------------------------------------------------

#include "gpos/io/ioutils.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CLogger.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CTaskContext.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		ioutils::CheckState
//
//	@doc:
//		Check state of file or directory
//
//---------------------------------------------------------------------------
void gpos::ioutils::CheckState(const char *file_path, SFileStat *file_state) {
  GPOS_ASSERT(nullptr != file_path);
  GPOS_ASSERT(nullptr != file_state);

  // reset file state
  (void)clib::Memset(file_state, 0, sizeof(*file_state));

  int32_t res;

  res = stat(file_path, file_state);

  if (0 != res) {
    GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::FStat
//
//	@doc:
//		Check state of file or directory by file descriptor
//
//---------------------------------------------------------------------------
void gpos::ioutils::CheckStateUsingFileDescriptor(const int32_t file_descriptor, SFileStat *file_state) {
  GPOS_ASSERT(nullptr != file_state);

  // reset file state
  (void)clib::Memset(file_state, 0, sizeof(*file_state));

  int32_t res;

  res = fstat(file_descriptor, file_state);

  if (0 != res) {
    GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::PathExists
//
//	@doc:
//		Check if path is mapped to an accessible file or directory
//
//---------------------------------------------------------------------------
bool gpos::ioutils::PathExists(const char *file_path) {
  GPOS_ASSERT(nullptr != file_path);

  SFileStat fs;

  int32_t res = stat(file_path, &fs);

  return (0 == res);
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::IsDir
//
//	@doc:
//		Check if path is directory
//
//---------------------------------------------------------------------------
bool gpos::ioutils::IsDir(const char *file_path) {
  GPOS_ASSERT(nullptr != file_path);

  SFileStat fs;
  CheckState(file_path, &fs);

  return S_ISDIR(fs.st_mode);
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::IsFile
//
//	@doc:
//		Check if path is file
//
//---------------------------------------------------------------------------
bool gpos::ioutils::IsFile(const char *file_path) {
  GPOS_ASSERT(nullptr != file_path);

  SFileStat fs;
  CheckState(file_path, &fs);

  return S_ISREG(fs.st_mode);
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::FileSize
//
//	@doc:
//		Get file size by file path
//
//---------------------------------------------------------------------------
uint64_t gpos::ioutils::FileSize(const char *file_path) {
  GPOS_ASSERT(nullptr != file_path);
  GPOS_ASSERT(IsFile(file_path));

  SFileStat fs;
  CheckState(file_path, &fs);

  return fs.st_size;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::FileSize
//
//	@doc:
//		Get file size by file descriptor
//
//---------------------------------------------------------------------------
uint64_t gpos::ioutils::FileSize(const int32_t file_descriptor) {
  SFileStat fs;
  CheckStateUsingFileDescriptor(file_descriptor, &fs);

  return fs.st_size;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::CheckFilePermissions
//
//	@doc:
//		Check permissions
//
//---------------------------------------------------------------------------
bool gpos::ioutils::CheckFilePermissions(const char *file_path, uint32_t permission_bits) {
  GPOS_ASSERT(nullptr != file_path);

  SFileStat fs;
  CheckState(file_path, &fs);

  return (permission_bits == (fs.st_mode & permission_bits));
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::CreateDir
//
//	@doc:
//		Create directory with specific permissions
//
//---------------------------------------------------------------------------
void gpos::ioutils::CreateDir(const char *file_path, uint32_t permission_bits) {
  GPOS_ASSERT(nullptr != file_path);

  int32_t res;

  res = mkdir(file_path, (mode_t)permission_bits);

  if (0 != res) {
    GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::RemoveDir
//
//	@doc:
//		Delete directory
//
//---------------------------------------------------------------------------
void gpos::ioutils::RemoveDir(const char *file_path) {
  GPOS_ASSERT(nullptr != file_path);
  GPOS_ASSERT(IsDir(file_path));

  int32_t res;

  // delete existing directory
  res = rmdir(file_path);

  if (0 != res) {
    GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::Unlink
//
//	@doc:
//		Delete file
//
//---------------------------------------------------------------------------
void gpos::ioutils::Unlink(const char *file_path) {
  GPOS_ASSERT(nullptr != file_path);

  // delete existing file
  (void)unlink(file_path);
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::OpenFile
//
//	@doc:
//		Open a file;
//		It shall establish the connection between a file
//		and a file descriptor
//
//---------------------------------------------------------------------------
int32_t gpos::ioutils::OpenFile(const char *file_path, int32_t mode, int32_t permission_bits) {
  GPOS_ASSERT(nullptr != file_path);

  int32_t res = open(file_path, mode, permission_bits);

  GPOS_ASSERT((0 <= res) || (EINVAL != errno));

  return res;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::CloseFile
//
//	@doc:
//		Close a file descriptor
//
//---------------------------------------------------------------------------
int32_t gpos::ioutils::CloseFile(int32_t file_descriptor) {
  int32_t res = close(file_descriptor);

  GPOS_ASSERT(0 == res || EBADF != errno);

  return res;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::GetFileState
//
//	@doc:
//		Get file status
//
//---------------------------------------------------------------------------
int32_t gpos::ioutils::GetFileState(int32_t file_descriptor, SFileStat *file_state) {
  GPOS_ASSERT(nullptr != file_state);

  int32_t res = fstat(file_descriptor, file_state);

  GPOS_ASSERT(0 == res || EBADF != errno);

  return res;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::Write
//
//	@doc:
//		Write to a file descriptor
//
//---------------------------------------------------------------------------
intptr_t gpos::ioutils::Write(int32_t file_descriptor, const void *buffer, const uintptr_t ulpCount) {
  GPOS_ASSERT(nullptr != buffer);
  GPOS_ASSERT(0 < ulpCount);
  GPOS_ASSERT(UINT64_MAX / 2 > ulpCount);

  ssize_t res = write(file_descriptor, buffer, ulpCount);

  GPOS_ASSERT((0 <= res) || EBADF != errno);

  return res;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::Read
//
//	@doc:
//		Read from a file descriptor
//
//---------------------------------------------------------------------------
intptr_t gpos::ioutils::Read(int32_t file_descriptor, void *buffer, const uintptr_t ulpCount) {
  GPOS_ASSERT(nullptr != buffer);
  GPOS_ASSERT(0 < ulpCount);
  GPOS_ASSERT(UINT64_MAX / 2 > ulpCount);

  ssize_t res = read(file_descriptor, buffer, ulpCount);

  GPOS_ASSERT((0 <= res) || EBADF != errno);

  return res;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::CreateTempDir
//
//	@doc:
//		Create a unique temporary directory
//
//---------------------------------------------------------------------------
void gpos::ioutils::CreateTempDir(char *dir_path) {
  GPOS_ASSERT(nullptr != dir_path);

#ifdef GPOS_DEBUG
  const size_t ulNumOfCmp = 6;

  size_t size = clib::Strlen(dir_path);

  GPOS_ASSERT(size > ulNumOfCmp);

  GPOS_ASSERT(0 == clib::Memcmp("XXXXXX", dir_path + (size - ulNumOfCmp), ulNumOfCmp));
#endif  // GPOS_DEBUG

  char *szRes;

  // check to simulate I/O error
  szRes = mkdtemp(dir_path);

  if (nullptr == szRes) {
    GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
  }

  return;
}

// EOF
