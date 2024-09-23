//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ioutils.h
//
//	@doc:
//		I/O utilities;
//		generic I/O functions that are not associated with file descriptors
//---------------------------------------------------------------------------

#ifndef GPOS_ioutils_H
#define GPOS_ioutils_H

#include <dlfcn.h>
#include <unistd.h>

#include "gpos/io/iotypes.h"
#include "gpos/types.h"

namespace gpos {
namespace ioutils {
// check state of file or directory
void CheckState(const char *file_path, SFileStat *file_state);

// check state of file or directory by file descriptor
void CheckStateUsingFileDescriptor(const int32_t file_descriptor, SFileStat *file_state);

// check if path is mapped to an accessible file or directory
bool PathExists(const char *file_path);

// get file size by file path
uint64_t FileSize(const char *file_path);

// get file size by file descriptor
uint64_t FileSize(const int32_t file_descriptor);

// check if path is directory
bool IsDir(const char *file_path);

// check if path is file
bool IsFile(const char *file_path);

// check permissions
bool CheckFilePermissions(const char *file_path, uint32_t permission_bits);

// create directory with specific permissions
void CreateDir(const char *file_path, uint32_t permission_bits);

// delete file
void RemoveDir(const char *file_path);

// delete file
void Unlink(const char *file_path);

// open a file
int32_t OpenFile(const char *file_path, int32_t mode, int32_t permission_bits);

// close a file descriptor
int32_t CloseFile(int32_t file_descriptor);

// get file status
int32_t GetFileState(int32_t file_descriptor, SFileStat *file_state);

// write to a file descriptor
intptr_t Write(int32_t file_descriptor, const void *buffer, const uintptr_t ulpCount);

// read from a file descriptor
intptr_t Read(int32_t file_descriptor, void *buffer, const uintptr_t ulpCount);

// create a unique temporary directory
void CreateTempDir(char *dir_path);

}  // namespace ioutils
}  // namespace gpos

#endif  // !GPOS_ioutils_H

// EOF
