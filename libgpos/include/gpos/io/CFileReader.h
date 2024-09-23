//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CFileReader.h
//
//	@doc:
//		File Reader
//---------------------------------------------------------------------------

#ifndef GPOS_CFileReader_H
#define GPOS_CFileReader_H

#include <fcntl.h>

#include "gpos/io/CFileDescriptor.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CFileReader
//
//	@doc:
//		Implementation of file handler for raw input;
//		does not provide thread-safety
//
//---------------------------------------------------------------------------
class CFileReader : public CFileDescriptor {
 private:
  // file size
  uint64_t m_file_size{0};

  // read size
  uint64_t m_file_read_size{0};

 public:
  CFileReader(const CFileReader &) = delete;

  // ctor
  CFileReader();

  // dtor
  ~CFileReader() override;

  // get file size
  uint64_t FileSize() const;

  // get file read size
  uint64_t FileReadSize() const;

  // open file for reading
  void Open(const char *file_path, const uint32_t permission_bits = S_IRUSR);

  // close file
  void Close();

  // read bytes to buffer
  uintptr_t ReadBytesToBuffer(uint8_t *read_buffer, const uintptr_t file_read_size);

};  // class CFileReader

}  // namespace gpos

#endif  // !GPOS_CFileReader_H

// EOF
