//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileWriter.h
//
//	@doc:
//		File writer
//---------------------------------------------------------------------------

#ifndef GPOS_CFileWriter_H
#define GPOS_CFileWriter_H

#include "gpos/io/CFileDescriptor.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CFileWriter
//
//	@doc:
//		Implementation of file handler for raw output;
//		does not provide thread-safety
//
//---------------------------------------------------------------------------
class CFileWriter : public CFileDescriptor {
 private:
  // file size
  uint64_t m_file_size{0};

 public:
  CFileWriter(const CFileWriter &) = delete;

  // ctor
  CFileWriter();

  // dtor
  ~CFileWriter() override = default;

  uint64_t FileSize() const { return m_file_size; }

  // open file for writing
  void Open(const char *file_path, uint32_t permission_bits);

  // close file
  void Close();

  // write bytes to file
  void Write(const uint8_t *read_buffer, const uintptr_t write_size);

};  // class CFileWriter

}  // namespace gpos

#endif  // !GPOS_CFileWriter_H

// EOF
