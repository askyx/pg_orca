//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileWriter.cpp
//
//	@doc:
//		Implementation of file handler for raw output
//---------------------------------------------------------------------------

#include "gpos/io/CFileWriter.h"

#include <fcntl.h>

#include "gpos/base.h"
#include "gpos/io/ioutils.h"
#include "gpos/task/IWorker.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::CFileWriter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFileWriter::CFileWriter() : CFileDescriptor() {}

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Open
//
//	@doc:
//		Open file for writing
//
//---------------------------------------------------------------------------
void CFileWriter::Open(const char *file_path, uint32_t permission_bits) {
  GPOS_ASSERT(nullptr != file_path);

  OpenFile(file_path, O_CREAT | O_WRONLY | O_RDONLY | O_TRUNC, permission_bits);

  GPOS_ASSERT(0 == ioutils::FileSize(file_path));

  m_file_size = 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Close
//
//	@doc:
//		Close file after writing
//
//---------------------------------------------------------------------------
void CFileWriter::Close() {
  CloseFile();

  m_file_size = 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Write
//
//	@doc:
//		Write bytes to file
//
//---------------------------------------------------------------------------
void CFileWriter::Write(const uint8_t *read_buffer, const uintptr_t write_size) {
  GPOS_ASSERT(CFileDescriptor::IsFileOpen() && "Attempt to write to invalid file descriptor");
  GPOS_ASSERT(0 < write_size);
  GPOS_ASSERT(nullptr != read_buffer);

  uintptr_t bytes_left_to_write = write_size;

  while (0 < bytes_left_to_write) {
    intptr_t current_byte;

    // write to file
    current_byte = ioutils::Write(GetFileDescriptor(), read_buffer, bytes_left_to_write);

    // check for error
    if (-1 == current_byte) {
      // in case an interrupt was received we retry
      if (EINTR == errno) {
        GPOS_CHECK_ABORT;

        continue;
      }

      GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
    }

    GPOS_ASSERT(current_byte <= (intptr_t)bytes_left_to_write);

    // increase file size
    m_file_size += current_byte;
    read_buffer += current_byte;
    bytes_left_to_write -= current_byte;
  }
}

// EOF
