//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CIOUtils.cpp
//
//	@doc:
//		Implementation of optimizer I/O utilities
//---------------------------------------------------------------------------

#include "gpopt/base/CIOUtils.h"

#include "gpos/base.h"
#include "gpos/io/CFileWriter.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CWorker.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CIOUtils::Dump
//
//	@doc:
//		Dump given string to output file
//
//---------------------------------------------------------------------------
void CIOUtils::Dump(char *file_name, char *sz) {
  CAutoSuspendAbort asa;

  const uint32_t ulWrPerms = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  GPOS_TRY {
    CFileWriter fw;
    fw.Open(file_name, ulWrPerms);
    const uint8_t *pb = reinterpret_cast<const uint8_t *>(sz);
    uintptr_t ulpLength = (uintptr_t)clib::Strlen(sz);
    fw.Write(pb, ulpLength);
    fw.Close();
  }
  GPOS_CATCH_EX(ex) {
    // ignore exceptions during dumping
    GPOS_RESET_EX;
  }
  GPOS_CATCH_END;
}

// EOF
