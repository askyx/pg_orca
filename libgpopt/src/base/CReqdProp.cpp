//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CReqdProp.cpp
//
//	@doc:
//		Implementation of required properties
//---------------------------------------------------------------------------

#include "gpopt/base/CReqdProp.h"

#include "gpopt/operators/COperator.h"
#include "gpos/base.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif  // GPOS_DEBUG

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CReqdProp::CReqdProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdProp::CReqdProp() = default;

//---------------------------------------------------------------------------
//	@function:
//		CReqdProp::~CReqdProp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdProp::~CReqdProp() = default;

// EOF
