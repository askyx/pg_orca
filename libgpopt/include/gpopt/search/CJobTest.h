//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJobTest.h
//
//	@doc:
//		Job implementation for testing purposes
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobTest_H
#define GPOPT_CJobTest_H

#include "gpopt/search/CJob.h"
#include "gpopt/search/CJobQueue.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJobTest
//
//	@doc:
//		Job derivative for unittests
//
//---------------------------------------------------------------------------
class CJobTest : public CJob {
  // friends
  friend class CJobFactory;

 public:
  // job test type
  enum ETestType { EttSpawn, EttStartQueue, EttQueueu };

 private:
  // test type
  ETestType m_ett{EttSpawn};

  // number of job spawning rounds
  uint32_t m_ulRounds{UINT32_MAX};

  // spawning fanout
  uint32_t m_ulFanout{UINT32_MAX};

  // CPU-burning iterations per job
  uint32_t m_ulIters{UINT32_MAX};

  // iteration counter
  static uintptr_t m_ulpCnt;

  // job queue
  CJobQueue *m_pjq;

  // test job spawning
  bool FSpawn(CSchedulerContext *psc);

  // start jobs to be queued
  bool FStartQueue(CSchedulerContext *psc);

  // test job queueing
  bool FQueue(CSchedulerContext *psc);

  // burn some CPU to simulate actual work
  void Loop() const;

 public:
  // ctor
  CJobTest();

  // dtor
  ~CJobTest() override;

  // execution
  bool FExecute(CSchedulerContext *psc) override;

#ifdef GPOS_DEBUG
  // printer
  IOstream &OsPrint(IOstream &) const override;
#endif  // GPOS_DEBUG

  // set execution parameters
  void Init(ETestType ett, uint32_t ulRounds, uint32_t ulFanout, uint32_t ulIters, CJobQueue *pjq) {
    m_ett = ett;
    m_ulRounds = ulRounds;
    m_ulFanout = ulFanout;
    m_ulIters = ulIters;
    m_pjq = pjq;
  }

  // copy execution parameters
  void Init(CJobTest *pjt) { Init(pjt->m_ett, pjt->m_ulRounds, pjt->m_ulFanout, pjt->m_ulIters, pjt->m_pjq); }

  // reset
  static void ResetCnt() { m_ulpCnt = 0; }

  // conversion function
  static CJobTest *PjConvert(CJob *pj) {
    GPOS_ASSERT(nullptr != pj);
    GPOS_ASSERT(EjtTest == pj->Ejt());

    return dynamic_cast<CJobTest *>(pj);
  }
};
}  // namespace gpopt

#endif  // !GPOPT_CJobTest_H

// EOF
