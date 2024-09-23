//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTreeMap.h
//
//	@doc:
//		Map of tree components to count, rank, and unrank abstract trees;
//
//		For description of algorithm, see also:
//
//			F. Waas, C. Galindo-Legaria, "Counting, Enumerating, and
//			Sampling of Execution Plans in a Cost-Based Query Optimizer",
//			ACM SIGMOD, 2000
//---------------------------------------------------------------------------
#ifndef GPOPT_CTreeMap_H
#define GPOPT_CTreeMap_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CTreeMap
//
//	@doc:
//		Lookup table for counting/unranking of trees;
//
//		Enables client to associate objects of type T (e.g. CGroupExpression)
//		with a topology solely given by edges between the object. The
//		unranking utilizes client provided function and generates results of
//		type R (e.g., CExpression);
//		U is a global context accessible to recursive rehydrate calls.
//		Pointers to objects of type U are passed through PrUnrank calls to the
//		rehydrate function of type PrFn.
//
//---------------------------------------------------------------------------
template <class T, class R, class U, uint32_t (*HashFn)(const T *), bool (*EqFn)(const T *, const T *)>
class CTreeMap {
  // array of source pointers (sources owned by 3rd party)
  using DrgPt = CDynamicPtrArray<T, CleanupNULL>;

  // array of result pointers (results owned by the tree we unrank)
  using DrgPr = CDynamicPtrArray<R, CleanupRelease<R>>;

  // generic rehydrate function
  using PrFn = R *(*)(CMemoryPool *, T *, DrgPr *, U *);

 private:
  // fwd declaration
  class CTreeNode;

  // arrays of internal nodes
  using CTreeNodeArray = CDynamicPtrArray<CTreeNode, CleanupNULL>;
  using CTreeNode2dArray = CDynamicPtrArray<CTreeNodeArray, CleanupRelease>;

  //---------------------------------------------------------------------------
  //	@class:
  //		STreeLink
  //
  //	@doc:
  //		Internal structure to monitor tree links for duplicate detection
  //		purposes
  //
  //---------------------------------------------------------------------------
  struct STreeLink {
   private:
    // parent node
    const T *m_ptParent;

    // child index
    uint32_t m_ulChildIndex;

    // child node
    const T *m_ptChild;

   public:
    // ctor
    STreeLink(const T *ptParent, uint32_t child_index, const T *ptChild)
        : m_ptParent(ptParent), m_ulChildIndex(child_index), m_ptChild(ptChild) {
      GPOS_ASSERT(nullptr != ptParent);
      GPOS_ASSERT(nullptr != ptChild);
    }

    // dtor
    virtual ~STreeLink() = default;

    // hash function
    static uint32_t HashValue(const STreeLink *ptlink) {
      uint32_t ulHashParent = HashFn(ptlink->m_ptParent);
      uint32_t ulHashChild = HashFn(ptlink->m_ptChild);
      uint32_t ulHashChildIndex = gpos::HashValue<uint32_t>(&ptlink->m_ulChildIndex);

      return CombineHashes(ulHashParent, CombineHashes(ulHashChild, ulHashChildIndex));
    }

    // equality function
    static bool Equals(const STreeLink *ptlink1, const STreeLink *ptlink2) {
      return EqFn(ptlink1->m_ptParent, ptlink2->m_ptParent) && EqFn(ptlink1->m_ptChild, ptlink2->m_ptChild) &&
             ptlink1->m_ulChildIndex == ptlink2->m_ulChildIndex;
    }
  };  // struct STreeLink

  //---------------------------------------------------------------------------
  //	@class:
  //		CTreeNode
  //
  //	@doc:
  //		Internal structure to manage source objects and their topology
  //
  //---------------------------------------------------------------------------
  class CTreeNode {
   private:
    // state of tree node during counting alternatives
    enum ENodeState {
      EnsUncounted,  // counting not initiated
      EnsCounting,   // counting in progress
      EnsCounted,    // counting complete

      EnsSentinel
    };

    // memory pool
    CMemoryPool *m_mp;

    // id of node
    uint32_t m_ul;

    // element
    const T *m_value;

    // array of children arrays
    CTreeNode2dArray *m_pdrgdrgptn;

    // number of trees rooted in this node
    uint64_t m_ullCount;

    // number of incoming edges
    uint32_t m_ulIncoming;

    // node state used for counting alternatives
    ENodeState m_ens;

    // total tree count for a given child
    uint64_t UllCount(uint32_t ulChild) {
      GPOS_CHECK_STACK_SIZE;

      uint64_t ull = 0;

      uint32_t ulCandidates = (*m_pdrgdrgptn)[ulChild]->Size();
      for (uint32_t ulAlt = 0; ulAlt < ulCandidates; ulAlt++) {
        CTreeNode *ptn = (*(*m_pdrgdrgptn)[ulChild])[ulAlt];
        uint64_t ullCount = ptn->UllCount();
        ull = gpos::Add(ull, ullCount);
      }

      return ull;
    }

    // rehydrate tree
    R *PrUnrank(CMemoryPool *mp, PrFn prfn, U *pU, uint32_t ulChild, uint64_t ullRank) {
      GPOS_CHECK_STACK_SIZE;
      GPOS_ASSERT(ullRank < UllCount(ulChild));

      CTreeNodeArray *pdrgptn = (*m_pdrgdrgptn)[ulChild];
      uint32_t ulCandidates = pdrgptn->Size();

      CTreeNode *ptn = nullptr;

      for (uint32_t ul = 0; ul < ulCandidates; ul++) {
        ptn = (*pdrgptn)[ul];
        uint64_t ullLocalCount = ptn->UllCount();

        if (ullRank < ullLocalCount) {
          // ullRank is now local rank for the child
          break;
        }

        ullRank -= ullLocalCount;
      }

      GPOS_ASSERT(nullptr != ptn);
      return ptn->PrUnrank(mp, prfn, pU, ullRank);
    }

   public:
    // ctor
    CTreeNode(CMemoryPool *mp, uint32_t ul, const T *value)
        : m_mp(mp),
          m_ul(ul),
          m_value(value),
          m_pdrgdrgptn(nullptr),
          m_ullCount(UINT64_MAX),
          m_ulIncoming(0),
          m_ens(EnsUncounted) {
      m_pdrgdrgptn = GPOS_NEW(mp) CTreeNode2dArray(mp);
    }

    // dtor
    ~CTreeNode() { m_pdrgdrgptn->Release(); }

    // add child alternative
    void Add(uint32_t ulPos, CTreeNode *ptn) {
      GPOS_ASSERT(!FCounted() && "Adding edges after counting not meaningful");

      // insert any child arrays skipped so far; make sure we have a dense
      // array up to the position of ulPos
      uint32_t length = m_pdrgdrgptn->Size();
      for (uint32_t ul = length; ul <= ulPos; ul++) {
        CTreeNodeArray *pdrg = GPOS_NEW(m_mp) CTreeNodeArray(m_mp);
        m_pdrgdrgptn->Append(pdrg);
      }

      // increment count of incoming edges
      ptn->m_ulIncoming++;

      // insert to appropriate array
      CTreeNodeArray *pdrg = (*m_pdrgdrgptn)[ulPos];
      GPOS_ASSERT(nullptr != pdrg);
      pdrg->Append(ptn);
    }

    // accessor
    const T *Value() const { return m_value; }

    // number of trees rooted in this node
    uint64_t UllCount() {
      GPOS_CHECK_STACK_SIZE;

      GPOS_ASSERT(EnsCounting != m_ens && "cycle in graph detected");

      if (!FCounted()) {
        // initiate counting on current node
        m_ens = EnsCounting;

        uint64_t ullCount = 1;

        uint32_t arity = m_pdrgdrgptn->Size();
        for (uint32_t ulChild = 0; ulChild < arity; ulChild++) {
          uint64_t ull = UllCount(ulChild);
          if (0 == ull) {
            // if current child has no alternatives, the parent cannot have alternatives
            ullCount = 0;
            break;
          }

          // otherwise, multiply number of child alternatives by current count
          ullCount = gpos::Multiply(ullCount, ull);
        }

        // counting is complete
        m_ullCount = ullCount;
        m_ens = EnsCounted;
      }

      return m_ullCount;
    }

    // check if count has been determined for this node
    bool FCounted() const { return (EnsCounted == m_ens); }

    // number of incoming edges
    uint32_t UlIncoming() const { return m_ulIncoming; }

    // unrank tree of a given rank with a given rehydrate function
    R *PrUnrank(CMemoryPool *mp, PrFn prfn, U *pU, uint64_t ullRank) {
      GPOS_CHECK_STACK_SIZE;

      R *pr = nullptr;

      if (0 == this->m_ul) {
        // global root, just unrank 0-th child
        pr = PrUnrank(mp, prfn, pU, 0 /* ulChild */, ullRank);
      } else {
        DrgPr *pdrg = GPOS_NEW(mp) DrgPr(mp);

        uint64_t ullRankRem = ullRank;

        uint32_t ulChildren = m_pdrgdrgptn->Size();
        for (uint32_t ulChild = 0; ulChild < ulChildren; ulChild++) {
          uint64_t ullLocalCount = UllCount(ulChild);
          GPOS_ASSERT(0 < ullLocalCount);
          uint64_t ullLocalRank = ullRankRem % ullLocalCount;

          pdrg->Append(PrUnrank(mp, prfn, pU, ulChild, ullLocalRank));

          ullRankRem /= ullLocalCount;
        }

        pr = prfn(mp, const_cast<T *>(this->Value()), pdrg, pU);
      }

      return pr;
    }

#ifdef GPOS_DEBUG

    // debug print
    IOstream &OsPrint(IOstream &os) {
      uint32_t ulChildren = m_pdrgdrgptn->Size();

      os << "=== Node " << m_ul << " [" << *Value() << "] ===" << std::endl
         << "# children: " << ulChildren << std::endl
         << "# count: " << this->UllCount() << std::endl;

      for (uint32_t ul = 0; ul < ulChildren; ul++) {
        os << "--- child: #" << ul << " ---" << std::endl;
        uint32_t ulAlt = (*m_pdrgdrgptn)[ul]->Size();

        for (uint32_t ulChild = 0; ulChild < ulAlt; ulChild++) {
          CTreeNode *ptn = (*(*m_pdrgdrgptn)[ul])[ulChild];
          os << "  -> " << ptn->m_ul << " [" << *ptn->Value() << "]" << std::endl;
        }
      }

      return os;
    }

#endif  // GPOS_DEBUG
  };

  // memory pool
  CMemoryPool *m_mp;

  // counter for nodes
  uint32_t m_ulCountNodes;

  // counter for links
  uint32_t m_ulCountLinks;

  // rehydrate function
  PrFn m_prfn;

  // universal root (internally used only)
  CTreeNode *m_ptnRoot;

  // map of all nodes
  using TMap = gpos::CHashMap<T, CTreeNode, HashFn, EqFn, CleanupNULL, CleanupDelete<CTreeNode>>;
  using TMapIter = gpos::CHashMapIter<T, CTreeNode, HashFn, EqFn, CleanupNULL, CleanupDelete<CTreeNode>>;

  // map of created links
  using LinkMap =
      CHashMap<STreeLink, bool, STreeLink::HashValue, STreeLink::Equals, CleanupDelete<STreeLink>, CleanupDelete<bool>>;

  TMap *m_ptmap;

  // map of nodes to outgoing links
  LinkMap *m_plinkmap;

  // recursive count starting in given node
  uint64_t UllCount(CTreeNode *ptn);

  // Convert to corresponding treenode, create treenode as necessary
  CTreeNode *Ptn(const T *value) {
    GPOS_ASSERT(nullptr != value);
    CTreeNode *ptn = m_ptmap->Find(value);

    if (nullptr == ptn) {
      ptn = GPOS_NEW(m_mp) CTreeNode(m_mp, ++m_ulCountNodes, value);
      (void)m_ptmap->Insert(const_cast<T *>(value), ptn);
    }

    return ptn;
  }

  // private copy ctor
  CTreeMap(const CTreeMap &);

 public:
  // ctor
  CTreeMap(CMemoryPool *mp, PrFn prfn)
      : m_mp(mp),
        m_ulCountNodes(0),
        m_ulCountLinks(0),
        m_prfn(prfn),
        m_ptnRoot(nullptr),
        m_ptmap(nullptr),
        m_plinkmap(nullptr) {
    GPOS_ASSERT(nullptr != mp);
    GPOS_ASSERT(nullptr != prfn);

    m_ptmap = GPOS_NEW(mp) TMap(mp);
    m_plinkmap = GPOS_NEW(mp) LinkMap(mp);

    // insert dummy node as global root -- the only node with NULL payload
    m_ptnRoot = GPOS_NEW(mp) CTreeNode(mp, 0 /* ulCounter */, nullptr /* value */);
  }

  // dtor
  ~CTreeMap() {
    m_ptmap->Release();
    m_plinkmap->Release();

    GPOS_DELETE(m_ptnRoot);
  }

  // insert edge as n-th child
  void Insert(const T *ptParent, uint32_t ulPos, const T *ptChild) {
    GPOS_ASSERT(ptParent != ptChild);

    // exit function if link already exists
    STreeLink *ptlink = GPOS_NEW(m_mp) STreeLink(ptParent, ulPos, ptChild);
    if (nullptr != m_plinkmap->Find(ptlink)) {
      GPOS_DELETE(ptlink);
      return;
    }

    CTreeNode *ptnParent = Ptn(ptParent);
    CTreeNode *ptnChild = Ptn(ptChild);

    ptnParent->Add(ulPos, ptnChild);
    ++m_ulCountLinks;

    // add created link to links map
    bool fInserted GPOS_ASSERTS_ONLY = m_plinkmap->Insert(ptlink, GPOS_NEW(m_mp) bool(true));
    GPOS_ASSERT(fInserted);
  }

  // insert a root node
  void InsertRoot(const T *value) {
    GPOS_ASSERT(NULL != value);
    GPOS_ASSERT(NULL != m_ptnRoot);

    // add logical root as 0-th child to global root
    m_ptnRoot->Add(0 /*ulPos*/, Ptn(value));
  }

  // count all possible combinations
  uint64_t UllCount() {
    // first, hookup all logical root nodes to the global root
    TMapIter mi(m_ptmap);
    uint32_t ulNodes = 0;
    for (ulNodes = 0; mi.Advance(); ulNodes++) {
      CTreeNode *ptn = const_cast<CTreeNode *>(mi.Value());

      if (0 == ptn->UlIncoming()) {
        // add logical root as 0-th child to global root
        m_ptnRoot->Add(0 /*ulPos*/, ptn);
      }
    }

    // special case of empty map
    if (0 == ulNodes) {
      return 0;
    }

    return m_ptnRoot->UllCount();
  }

  // unrank a specific tree
  R *PrUnrank(CMemoryPool *mp, U *pU, uint64_t ullRank) const { return m_ptnRoot->PrUnrank(mp, m_prfn, pU, ullRank); }

  // return number of nodes
  uint32_t UlNodes() const { return m_ulCountNodes; }

  // return number of links
  uint32_t UlLinks() const { return m_ulCountLinks; }

#ifdef GPOS_DEBUG

  // retrieve count for individual element
  uint64_t UllCount(const T *value) {
    CTreeNode *ptn = m_ptmap->Find(value);
    GPOS_ASSERT(nullptr != ptn);

    return ptn->UllCount();
  }

  // debug print of entire map
  IOstream &OsPrint(IOstream &os) const {
    TMapIter mi(m_ptmap);
    uint32_t ulNodes = 0;
    for (ulNodes = 0; mi.Advance(); ulNodes++) {
      CTreeNode *ptn = const_cast<CTreeNode *>(mi.Value());
      (void)ptn->OsPrint(os);
    }

    os << "total number of nodes: " << ulNodes << std::endl;

    return os;
  }

#endif  // GPOS_DEBUG

};  // class CTreeMap

}  // namespace gpopt

#endif  // !GPOPT_CTreeMap_H

// EOF
