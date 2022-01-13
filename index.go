package gosearchengine

import "sort"

type InvertedIndex map[TokenID]PostingList

func NewInvertedIndex(m map[TokenID]PostingList) InvertedIndex {
	return InvertedIndex(m)
}

func (ii InvertedIndex) TokenIDs() []TokenID {
	ids := []TokenID{}
	for i := range ii {
		ids = append(ids, i)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

type PostingList struct {
	Postings *Postings
}

func (p PostingList) Size() int {
	size := 0
	pp := p.Postings
	for pp != nil {
		pp = pp.Next
		size++
	}
	return size
}

func (p PostingList) AppearanceCountInDocument(docID DocumentID) int {
	count := 0
	pp := p.Postings
	for pp != nil {
		if pp.DocumentID == docID {
			count = len(pp.Positions)
			break
		}
		pp = pp.Next
	}
	return count
}

type Postings struct {
	DocumentID DocumentID
	Positions  []uint64
	Next       *Postings
}

func (p *Postings) PushBack(e *Postings) {
	e.Next = p.Next
	p.Next = e
}
