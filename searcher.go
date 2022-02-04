package gosearchengine

import "sort"

type Logic int

const (
	AND Logic = iota + 1
	OR
)

type Searcher interface {
	Search() ([]Document, error)
}

type MatchSearcher struct {
	tokenStream TokenStream
	logic       Logic
	storage     Storage
	sorter      Sorter
}

func (ms MatchSearcher) Search() ([]Document, error) {
	if ms.tokenStream.Size() == 0 {
		return []Document{}, nil
	}

	tokens, err := ms.storage.GetTokensByTerms(ms.tokenStream.Terms())
	if err != nil {
		return nil, err
	}

	// 対応トークンが一つも存在しないなら、マッチするドキュメントなしでリターン
	if len(tokens) == 0 {
		return []Document{}, nil
	}

	// AND検索で対応するトークンが全て存在していなかったら、マッチするドキュメントなしでリターン
	if ms.logic == AND && len(tokens) != len(ms.tokenStream.Terms()) {
		return []Document{}, nil
	}

	// ストレージから転置インデックスをREAD
	inverted, err := ms.storage.GetInvertedIndexByTokenIDs(tokenIDs(tokens))
	if err != nil {
		return nil, err
	}

	// ポスティングリストを抽出
	postings := make([]*Postings, len(inverted))
	for i, t := range tokens {
		postings[i] = inverted[t.ID].Postings
	}

	// ポスティングリストを走査してドキュメントIDを取得
	var matchedIds []DocumentID
	if ms.logic == AND {
		matchedIds = andMatch(postings)
	} else if ms.logic == OR {
		matchedIds = orMatch(postings)
	}

	documents, err := ms.storage.GetDocuments(matchedIds)
	if err != nil {
		return nil, err
	}

	if ms.sorter == nil {
		return documents, nil
	}
	return ms.sorter.Sort(documents, inverted, tokens)
}

func tokenIDs(tokens []Token) []TokenID {
	ids := make([]TokenID, len(tokens))
	for i, t := range tokens {
		ids[i] = t.ID
	}
	return ids
}

// 全てのポスティングリストがnilではない
func notAllNil(postings []*Postings) bool {
	for _, p := range postings {
		if p == nil {
			return false
		}
	}
	return true
}

func allNil(postings []*Postings) bool {
	for _, p := range postings {
		if p != nil {
			return false
		}
	}
	return true
}

func isSameDocumentId(postings []*Postings) bool {
	for i := 0; i < len(postings)-1; i++ {
		if postings[i].DocumentID != postings[i+1].DocumentID {
			return false
		}
	}
	return true
}

func next(postings []*Postings) {
	for i := range postings {
		postings[i] = postings[i].Next
	}
}

// ポスティングリストのスライスから最小のドキュメントIDを指しているポスティングリストのインデックス
func minDocumentIDIndex(postings []*Postings) int {
	min := 0
	for i := 1; i < len(postings); i++ {
		if postings[min].DocumentID > postings[i].DocumentID {
			min = i
		}
	}
	return min
}

// ドキュメントIDのスライスで重複を削除
func uniqueDocumentId(ids []DocumentID) []DocumentID {
	m := make(map[DocumentID]struct{})
	for _, id := range ids {
		m[id] = struct{}{}
	}
	uniq := []DocumentID{}
	for k := range m {
		uniq = append(uniq, k)
	}
	sort.Slice(uniq, func(i, j int) bool { return uniq[i] < uniq[j] })
	return uniq
}

func andMatch(postings []*Postings) []DocumentID {
	var ids []DocumentID = make([]DocumentID, 0)
	for notAllNil(postings) {
		if isSameDocumentId(postings) {
			ids = append(ids, postings[0].DocumentID)
			next(postings)
			continue
		}
		idx := minDocumentIDIndex(postings)
		postings[idx] = postings[idx].Next
	}
	return ids
}

func orMatch(postings []*Postings) []DocumentID {
	ids := []DocumentID{}
	for !allNil(postings) {
		for i, p := range postings {
			if p == nil {
				continue
			}
			ids = append(ids, p.DocumentID)
			postings[i] = postings[i].Next
		}
	}
	return uniqueDocumentId(ids)
}
