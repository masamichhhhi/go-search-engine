package gosearchengine

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

func andMatch(postings []*Postings) DocumentID

func orMatch(postings []*Postings) DocumentID
