package gosearchengine

type Indexer struct {
	storage            Storage
	analyzer           Analyzer
	invertedIndex      InvertedIndex
	indexSizeThreshold int // ストレージにマージするときの閾値
}

func (i *Indexer) AddDocument(doc Document) error {
	// ドキュメントをトークンの集合に分解
	tokens := i.analyzer.Analyze(doc.Body)
	doc.TokenCount = tokens.Size()

	docID, err := i.storage.AddDocument(doc)
	if err != nil {
		return err
	}
	doc.ID = docID

	if err := i.updateMemoryInvertedIndexByDocument(docID, tokens); err != nil {
		return err
	}

	// 閾値を超えているかチェック
	if len(i.invertedIndex) < i.indexSizeThreshold {
		return nil
	}

	// 超えていればストレージとメモリの転置インデックスをマージ
	storageInvertedIndex, err := i.storage.GetInvertedIndexByTokenIDs(i.invertedIndex.TokenIDs())
	if err != nil {
		return err
	}

	for tokenID, postingList := range i.invertedIndex {
		i.invertedIndex[tokenID] = merge(postingList, storageInvertedIndex[tokenID])
	}

	if err := i.storage.UpsertInvertedIndex(i.invertedIndex); err != nil {
		return nil
	}

	i.invertedIndex = InvertedIndex{}
	return nil
}

// トークンからメモリ上の転置インデックスを更新する
func (i *Indexer) updateMemoryInvertedIndexByDocument(docID DocumentID, tokens TokenStream) error {
	for pos, token := range tokens.Tokens {
		if err := i.updateMemoryPostingListByToken(docID, token, uint64(pos)); err != nil {
			return err
		}
	}
	return nil
}

func (i *Indexer) updateMemoryPostingListByToken(docID DocumentID, token Token, pos uint64) error {
	sToken, err := i.storage.GetTokenByTerm(token.Term)
	if err != nil {
		return err
	}
	var tokenID TokenID
	if sToken == nil {
		tokenID, err = i.storage.AddToken(NewToken(token.Term))
		if err != nil {
			return err
		}
	} else {
		tokenID = sToken.ID
	}

	postingList, ok := i.invertedIndex[tokenID]
	// メモリ上にトークンに対応するポスティングが無い時
	if !ok {
		i.invertedIndex[tokenID] = PostingList{
			Postings: NewPosting(docID, []uint64{pos}, nil),
		}
		return nil
	}

	// ドキュメントに対応するポスティングが存在するかどうか
	// p == nilになる前にループ終了: 存在する
	// p == nilまでループが回る: 存在しない
	var p *Postings = postingList.Postings
	for p != nil && p.DocumentID != docID {
		p = p.Next
	}

	// 存在したら、ポジションを更新
	if p != nil {
		p.Positions = append(p.Positions, pos)
		i.invertedIndex[tokenID] = postingList
		return nil
	}

	// まだ対象ドキュメントのポスティングが存在しない時
	// 1.追加されるポスティングのドキュメントIDが最小の時 or 2.追加されるポスティングのドキュメントIDが最小でない時
	// 1の時
	if docID < postingList.Postings.DocumentID {
		postingList.Postings = NewPosting(docID, []uint64{pos}, postingList.Postings)
		i.invertedIndex[tokenID] = postingList
		return nil
	}
	// 2の時
	// ドキュメントIDが昇順になるように挿入する場所を見つける
	var t *Postings = postingList.Postings
	for t.Next != nil && t.Next.DocumentID < docID {
		t = t.Next
	}
	t.PushBack(NewPosting(docID, []uint64{pos}, nil))
	i.invertedIndex[tokenID] = postingList
	return nil

}

func merge(origin, target PostingList) PostingList {
	if origin.Postings == nil {
		return target
	}
	if target.Postings == nil {
		return origin
	}

	merged := PostingList{
		Postings: nil,
	}
	var smaller, larger *Postings
	if origin.Postings.DocumentID <= target.Postings.DocumentID {
		merged.Postings = origin.Postings
		smaller, larger = origin.Postings, target.Postings
	} else {
		merged.Postings = target.Postings
		smaller, larger = target.Postings, origin.Postings
	}

	// 昇順にしたときの一番最後まで
	for larger != nil {
		// smallerが一番大きい（後ろがなにもない）とき
		if smaller.Next == nil {
			smaller.Next = larger
			break
		}

		if smaller.Next.DocumentID < larger.DocumentID {
			smaller = smaller.Next
		} else if smaller.Next.DocumentID > larger.DocumentID {
			largerNext, smallerNext := larger.Next, smaller.Next
			smaller.Next, larger.Next = larger, smallerNext
			smaller = larger
			larger = largerNext
		} else if smaller.Next.DocumentID == larger.DocumentID {
			smaller, larger = smaller.Next, larger.Next
		}
	}
	return merged
}
