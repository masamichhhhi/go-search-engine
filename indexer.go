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

func (i *Indexer) updateMemoryInvertedIndexByDocument(docID DocumentID, tokens TokenStream) error

func merge(origin, target PostingList) PostingList
