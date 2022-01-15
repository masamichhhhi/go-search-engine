package gosearchengine

type Storage interface {
	CountDocuments() (int, error)
	GetAllDocuments() ([]Document, error)
	GetDocuments([]DocumentID) ([]Document, error)
	AddDocument(Document) (DocumentID, error)
	AddToken(token Token) (TokenID, error)
	GetTokenByTerm(string) (*Token, error)
	GetTokensByTerms([]string) ([]Token, error)
	GetInvertedIndexByTokenIDs([]TokenID) (InvertedIndex, error)
	UpsertInvertedIndex(InvertedIndex) error
}
