package gosearchengine

type Sorter interface {
	Sort([]Document, InvertedIndex, []Token) ([]Document, error)
}
