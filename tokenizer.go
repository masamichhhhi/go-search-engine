package gosearchengine

import "github.com/masamichhhhi/go-search-engine/morphology"

type Tokenizer interface {
	Tokenize(string) TokenStream
}

type MorphologicalTokenizer struct {
	morphology morphology.Morphology
}

func (t MorphologicalTokenizer) Tokenize(s string) TokenStream {
	mTokens := t.morphology.Analyze(s)
	tokens := make([]Token, len(mTokens))
	for i, t := range mTokens {
		tokens[i] = NewToken(t.Term, setKana(t.Kana))
	}
	return NewTokenStream(tokens)
}
