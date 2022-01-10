package gosearchengine

type Analyzer struct {
	charFilters  []CharFilter
	tokenizer    []Tokenizer
	tokenFilters []TokenFilters
}

func (a Analyzer) Analyze(s string) TokenStream {
	for _, c := range a.charFilters {
		s = c.Filter(s)
	}

	tokenStream := a.tokenizer.Tokenize(s)
	for _, f := range a.tokenFilters {
		tokenStream = f.filter(tokenStream)
	}

	return tokenStream
}
