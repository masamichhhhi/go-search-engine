package gosearchengine

import "strings"

type CharFilter interface {
	Filter(string) string
}

// 特定の単語から特定の単語への変換マップ(ex. ":("" → "sad"))
type MappingCharFilter struct {
	mapper map[string]string
}

func (c MappingCharFilter) Filter(s string) string {
	for k, v := range c.mapper {
		s = strings.Replace(s, k, v, -1)
	}
	return s
}
