package shared

import (
	"sort"
	"strings"
)

type StringSlice []string

func (s StringSlice) Contains(text string) bool {
	for _, str := range s {
		if str == text {
			return true
		}
	}

	return false
}

func (s StringSlice) String() string {
	return strings.Join(s, ", ")
}

func (s StringSlice) Sort() StringSlice {
	sort.Strings(s)
	return s
}
