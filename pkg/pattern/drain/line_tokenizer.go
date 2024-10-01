package drain

import (
	"bytes"
	"fmt"
	"strings"
	"unicode"
	"unique"

	"github.com/buger/jsonparser"
	gologfmt "github.com/go-logfmt/logfmt"

	"github.com/grafana/loki/v3/pkg/logql/log/logfmt"
)

type LineTokenizer interface {
	Tokenize(line string, tokens []unique.Handle[string], state interface{}) ([]unique.Handle[string], interface{})
	Join(tokens []unique.Handle[string], state interface{}) string
	Clone(tokens []unique.Handle[string], state interface{}) ([]unique.Handle[string], interface{})
}

type spacesTokenizer struct{}

func (spacesTokenizer) Tokenize(line string, _ []string, _ interface{}) ([]string, interface{}) {
	return strings.Split(line, " "), nil
}

func (spacesTokenizer) Join(tokens []string, _ interface{}) string {
	return strings.Join(tokens, " ")
}

func (spacesTokenizer) Clone(tokens []unique.Handle[string], _ interface{}) ([]unique.Handle[string], interface{}) {
	res := make([]unique.Handle[string], len(tokens))
	copy(res, tokens)
	return res, nil
}

type punctuationTokenizer struct {
	includeDelimiters [128]rune
	excludeDelimiters [128]rune
}

func newPunctuationTokenizer() *punctuationTokenizer {
	var included [128]rune
	var excluded [128]rune
	included['='] = 1
	excluded['_'] = 1
	excluded['-'] = 1
	excluded['.'] = 1
	excluded[':'] = 1
	excluded['/'] = 1
	return &punctuationTokenizer{
		includeDelimiters: included,
		excludeDelimiters: excluded,
	}
}

func (p *punctuationTokenizer) Tokenize(line string, tokens []unique.Handle[string], state interface{}) ([]unique.Handle[string], interface{}) {
	if cap(tokens) == 0 {
		tokens = make([]unique.Handle[string], 0, 128)
	}
	tokens = tokens[:0]
	if state == nil || cap(state.([]int)) == 0 {
		state = make([]int, 0, 64)
	}
	spacesAfter := state.([]int)
	spacesAfter = spacesAfter[:0]

	start := 0
	for i, char := range line {
		if len(tokens) >= 127 {
			break
		}
		if unicode.IsLetter(char) || unicode.IsNumber(char) || char < 128 && p.excludeDelimiters[char] != 0 {
			continue
		}
		included := char < 128 && p.includeDelimiters[char] != 0
		if char == ' ' || included || unicode.IsPunct(char) {
			if i > start {
				tokens = append(tokens, unique.Make(line[start:i]))
			}
			if char == ' ' {
				spacesAfter = append(spacesAfter, len(tokens)-1)
			} else {
				tokens = append(tokens, unique.Make(line[i:i+1]))
			}
			start = i + 1
		}
	}

	if start < len(line) {
		tokens = append(tokens, unique.Make(line[start:]))
	}

	return tokens, spacesAfter
}

func (p *punctuationTokenizer) Join(tokens []unique.Handle[string], state interface{}) string {
	spacesAfter := state.([]int)
	strBuilder := strings.Builder{}
	spacesIdx := 0
	for i, token := range tokens {
		strBuilder.WriteString(token.Value())
		for spacesIdx < len(spacesAfter) && i == spacesAfter[spacesIdx] {
			// One entry for each space following the token
			strBuilder.WriteRune(' ')
			spacesIdx++
		}
	}
	return strBuilder.String()
}

func (p *punctuationTokenizer) Clone(tokens []unique.Handle[string], state interface{}) ([]unique.Handle[string], interface{}) {
	res := make([]unique.Handle[string], len(tokens))
	copy(res, tokens)
	if state == nil {
		return res, nil
	}
	spacesAfter := state.([]int)
	spacesAfterCopy := make([]int, len(spacesAfter))
	copy(spacesAfterCopy, spacesAfter)
	return res, spacesAfterCopy
}

type splittingTokenizer struct{}

func (splittingTokenizer) Tokenize(line string, tokens []string, state interface{}) ([]string, interface{}) {
	numEquals := strings.Count(line, "=")
	numColons := strings.Count(line, ":")
	numSpaces := strings.Count(line, " ")

	expectedTokens := numSpaces + numEquals
	keyvalSeparator := "="
	if numColons > numEquals {
		keyvalSeparator = ":"
		expectedTokens = numSpaces + numColons
	}

	if cap(tokens) == 0 {
		tokens = make([]string, 0, expectedTokens)
	}
	tokens = tokens[:0]
	if state == nil || cap(state.([]int)) == 0 {
		state = make([]int, 0, numSpaces)
	}
	spacesAfter := state.([]int)
	spacesAfter = spacesAfter[:0]

	for _, token := range strings.SplitAfter(line, keyvalSeparator) {
		words := strings.Split(token, " ")
		for i, entry := range words {
			tokens = append(tokens, entry)
			if i == len(words)-1 {
				continue
			}
			spacesAfter = append(spacesAfter, len(tokens)-1)
		}
	}
	return tokens, spacesAfter
}

func (splittingTokenizer) Join(tokens []string, state interface{}) string {
	spacesAfter := state.([]int)
	strBuilder := strings.Builder{}
	spacesIdx := 0
	for i, token := range tokens {
		strBuilder.WriteString(token)
		for spacesIdx < len(spacesAfter) && i == spacesAfter[spacesIdx] {
			// One entry for each space following the token
			strBuilder.WriteRune(' ')
			spacesIdx++
		}
	}
	return strBuilder.String()
}

func (splittingTokenizer) Clone(tokens []unique.Handle[string], state interface{}) ([]unique.Handle[string], interface{}) {
	res := make([]unique.Handle[string], len(tokens))
	copy(res, tokens)
	if state == nil {
		return res, nil
	}
	spacesAfter := state.([]int)
	spacesAfterCopy := make([]int, len(spacesAfter))
	copy(spacesAfterCopy, spacesAfter)
	return res, spacesAfterCopy
}

type logfmtTokenizer struct {
	dec        *logfmt.Decoder
	varReplace unique.Handle[string]
}

func newLogfmtTokenizer(varReplace unique.Handle[string]) *logfmtTokenizer {
	return &logfmtTokenizer{
		dec:        logfmt.NewDecoder(nil),
		varReplace: varReplace,
	}
}

func (t *logfmtTokenizer) Tokenize(line string, tokens []unique.Handle[string], _ interface{}) ([]unique.Handle[string], interface{}) {
	if cap(tokens) == 0 {
		tokens = make([]unique.Handle[string], 0, 64)
	}
	tokens = tokens[:0]
	t.dec.Reset(unsafeBytes(line))
	for !t.dec.EOL() && t.dec.ScanKeyval() {
		key := t.dec.Key()
		if isVariableField(key) {
			tokens = append(tokens, unique.Make(unsafeString(t.dec.Key())), t.varReplace)

			continue
		}
		// todo we want to pass bytes and let user copy if needed.
		tokens = append(tokens, unique.Make(unsafeString(t.dec.Key())), unique.Make(unsafeString(t.dec.Value())))
	}
	if t.dec.Err() != nil {
		return nil, nil
	}
	return tokens, nil
}

func (t *logfmtTokenizer) Join(tokens []unique.Handle[string], _ interface{}) string {
	if len(tokens) == 0 {
		return ""
	}
	if len(tokens)%2 == 1 {
		tokens = append(tokens, unique.Make(""))
	}
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	enc := gologfmt.NewEncoder(buf)
	for i := 0; i < len(tokens); i += 2 {
		k, v := tokens[i].Value(), unsafeBytes(tokens[i+1].Value())
		if err := enc.EncodeKeyval(k, v); err != nil {
			return ""
		}
	}
	return buf.String()
}

func (t *logfmtTokenizer) Clone(tokens []unique.Handle[string], _ interface{}) ([]unique.Handle[string], interface{}) {
	res := make([]unique.Handle[string], len(tokens))
	copy(res, tokens)
	return res, nil
}

type jsonTokenizer struct {
	*punctuationTokenizer
	varReplace unique.Handle[string]
}

func newJSONTokenizer(varReplace unique.Handle[string]) *jsonTokenizer {
	return &jsonTokenizer{newPunctuationTokenizer(), varReplace}
}

func (t *jsonTokenizer) Tokenize(line string, tokens []unique.Handle[string], state interface{}) ([]unique.Handle[string], interface{}) {
	var found []byte
	for _, key := range []string{"log", "message", "msg", "msg_", "_msg", "content"} {
		msg, ty, _, err := jsonparser.Get(unsafeBytes(line), key)
		if err == nil && ty == jsonparser.String {
			found = msg
			break
		}
	}

	if found == nil {
		return nil, nil
	}

	return t.punctuationTokenizer.Tokenize(unsafeString(found), tokens, state)
}

func (t *jsonTokenizer) Join(tokens []unique.Handle[string], state interface{}) string {
	return fmt.Sprintf("%s%s%s", t.varReplace.Value(), t.punctuationTokenizer.Join(tokens, state), t.varReplace.Value())
}

func isVariableField(key []byte) bool {
	return bytes.EqualFold(key, []byte("ts")) ||
		bytes.Equal(key, []byte("t")) ||
		bytes.EqualFold(key, []byte("traceID")) ||
		bytes.EqualFold(key, []byte("time")) ||
		bytes.EqualFold(key, []byte("timestamp"))
}

type DedupingTokenizer struct {
	LineTokenizer
	dedupParam unique.Handle[string]
}

func (d DedupingTokenizer) Join(tokens []unique.Handle[string], state interface{}) string {
	return deduplicatePlaceholders(d.LineTokenizer.Join(tokens, state), d.dedupParam)
}
