package mgodumper

type PropertyFilter struct {
	Field string   `json:"field"`
	Eq    []string `json:"eq"`

	eqMap map[string]struct{}
}

func (p *PropertyFilter) Generate() {
	p.eqMap = map[string]struct{}{}

	for _, item := range p.Eq {
		p.eqMap[item] = struct{}{}
	}
}
