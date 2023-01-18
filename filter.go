package mgodumper

import (
	"fmt"
	"io"
	"time"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
)

type Filter struct {
	parsers []<-chan bson.Raw
	pick    []string
	output  io.Writer
	limit   int

	query bson.M

	propertyFilter *PropertyFilter
}

func NewFilter(parsers []<-chan bson.Raw, pick []string, output io.Writer, limit int) *Filter {
	return &Filter{parsers: parsers, pick: pick, limit: limit, output: output}
}

func (f *Filter) SetPropertyFilter(filter *PropertyFilter) {
	f.propertyFilter = filter
}

func (f *Filter) SetQuery(query string) error {
	q := bson.M{}

	if err := bson.UnmarshalExtJSON([]byte(query), false, &q); err != nil {
		return fmt.Errorf("cannot parse extjson: %w", err)
	}

	f.query = q

	return nil
}

func (f *Filter) Start() error {
	multiChannel := lo.FanIn(1000, f.parsers...)

	count := 0
	matched := 0
	lastTimeLog := time.Now()
	startTime := time.Now()

	for doc := range multiChannel {
		if f.limit > 0 && count >= f.limit {
			break
		}

		m := bson.M{}

		for _, field := range f.pick {
			rawValue := doc.Lookup(field)

			switch rawValue.Type {
			case bson.TypeString:
				m[field] = rawValue.StringValue()
			case bson.TypeObjectID:
				m[field] = rawValue.ObjectID()
			case bson.TypeDateTime:
				m[field] = rawValue.Time()
			case bson.TypeTimestamp:
				m[field] = rawValue.DateTime()
			case bson.TypeArray:
				var arr []interface{}

				if err := rawValue.Unmarshal(&arr); err != nil {
					return err
				}

				m[field] = arr
			}
		}

		match, err := f.match(m)
		if err != nil {
			return err
		}

		propertyMatched := true

		if f.propertyFilter != nil {
			prop := m[f.propertyFilter.Field]
			propString, ok := prop.(string)
			if ok && propString != "" {
				if _, ok := f.propertyFilter.eqMap[propString]; !ok {
					propertyMatched = false
				}
			}
		}

		if match && propertyMatched {
			matched++
			if err := f.format(m); err != nil {
				return err
			}
		}

		count++

		if count%100_000 == 0 {
			fmt.Printf("Have processed %v documents, matched % documents. Speed %v on %s \r\n", count, matched, 100_000, time.Since(lastTimeLog))
			lastTimeLog = time.Now()
		}
	}

	fmt.Println("have finished work", time.Since(startTime))

	return nil
}

func (f *Filter) match(data bson.M) (bool, error) {
	kitDoc, err := bsonkit.Convert(data)
	if err != nil {
		return false, err
	}

	queryDoc, err := bsonkit.Convert(f.query)
	if err != nil {
		return false, err
	}

	match, err := mongokit.Match(kitDoc, queryDoc)
	if err != nil {
		return false, err
	}

	return match, nil
}

func (f *Filter) format(data bson.M) error {
	b, err := bson.MarshalExtJSON(data, false, false)
	if err != nil {
		return err
	}

	f.output.Write(b)
	f.output.Write([]byte("\n"))

	return nil
}
