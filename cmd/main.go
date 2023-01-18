package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/matroskin13/mgodumper"

	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	if err := process(); err != nil {
		log.Fatal(err)
	}
}

func process() error {
	outputPath := flag.String("output", "output.json", "Specify output file")
	query := flag.String("query", "{}", "Specify query for filter documents")
	fieldsRaw := flag.String("fields", "_id", "Specify field in output documents")
	propertyFilterPath := flag.String("property-filter", "", "")
	path := flag.String("path", "", "")

	flag.Parse()

	fields := strings.Split(*fieldsRaw, ",")

	parser := mgodumper.NewParser(*path, true)
	output, err := os.Create(*outputPath)
	if err != nil {
		return err
	}

	ch, err := parser.Start()
	if err != nil {
		return err
	}

	// []string{"_id", "page_id", "variables", "fb_user_info_revised_date", "ig_user_info_changed_date", "created_date"}

	filter := mgodumper.NewFilter([]<-chan bson.Raw{ch}, fields, output, 0)

	if err := filter.SetQuery(*query); err != nil {
		return err
	}

	if propertyFilterPath != nil && *propertyFilterPath != "" {
		pf, err := loadPropertyFilter(*propertyFilterPath)
		if err != nil {
			return err
		}

		pf.Generate()

		filter.SetPropertyFilter(pf)
	}

	return filter.Start()
}

func loadPropertyFilter(path string) (*mgodumper.PropertyFilter, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var propFilter mgodumper.PropertyFilter

	if err := json.Unmarshal(b, &propFilter); err != nil {
		return nil, err
	}

	return &propFilter, nil
}
