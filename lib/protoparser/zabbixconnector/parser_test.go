package zabbixconnector

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestRowsUnmarshalFailure(t *testing.T) {
	f := func(s string, fl []string) {
		t.Helper()

		os.Args = fl
		flag.Parse()

		var rows Rows
		rows.Unmarshal(s)
		if len(rows.Rows) != 0 {
			t.Fatalf("expecting zero rows; got %d rows", len(rows.Rows))
		}

		// Try again
		rows.Unmarshal(s)
		if len(rows.Rows) != 0 {
			t.Fatalf("expecting zero rows; got %d rows", len(rows.Rows))
		}
	}

	par := []string{"test", "-zabbixconnector.addGroups", "1", "-zabbixconnector.addEmptyTags", "1"}
	// Invalid json line
	f("", par)
	f("\n", par)
	f("foo\n", par)
	f("123", par)
	f("[1,3]", par)
	f("{}", par)
	f("[]", par)
	f(`{"foo":"bar"}`, par)

	// Invalid type
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":"0"}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":[]}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":2}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":10}`, par)

	// Invalid host object
	f(`{"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":1},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":[]},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":1,"name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":{},"name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":"1","groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":[],"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)

	// Invalid item name
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":1,"clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":{},"clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)

	// Invalid item value
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":"1","type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":[],"type":0}`, par)

	// Invalid clock
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":"1712417868","ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":[],"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1.1,"ns":425677241,"value":1,"type":0}`, par)

	// Invalid ns
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":"425677241","value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":{},"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":1.2,"value":1,"type":0}`, par)

	// Invalit groups
	f(`{"host":{"host":"h1","name":"n1"},"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":1,"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":{},"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)

	// Invalid item_tags
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":1,"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":{},"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par)
}

func TestRowsUnmarshalSuccess(t *testing.T) {
	f := func(s string, fl []string, rowsExpected *Rows) {
		t.Helper()

		os.Args = fl
		flag.Parse()

		var rows Rows
		rows.Unmarshal(s)

		if err := compareRows(&rows, rowsExpected); err != nil {
			t.Fatalf("unexpected rows: %s;\ngot\n%+v;\nwant\n%+v", err, rows.Rows, rowsExpected.Rows)
		}

		// Try unmarshaling again
		rows.Unmarshal(s)
		if err := compareRows(&rows, rowsExpected); err != nil {
			t.Fatalf("unexpected rows at second unmarshal: %s;\ngot\n%+v;\nwant\n%+v", err, rows.Rows, rowsExpected.Rows)
		}

		rows.Reset()
		if len(rows.Rows) != 0 {
			t.Fatalf("non-empty rows after reset: %+v", rows.Rows)
		}
	}

	// Add groups and empty tags
	par := []string{"test", "-zabbixconnector.addGroups", "1", "-zabbixconnector.addEmptyTags", "1"}
	// Empty line
	f("", par, &Rows{})
	f("\n\n", par, &Rows{})
	f("\n\r\n", par, &Rows{})

	// Single line with groups and empty tags
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("g1"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
						{
							Key:   []byte("tn2"),
							Value: []byte("1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
			},
		})

	// Single line with groups and damage tags
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":{}},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("g1"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn2"),
							Value: []byte("1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
			},
		})

	// Single line with empty groups and empty tags
	f(`{"host":{"host":"h1","name":"n1"},"groups":[],"item_tags":[],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
			},
		})

	// Multiple lines
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}
	{"host":{"host":"h2","name":"n2"},"groups":["g2"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":2,"name":"in2","clock":1712417868,"ns":425677241,"value":1.5,"type":3}
	`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("g1"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
						{
							Key:   []byte("tn2"),
							Value: []byte("1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h2"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n2"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in2"),
						},
						{
							Key:   []byte("g2"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1.5,
					Timestamp: 1712417868425,
				},
			},
		})

	// Multiple lines with invalid lines in the middle.
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}
	failed line

	{"host":{"host":"h2","name":"n2"},"groups":["g2"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":2,"name":"in2","clock":1712417868,"ns":425677241,"value":1.5,"type":3}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("g1"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
						{
							Key:   []byte("tn2"),
							Value: []byte("1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h2"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n2"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in2"),
						},
						{
							Key:   []byte("g2"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1.5,
					Timestamp: 1712417868425,
				},
			},
		})

	// No newline after the second line.
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}
	{"host":{"host":"h2","name":"n2"},"groups":["g2"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":2,"name":"in2","clock":1712417868,"ns":425677241,"value":1.5,"type":3}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("g1"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
						{
							Key:   []byte("tn2"),
							Value: []byte("1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h2"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n2"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in2"),
						},
						{
							Key:   []byte("g2"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1.5,
					Timestamp: 1712417868425,
				},
			},
		})

	// Add empty tags and skip groups
	par = []string{"test", "-zabbixconnector.addGroups", "", "-zabbixconnector.addEmptyTags", "1"}
	// Multiple lines with invalid lines in the middle.
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}
	failed line

	{"host":{"host":"h2","name":"n2"},"groups":["g2"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":2,"name":"in2","clock":1712417868,"ns":425677241,"value":1.5,"type":3}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
						{
							Key:   []byte("tn2"),
							Value: []byte("1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h2"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n2"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in2"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1.5,
					Timestamp: 1712417868425,
				},
			},
		})

	// Add groups and skip empty tags
	par = []string{"test", "-zabbixconnector.addGroups", "1", "-zabbixconnector.addEmptyTags", ""}

	// Multiple lines with invalid lines in the middle.
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}
	failed line

	{"host":{"host":"h2","name":"n2"},"groups":["g2"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":2,"name":"in2","clock":1712417868,"ns":425677241,"value":1.5,"type":3}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("g1"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h2"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n2"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in2"),
						},
						{
							Key:   []byte("g2"),
							Value: []byte("1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1.5,
					Timestamp: 1712417868425,
				},
			},
		})

	// skip groups and empty tags
	par = []string{"test", "-zabbixconnector.addGroups", "", "-zabbixconnector.addEmptyTags", ""}

	// Multiple lines with invalid lines in the middle.
	f(`{"host":{"host":"h1","name":"n1"},"groups":["g1"],"item_tags":[{"tag":"tn1","value":"tv1"},{"tag":"tn2","value":""}],"itemid":1,"name":"in1","clock":1712417868,"ns":425677241,"value":1,"type":0}
failed line

{"host":{"host":"h2","name":"n2"},"groups":["g2"],"item_tags":[{"tag":"tn1","value":"tv1"}],"itemid":2,"name":"in2","clock":1712417868,"ns":425677241,"value":1.5,"type":3}`, par,
		&Rows{
			Rows: []Row{
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h1"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n1"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in1"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1,
					Timestamp: 1712417868425,
				},
				{
					Tags: []Tag{
						{
							Key:   []byte("host"),
							Value: []byte("h2"),
						},
						{
							Key:   []byte("hostname"),
							Value: []byte("n2"),
						},
						{
							Key:   []byte("__name__"),
							Value: []byte("in2"),
						},
						{
							Key:   []byte("tn1"),
							Value: []byte("tv1"),
						},
					},
					Value:     1.5,
					Timestamp: 1712417868425,
				},
			},
		})
}

func compareRows(rows, rowsExpected *Rows) error {
	if len(rows.Rows) != len(rowsExpected.Rows) {
		return fmt.Errorf("unexpected number of rows; got %d; want %d", len(rows.Rows), len(rowsExpected.Rows))
	}
	for i, row := range rows.Rows {
		rowExpected := rowsExpected.Rows[i]
		if err := compareSingleRow(&row, &rowExpected); err != nil {
			return fmt.Errorf("unexpected row at position #%d: %w", i, err)
		}
	}
	return nil
}

func compareSingleRow(row, rowExpected *Row) error {
	if !reflect.DeepEqual(row.Tags, rowExpected.Tags) {
		return fmt.Errorf("unexpected tags; got %q; want %q", row.Tags, rowExpected.Tags)
	}
	if row.Value != rowExpected.Value {
		return fmt.Errorf("unexpected values; got %v; want %v", row.Value, rowExpected.Value)
	}
	if row.Timestamp != rowExpected.Timestamp {
		return fmt.Errorf("unexpected values; got %v; want %v", row.Timestamp, rowExpected.Timestamp)
	}
	return nil
}
