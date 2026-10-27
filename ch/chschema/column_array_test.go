package chschema

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uptrace/go-clickhouse/ch/chproto"
)

// A query decodes every block into the same Column, as Block.Column caches
// it by name. These tests decode two blocks into one column and check that
// the rows scanned from the first block still hold their own values.
type ColumnBlockReuseTestSuite struct {
	suite.Suite
}

func TestColumnBlockReuseTestSuite(t *testing.T) {
	suite.Run(t, new(ColumnBlockReuseTestSuite))
}

func u32(n uint32) *uint32 { return &n }

type blockReuseCase struct {
	Scenario      string
	CHType        string
	Typ           reflect.Type
	First, Second []any
}

func blockReuseCases() []blockReuseCase {
	return []blockReuseCase{
		{
			"Array(UInt32)", "Array(UInt32)", nil,
			[]any{[]uint32{140, 141}, []uint32{7}, []uint32{1, 2, 3}},
			[]any{[]uint32{124, 758}, []uint32{9}, []uint32{4, 5, 6}},
		},
		{
			"Array(Int64)", "Array(Int64)", nil,
			[]any{[]int64{-1, 2}, []int64{3, 4}},
			[]any{[]int64{50, 60}, []int64{70, 80}},
		},
		{
			"Array(String)", "Array(String)", nil,
			[]any{[]string{"u1", "u2"}, []string{"u3"}},
			[]any{[]string{"x1", "x2"}, []string{"x3"}},
		},
		{
			"Array(LowCardinality(String))", "Array(LowCardinality(String))", nil,
			[]any{[]string{"a", "b"}, []string{"c"}},
			[]any{[]string{"d", "e"}, []string{"f"}},
		},
		{
			"Array(Decimal(18, 4))", "Array(Decimal(18, 4))", nil,
			[]any{[]float64{1.25, 2.5}, []float64{3.75}},
			[]any{[]float64{9.5, 8.25}, []float64{7.125}},
		},
		{
			"Array(Array(Int64))", "Array(Array(Int64))", nil,
			[]any{[][]int64{{1, 2}, {3}}, [][]int64{{4}}},
			[]any{[][]int64{{9, 8}, {7}}, [][]int64{{6}}},
		},
	}
}

func (s *ColumnBlockReuseTestSuite) TestRowsKeepTheirBlocksValues() {
	for _, tc := range blockReuseCases() {
		s.Run(tc.Scenario, func() {
			rd := tc.reader(s.T(), tc.First, tc.Second)
			col := NewColumn(tc.CHType, tc.Typ)

			first := s.decodeRows(col, rd, len(tc.First))
			second := s.decodeRows(col, rd, len(tc.Second))

			for i, want := range tc.First {
				s.Equal(want, first[i].Interface(), "first block row %d", i)
			}
			for i, want := range tc.Second {
				s.Equal(want, second[i].Interface(), "second block row %d", i)
			}
		})
	}
}

func (s *ColumnBlockReuseTestSuite) TestAppendingToARowLeavesTheNextRowAlone() {
	for _, tc := range blockReuseCases() {
		s.Run(tc.Scenario, func() {
			rd := tc.reader(s.T(), tc.First)
			rows := s.decodeRows(NewColumn(tc.CHType, tc.Typ), rd, len(tc.First))

			first := rows[0]
			first.Set(reflect.Append(first, reflect.Zero(first.Type().Elem())))

			s.Equal(tc.First[1], rows[1].Interface())
		})
	}
}

func (s *ColumnBlockReuseTestSuite) TestNullableColumnarValueKeepsItsBlock() {
	tc := blockReuseCase{
		CHType: "Nullable(UInt32)",
		Typ:    reflect.TypeOf((*uint32)(nil)),
		First:  []any{u32(1), (*uint32)(nil), u32(3)},
		Second: []any{u32(7), u32(8), (*uint32)(nil)},
	}
	rd := tc.reader(s.T(), tc.First, tc.Second)
	col := NewColumn(tc.CHType, tc.Typ)

	s.Require().NoError(col.ReadFrom(rd, len(tc.First)))
	values := col.Value().([]*uint32)
	s.Require().NoError(col.ReadFrom(rd, len(tc.Second)))

	s.Equal([]*uint32{u32(1), nil, u32(3)}, values)
}

// decodeRows reads one block into col and scans each row into its own
// value, as the row-oriented scan models do.
func (s *ColumnBlockReuseTestSuite) decodeRows(col Columnar, rd *chproto.Reader, numRow int) []reflect.Value {
	s.Require().NoError(col.ReadFrom(rd, numRow))
	rows := make([]reflect.Value, numRow)
	for i := range rows {
		rows[i] = reflect.New(col.Type()).Elem()
		s.Require().NoError(col.ConvertAssign(i, rows[i]))
	}
	return rows
}

// reader serves each block of rows, encoded as the case's column type, in
// order on one stream.
func (tc blockReuseCase) reader(t *testing.T, blocks ...[]any) *chproto.Reader {
	var buf bytes.Buffer
	wr := chproto.NewWriter(&buf)
	for _, rows := range blocks {
		col := NewColumn(tc.CHType, tc.Typ)
		col.ResetForWriting(len(rows))
		for _, row := range rows {
			col.AppendValue(reflect.ValueOf(row))
		}
		if err := col.WriteTo(wr); err != nil {
			t.Fatal(err)
		}
	}
	if err := wr.Flush(); err != nil {
		t.Fatal(err)
	}
	return chproto.NewReader(&buf)
}
