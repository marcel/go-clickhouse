package chschema

import (
	"fmt"
	"math"
	"math/big"
	"reflect"

	"github.com/uptrace/go-clickhouse/ch/chproto"
)

// DecimalColumn handles Decimal32, Decimal64, Decimal128, and Decimal256 types.
// All decimal values are mapped to Go float64 type.
type DecimalColumn struct {
	ColumnOf[float64]
	size      int // 32, 64, 128, or 256 bits
	precision int
	scale     int
	divisor   float64 // 10^scale, cached for performance
}

var _ Columnar = (*DecimalColumn)(nil)

func NewDecimalColumn() Columnar {
	return new(DecimalColumn)
}

func (c *DecimalColumn) Init(chType string) error {
	size, precision, scale, err := parseDecimalParams(chType)
	if err != nil {
		return err
	}
	c.size = size
	c.precision = precision
	c.scale = scale
	c.divisor = math.Pow10(scale)
	return nil
}

func (c *DecimalColumn) Type() reflect.Type {
	return float64Type
}

func (c *DecimalColumn) AppendValue(v reflect.Value) {
	c.Column = append(c.Column, v.Float())
}

func (c *DecimalColumn) ReadFrom(rd *chproto.Reader, numRow int) error {
	if numRow == 0 {
		return nil
	}
	c.AllocForReading(numRow)

	for i := range c.Column {
		val, err := c.readDecimalValue(rd)
		if err != nil {
			return err
		}
		c.Column[i] = val
	}
	return nil
}

func (c *DecimalColumn) readDecimalValue(rd *chproto.Reader) (float64, error) {
	switch c.size {
	case 32:
		n, err := rd.Int32()
		if err != nil {
			return 0, err
		}
		return float64(n) / c.divisor, nil
	case 64:
		n, err := rd.Int64()
		if err != nil {
			return 0, err
		}
		return float64(n) / c.divisor, nil
	case 128:
		b, err := rd.Int128()
		if err != nil {
			return 0, err
		}
		return c.int128ToFloat64(b), nil
	case 256:
		b, err := rd.Int256()
		if err != nil {
			return 0, err
		}
		return c.int256ToFloat64(b), nil
	default:
		return 0, fmt.Errorf("unsupported decimal size: %d", c.size)
	}
}

func (c *DecimalColumn) WriteTo(wr *chproto.Writer) error {
	for _, val := range c.Column {
		if err := c.writeDecimalValue(wr, val); err != nil {
			return err
		}
	}
	return nil
}

func (c *DecimalColumn) writeDecimalValue(wr *chproto.Writer, val float64) error {
	switch c.size {
	case 32:
		n := int32(math.Round(val * c.divisor))
		wr.Int32(n)
	case 64:
		n := int64(math.Round(val * c.divisor))
		wr.Int64(n)
	case 128:
		wr.Int128(c.float64ToInt128(val))
	case 256:
		wr.Int256(c.float64ToInt256(val))
	default:
		return fmt.Errorf("unsupported decimal size: %d", c.size)
	}
	return nil
}

// int128ToFloat64 converts a little-endian signed 128-bit integer to float64.
func (c *DecimalColumn) int128ToFloat64(b [16]byte) float64 {
	// Check sign bit (MSB of last byte in little-endian)
	isNegative := b[15]&0x80 != 0

	// Convert to big.Int (big.Int expects big-endian bytes)
	bigEndian := make([]byte, 16)
	for i := 0; i < 16; i++ {
		bigEndian[i] = b[15-i]
	}

	bigInt := new(big.Int)
	if isNegative {
		// Two's complement for negative numbers
		// Invert all bits
		for i := range bigEndian {
			bigEndian[i] = ^bigEndian[i]
		}
		bigInt.SetBytes(bigEndian)
		bigInt.Add(bigInt, big.NewInt(1))
		bigInt.Neg(bigInt)
	} else {
		bigInt.SetBytes(bigEndian)
	}

	// Convert to float and divide by scale
	bigFloat := new(big.Float).SetInt(bigInt)
	bigFloat.Quo(bigFloat, new(big.Float).SetFloat64(c.divisor))
	result, _ := bigFloat.Float64()
	return result
}

// int256ToFloat64 converts a little-endian signed 256-bit integer to float64.
func (c *DecimalColumn) int256ToFloat64(b [32]byte) float64 {
	// Check sign bit (MSB of last byte in little-endian)
	isNegative := b[31]&0x80 != 0

	// Convert to big.Int (big.Int expects big-endian bytes)
	bigEndian := make([]byte, 32)
	for i := 0; i < 32; i++ {
		bigEndian[i] = b[31-i]
	}

	bigInt := new(big.Int)
	if isNegative {
		// Two's complement for negative numbers
		for i := range bigEndian {
			bigEndian[i] = ^bigEndian[i]
		}
		bigInt.SetBytes(bigEndian)
		bigInt.Add(bigInt, big.NewInt(1))
		bigInt.Neg(bigInt)
	} else {
		bigInt.SetBytes(bigEndian)
	}

	// Convert to float and divide by scale
	bigFloat := new(big.Float).SetInt(bigInt)
	bigFloat.Quo(bigFloat, new(big.Float).SetFloat64(c.divisor))
	result, _ := bigFloat.Float64()
	return result
}

// float64ToInt128 converts a float64 to a little-endian signed 128-bit integer.
func (c *DecimalColumn) float64ToInt128(val float64) [16]byte {
	var result [16]byte

	// Multiply by scale and round
	scaled := new(big.Float).SetFloat64(val * c.divisor)

	// Convert to big.Int
	bigInt, _ := scaled.Int(nil)

	// Handle negative numbers
	isNegative := bigInt.Sign() < 0
	if isNegative {
		bigInt.Neg(bigInt)
	}

	// Get bytes (big-endian)
	bytes := bigInt.Bytes()

	// Copy to result in little-endian format
	for i := 0; i < len(bytes) && i < 16; i++ {
		result[i] = bytes[len(bytes)-1-i]
	}

	// Apply two's complement for negative numbers
	if isNegative {
		// Invert all bits
		for i := range result {
			result[i] = ^result[i]
		}
		// Add 1
		carry := uint16(1)
		for i := 0; i < 16; i++ {
			sum := uint16(result[i]) + carry
			result[i] = byte(sum)
			carry = sum >> 8
		}
	}

	return result
}

// float64ToInt256 converts a float64 to a little-endian signed 256-bit integer.
func (c *DecimalColumn) float64ToInt256(val float64) [32]byte {
	var result [32]byte

	// Multiply by scale and round
	scaled := new(big.Float).SetFloat64(val * c.divisor)

	// Convert to big.Int
	bigInt, _ := scaled.Int(nil)

	// Handle negative numbers
	isNegative := bigInt.Sign() < 0
	if isNegative {
		bigInt.Neg(bigInt)
	}

	// Get bytes (big-endian)
	bytes := bigInt.Bytes()

	// Copy to result in little-endian format
	for i := 0; i < len(bytes) && i < 32; i++ {
		result[i] = bytes[len(bytes)-1-i]
	}

	// Apply two's complement for negative numbers
	if isNegative {
		// Invert all bits
		for i := range result {
			result[i] = ^result[i]
		}
		// Add 1
		carry := uint16(1)
		for i := 0; i < 32; i++ {
			sum := uint16(result[i]) + carry
			result[i] = byte(sum)
			carry = sum >> 8
		}
	}

	return result
}

//------------------------------------------------------------------------------

// ArrayDecimalColumn handles Array(Decimal(...)) types.
type ArrayDecimalColumn struct {
	ColumnOf[[]float64]
	elem DecimalColumn
}

var (
	_ Columnar      = (*ArrayDecimalColumn)(nil)
	_ ArrayColumnar = (*ArrayDecimalColumn)(nil)
)

func NewArrayDecimalColumn() Columnar {
	return new(ArrayDecimalColumn)
}

func (c *ArrayDecimalColumn) Init(chType string) error {
	return c.elem.Init(chArrayElemType(chType))
}

func (c *ArrayDecimalColumn) Type() reflect.Type {
	return reflect.TypeOf((*[]float64)(nil)).Elem()
}

func (c *ArrayDecimalColumn) ReadFrom(rd *chproto.Reader, numRow int) error {
	if numRow == 0 {
		return nil
	}

	c.AllocForReading(numRow)

	offsets, err := c.readOffsets(rd, numRow)
	if err != nil {
		return err
	}

	// Each block gets its own element buffer: a later block reuses this column,
	// and the rows already scanned keep slices of the buffer.
	c.elem.Column = nil
	if err := c.elem.ReadFrom(rd, offsets[len(offsets)-1]); err != nil {
		return err
	}

	var prev int
	for i, offset := range offsets {
		c.Column[i] = c.elem.Column[prev:offset:offset]
		prev = offset
	}

	return nil
}

func (c *ArrayDecimalColumn) readOffsets(rd *chproto.Reader, numRow int) ([]int, error) {
	offsets := make([]int, numRow)
	for i := range offsets {
		offset, err := rd.UInt64()
		if err != nil {
			return nil, err
		}
		offsets[i] = int(offset)
	}
	return offsets, nil
}

func (c *ArrayDecimalColumn) WriteTo(wr *chproto.Writer) error {
	_ = c.WriteOffset(wr, 0)
	return c.WriteData(wr)
}

func (c *ArrayDecimalColumn) WriteOffset(wr *chproto.Writer, offset int) int {
	for _, el := range c.Column {
		offset += len(el)
		wr.UInt64(uint64(offset))
	}
	return offset
}

func (c *ArrayDecimalColumn) WriteData(wr *chproto.Writer) error {
	for _, ss := range c.Column {
		c.elem.Column = ss
		if err := c.elem.WriteTo(wr); err != nil {
			return err
		}
	}
	return nil
}
