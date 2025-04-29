package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyFields_Normal(t *testing.T) {
	type a struct {
		Thing        int
		AnotherThing string
		OneMoreThing bool
	}

	type b struct {
		Thing        int
		AnotherThing string
	}

	thing1 := &a{
		Thing:        123,
		AnotherThing: "abc",
		OneMoreThing: false,
	}

	thing2 := &b{}

	CopyFields(thing1, thing2)
	assert.Equal(t, thing2.Thing, 123)
	assert.Equal(t, thing2.AnotherThing, "abc")
}

func TestCopyFields_Unexported(t *testing.T) {
	type a struct {
		thing        int
		anotherThing string
		oneMoreThing bool
	}

	type b struct {
		thing        int
		anotherThing string
	}

	thing1 := &a{
		thing:        123,
		anotherThing: "abc",
		oneMoreThing: false,
	}

	thing2 := &b{}

	CopyFields(thing1, thing2)
	assert.Equal(t, thing2.thing, 0)
	assert.Equal(t, thing2.anotherThing, "")
}

func TestCopyFields_DifferentType(t *testing.T) {
	type a struct {
		Thing        int
		AnotherThing string
		OneMoreThing bool
	}

	type b struct {
		Thing        int
		AnotherThing string
		OneMoreThing string
	}

	thing1 := &a{
		Thing:        123,
		AnotherThing: "abc",
		OneMoreThing: false,
	}

	thing2 := &b{}

	CopyFields(thing1, thing2)
	assert.Equal(t, thing2.Thing, 123)
	assert.Equal(t, thing2.AnotherThing, "abc")
	assert.Equal(t, thing2.OneMoreThing, "")
}

func TestCopyFields_UnexportedReceiver(t *testing.T) {
	type a struct {
		Thing        int
		AnotherThing string
		OneMoreThing bool
	}

	type b struct {
		thing        int
		anotherThing string
	}

	thing1 := &a{
		Thing:        123,
		AnotherThing: "abc",
		OneMoreThing: false,
	}

	thing2 := &b{}

	CopyFields(thing1, thing2)
	assert.Equal(t, thing2.thing, 0)
	assert.Equal(t, thing2.anotherThing, "")
}

func TestCopyFields_RefField(t *testing.T) {
	type a struct {
		Thing        map[string]string
		AnotherThing string
	}

	type b struct {
		Thing map[string]string
	}

	thing1 := &a{
		Thing: map[string]string{
			"a": "b",
			"c": "d",
		},
	}

	thing2 := &b{}

	CopyFields(thing1, thing2)
	assert.Equal(t, thing2.Thing, map[string]string{
		"a": "b",
		"c": "d",
	})
}

func TestCopyFields_Func(t *testing.T) {
	type a struct {
		Thing        func(int) string
		AnotherThing int
	}

	type b struct {
		Thing        func(int) string
		AnotherThing string
	}

	thing1 := &a{
		Thing: func(i int) string {
			return fmt.Sprintf("hello %d", i)
		},
	}

	thing2 := &b{}

	CopyFields(thing1, thing2)
	assert.Equal(t, thing2.Thing(123), "hello 123")
	assert.Equal(t, thing2.AnotherThing, "")
}
