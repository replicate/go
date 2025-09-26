package util

import "reflect"

// CopyFields copies any fields with the same name and type from one struct to another
func CopyFields[A any, B any](a *A, b *B) {
	val := reflect.Indirect(reflect.ValueOf(a))
	val2 := reflect.Indirect(reflect.ValueOf(b))
	for i := 0; i < val.Type().NumField(); i++ {
		name := val.Type().Field(i).Name
		field := val2.FieldByName(name)
		if !field.IsValid() {
			continue
		}
		if val.Type().Field(i).Type != field.Type() {
			continue
		}
		if !val.Type().Field(i).IsExported() {
			continue
		}
		field.Set(reflect.Indirect(reflect.ValueOf(a)).FieldByName(name))
	}
}
