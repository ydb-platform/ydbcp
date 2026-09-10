package db_test

import (
	"reflect"
	"strings"
	"testing"

	"ydbcp/internal/connectors/db"
)

// Protect the public boundary even if a new method/filter is added later.
func TestMetadataContractDoesNotExposeYDBTypes(t *testing.T) {
	visited := map[reflect.Type]bool{}
	var check func(reflect.Type)
	check = func(typ reflect.Type) {
		if visited[typ] {
			return
		}
		visited[typ] = true
		if strings.HasPrefix(typ.PkgPath(), "github.com/ydb-platform/ydb-go-") {
			t.Errorf("metadata contract exposes %s", typ)
		}
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
			check(typ.Elem())
		case reflect.Map:
			check(typ.Key())
			check(typ.Elem())
		case reflect.Func:
			for i := 0; i < typ.NumIn(); i++ {
				check(typ.In(i))
			}
			for i := 0; i < typ.NumOut(); i++ {
				check(typ.Out(i))
			}
		case reflect.Interface:
			for i := 0; i < typ.NumMethod(); i++ {
				check(typ.Method(i).Type)
			}
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				if f := typ.Field(i); f.IsExported() {
					check(f.Type)
				}
			}
		}
	}
	check(reflect.TypeOf((*db.DBConnector)(nil)).Elem())
}
