package toolkit

import (
	"reflect"
	"errors"
	"fbs.com/ps/structs"
	"strconv"
	"fmt"
	"strings"
	"unicode"
)

func LoadConfigSettings(obj interface{}, settings structs.Settings, useSandbox bool) (err error) {
	for k, v := range settings.Common {
		// Try to set config field and panic if any errors
		err = SetField(obj, k.(string), v)
		if err != nil {
			return
		}
	}
	if useSandbox {
		for k, v := range settings.Sandbox {
			// Try to set config field and panic if any errors
			err = SetField(obj, k.(string), v)
			if err != nil {
				return
			}
		}
	} else {
		for k, v := range settings.Production {
			// Try to set config field and panic if any errors
			err = SetField(obj, k.(string), v)
			if err != nil {
				return
			}
		}
	}
	return
}

func SetField(obj interface{}, name string, value interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return errors.New("No such field:" + name + " in obj")
	}

	if !structFieldValue.CanSet() {
		return errors.New("Cannot set " + name + " field value")
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return errors.New("Provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}

func MoneyToCoins(value string) (amount int, err error) {
	var f float64

	f, err = strconv.ParseFloat(value, 64)
	if err != nil {
		return
	}
	amount, err = strconv.Atoi(fmt.Sprintf("%.0f", f * 100))
	if err != nil {
		return
	}

	return
}


func RemoveSpaces(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}


func StringInSlice (a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}