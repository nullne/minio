package cmd

//
// import "errors"
//
// type Map struct {
// 	keysA   map[string]string
// 	keysB   map[string]string
// 	valuesA map[string]map[string]string
// 	valuesB map[string]map[string]string
// }
//
// func NewMap(keys []string, values [][]string) (Map, error) {
// 	if len(values) != len(keys) {
// 		return nil, errors.New("length of keys and values should be the same")
// 	}
// 	m := Map{
// 		keysA:   make(map[string]string, len(keys)),
// 		keysB:   make(map[string]string, len(keys)),
// 		valuesA: make(map[string]map[string]string, len(keys)),
// 		valuesB: make(map[string]map[string]string, len(keys)),
// 	}
// 	for i, key := range keys {
// 		if i > 255 || len(key) == 1 {
// 			return nil, errors.New("number should NOT be greater than 255 and value length should not be 1")
// 		}
// 		s = string(byte(i))
// 		keysA[s] = key
// 		keysB[key] = s
//
// 		value := values[i]
// 		if value == nil {
// 			continue
// 		}
// 		va = make(map[string]string, len(value))
// 		vb = make(map[string]string, len(value))
// 		for j, k := range value {
// 			// if j >
// 		}
// 	}
// }
//
// func (m Map) Slim(m1 map[string]string) map[string]string {
// 	m2 := make(map[string]string, len(m1))
// 	for k1, v1 := range m1 {
// 		l, ok := m.a[k1]
// 		if !ok {
// 			l = []string{}
// 		}
//
// 	}
// 	return m2
// }
//
// func (m Map) Fat(m1 map[string]string) map[string]string {
// 	m2 := make(map[string]string, len(m1))
// 	return m2
// }
