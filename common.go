package GoStore

import "encoding/json"

type M map[string]interface{}


func Json2Map(sjson string)  (M, error) {
	var rs interface{}
	err := json.Unmarshal([]byte(sjson), &rs)
	if err != nil {
		return nil, err
	}
	return rs.(M), nil
}

