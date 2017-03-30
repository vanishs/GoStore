package store

import (
	. "github.com/seewindcn/GoStore"
	_ "github.com/seewindcn/GoStore/cache"
	"encoding/json"
)


type Store struct {

}


func New() Store {
	return &Store{}
}


func Json2Map(sjson string)  (M, error) {
	var rs interface{}
	err := json.Unmarshal(sjson, &rs)
	if err != nil {
		return nil, err
	}
	return rs.(M), nil
}

