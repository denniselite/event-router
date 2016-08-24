package structs

import "encoding/json"

type Response struct {
    Error error
    Message string
}

func (res Response) GetJson() (data []byte) {
    data, _ = json.Marshal(res)
    return
}