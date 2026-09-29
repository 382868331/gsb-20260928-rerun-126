package logengine

import "encoding/json"

func jsonMarshal(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}

func jsonUnmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
