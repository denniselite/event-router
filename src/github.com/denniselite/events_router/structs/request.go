package structs

const (
)

type InputMessage struct {
    Route string `validate:"nonzero"`
    Data string `validate:"nonzero"`
}
