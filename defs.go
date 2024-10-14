package simplex

type Block interface{}

type Signature struct {
	Signer []byte
	Value  []byte
	Msg    []byte
}

type WAL interface {
	MarkVoted(Signature) error
}
