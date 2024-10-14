package simplex

type Signer interface {
	Sign([]byte) ([]byte, error)
}

type Sender interface {
	Send(msg, to []byte)
}
