package message

type TxnDependencies struct {
	Deps []Transaction `json:"deps"`
}
