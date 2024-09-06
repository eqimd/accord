package message

import "github.com/eqimd/accord/common"

type TxnDependencies struct {
	Deps common.Set[Transaction]
}
