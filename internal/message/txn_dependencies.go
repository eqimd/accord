package message

import "github.com/eqimd/accord/internal/common"

type TxnDependencies struct {
	Deps common.Set[Transaction]
}
