// Account, AccountState
// Some basic operation about accountState

package core

import (
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/json"
	"log"
	"math/big"
)

type Account struct {
	AcAddress utils.Address
	PublicKey []byte
}

// AccoutState record the details of an account, it will be saved in status trie
type AccountState struct {
	AcAddress   utils.Address // this part is not useful, abort
	Nonce       uint64
	Balance     *big.Int
	StorageRoot []byte // only for smart contract account
	CodeHash    []byte // only for smart contract account
}

// Reduce the balance of an account
func (as *AccountState) Deduct(val *big.Int) bool {
	if as.Balance.Cmp(val) < 0 {
		return false
	}
	as.Balance.Sub(as.Balance, val)
	return true
}

// Increase the balance of an account
func (s *AccountState) Deposit(value *big.Int) {
	s.Balance.Add(s.Balance, value)
}

// Encode AccountState in order to store in the MPT
func (as *AccountState) Encode() []byte {
	// State-trie values must not depend on process-local gob type IDs.
	b, err := json.Marshal(as)
	if err != nil {
		log.Panic(err)
	}
	return b
}

// Decode AccountState
func DecodeAS(b []byte) *AccountState {
	var as AccountState

	err := json.Unmarshal(b, &as)
	if err != nil {
		log.Panic(err)
	}
	return &as
}

// Hash AccountState for computing the MPT Root
func (as *AccountState) Hash() []byte {
	h := sha256.Sum256(as.Encode())
	return h[:]
}
