package handlers

import (
	"sync"
)

type RunnableHandler interface {
	Async(wg *sync.WaitGroup) error
	Run() error
	Status() bool
	Stop() error
}
