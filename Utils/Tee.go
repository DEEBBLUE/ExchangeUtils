package tee

import "sync"

func CreateTee[T any](mainCh chan T, amount int) ([]chan T,error){
	list := make([]chan T,amount)
	for i := range amount{
		list[i] = make(chan T)
	}
	go func ()  {
		for i := range amount {
			defer close(list[i])
		}
		for val := range mainCh{
			wg := &sync.WaitGroup{}
			for i := range amount {
				wg.Add(1)
				go func ()  {
					list[i] <- val
					wg.Done()
				}()
			}		
			wg.Wait()
		}	
	}()
	return list,nil
}
