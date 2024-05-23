package pkg

type WorkerPool struct {
	worker chan struct{}
}

func InitWp(limit int) WorkerPool {

	return WorkerPool{
		worker: make(chan struct{}, limit),
	}

}

func (wp WorkerPool) AddWorker(worker func()) {

	wp.worker <- struct{}{}
	go func() {
		worker()
		<-wp.worker
	}()

}
