package rabbitmq

type Func func(attempt int) (retry bool, err error)


func Do(fn Func) error {
	var err error
	var cont bool
	attempt := 0
	for {
		cont, err = fn(attempt)
		attempt++
		if !cont || err == nil {
			break
		}
	}
	return err
}
