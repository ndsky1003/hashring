package hashring

func Options[T any]() *options[T] {
	return &options[T]{}
}

type options[T any] struct {
	replica_count *int
	string_func   func(T) string
}

func (this *options[T]) SetReplicaCount(replicaCount int) *options[T] {
	this.replica_count = &replicaCount
	return this
}

func (this *options[T]) SetStringFunc(stringFunc func(T) string) *options[T] {
	this.string_func = stringFunc
	return this
}

func (this *options[T]) merge(delta *options[T]) *options[T] {
	if delta == nil {
		return this
	}
	if delta.replica_count != nil {
		this.replica_count = delta.replica_count
	}
	if delta.string_func != nil {
		this.string_func = delta.string_func
	}
	return this
}

func (this *options[T]) merges(opts ...*options[T]) *options[T] {
	for _, opt := range opts {
		this.merge(opt)
	}
	return this
}
