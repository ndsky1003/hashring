package hashring

func Options[T any]() *Option[T] {
	return &Option[T]{}
}

type Option[T any] struct {
	replica_count *int
	string_func   func(T) string
}

func (this *Option[T]) SetReplicaCount(replicaCount int) *Option[T] {
	this.replica_count = &replicaCount
	return this
}

func (this *Option[T]) SetStringFunc(stringFunc func(T) string) *Option[T] {
	this.string_func = stringFunc
	return this
}

func (this *Option[T]) merge(delta *Option[T]) *Option[T] {
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

func (this *Option[T]) merges(opts ...*Option[T]) *Option[T] {
	for _, opt := range opts {
		this.merge(opt)
	}
	return this
}
