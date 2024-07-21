package filtersquirrel

// FieldMapperFunc is a function to map domain object field names to database table columns.
type FieldMapperFunc func(fieldName string) (string, error)

func FieldAsIsMapperFunc(fieldName string) (string, error) {
	return fieldName, nil
}

type Options struct {
	MapperFunc FieldMapperFunc
}

type Option func(o *Options)

func DefaultOptions() *Options {
	return &Options{
		MapperFunc: FieldAsIsMapperFunc,
	}
}

func FromDefaultOptions(opts ...Option) *Options {
	o := DefaultOptions()
	for _, applyOption := range opts {
		applyOption(o)
	}
	return o
}

func WithMapperFunc(f FieldMapperFunc) Option {
	return func(o *Options) {
		if f == nil {
			return
		}
		o.MapperFunc = f
	}
}
