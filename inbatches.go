package inbatches

import (
	"database/sql"
)

type Params struct {
	Limit, Offset int64
}

type Query func(params Params) (*sql.Rows, error)

type Rows struct {
	*sql.Rows

	query  Query
	params Params
	count  int64
	err    error
}

func Of(limit int64, query Query) (*Rows, error) {
	r := &Rows{
		query:  query,
		params: Params{Limit: limit},
	}
	r.Rows, r.err = query(r.params)
	return r, r.err
}

func (r *Rows) Next() bool {
	if r.err != nil {
		return false
	}
	next := r.Rows.Next()
	if !next && r.Err() == nil {
		return r.nextBatch()
	}
	r.count++
	return next
}

func (r *Rows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Rows.Err()
}

func (r *Rows) nextBatch() bool {
	r.err = r.Close()
	if r.err != nil || r.done() {
		return false
	}
	r.count = 0
	r.params.Offset += r.params.Limit
	r.Rows, r.err = r.query(r.params)
	if r.err != nil {
		return false
	}
	next := r.Rows.Next()
	return next
}

func (r *Rows) done() bool {
	return r.count < r.params.Limit
}
