// Package inbatches allows fetching large datasets in smaller batches.
//
// Databases typically wait until they have collected the entire dataset
// before streaming the data to the client. This causes a large delay between
// starting the query and receiving the first result. Breaking the query into
// batches can help mitigate this.
package inbatches

import (
	"database/sql"
)

type Params struct {
	Limit, Offset int64
}

type Query func(params Params) (*sql.Rows, error)

// Rows provides a database/sql like interface for iterating over the rows of
// the batches.
type Rows struct {
	*sql.Rows

	query  Query
	params Params
	count  int64
	err    error
}

// Of initiates a query in batches of limit rows. The query must be ordered so
// that batches are consistent and can return all rows. query will be called
// once per batch. Batches stop when there is an error or when query returns
// less results than limit.
//
//     rows, err := inbatches.Of(1000, func(p Params) (*sql.Rows, error) {
//     	return db.Query("SELECT ... ORDER BY id OFFSET ? LIMIT ?", p.Offset, p.Limit)
//     })
//     ...
//     defer rows.Close()
//     for rows.Next() {
//     	var id int
//     	var name string
//     	err = rows.Scan(&id, &name)
//     	...
//     }
//     err = rows.Err()
//     ...
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
