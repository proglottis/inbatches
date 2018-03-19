package inbatches

import (
	"database/sql"
	"testing"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

type testData struct {
	ID     int
	Number int
}

var data = []testData{
	{ID: 1, Number: 0},
	{ID: 2, Number: 1},
	{ID: 3, Number: 1},
	{ID: 4, Number: 2},
	{ID: 5, Number: 3},
	{ID: 6, Number: 5},
}

func TestIn_success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal("mock:", err)
	}
	defer db.Close()

	mockRows := sqlmock.NewRows([]string{"id", "number"})
	for _, d := range data[:5] {
		mockRows.AddRow(d.ID, d.Number)
	}
	mock.ExpectQuery("SELECT \\* FROM data OFFSET \\? LIMIT \\?").WithArgs(0, 5).WillReturnRows(mockRows)
	mockRows = sqlmock.NewRows([]string{"id", "number"})
	for _, d := range data[5:] {
		mockRows.AddRow(d.ID, d.Number)
	}
	mock.ExpectQuery("SELECT \\* FROM data OFFSET \\? LIMIT \\?").WithArgs(5, 5).WillReturnRows(mockRows)

	rows, err := Of(5, func(p Params) (*sql.Rows, error) {
		return db.Query("SELECT * FROM data OFFSET ? LIMIT ?", p.Offset, p.Limit)
	})
	if err != nil {
		t.Fatal("of:", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, number int
		err := rows.Scan(&id, &number)
		if err != nil {
			t.Fatal("scan:", err)
		}
	}
	if rows.Err() != nil {
		t.Fatal("err:", rows.Err())
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("unmet expectation error:", rows.Err())
	}
}
