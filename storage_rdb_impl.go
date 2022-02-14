package gosearchengine

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func NewDBClient(dbConfig *DBConfig) (*sqlx.DB, error) {
	db, err := sqlx.Open(
		"mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbConfig.User, dbConfig.Password, dbConfig.Addr, dbConfig.Port, dbConfig.DB),
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}

type StorageRdbImpl struct {
	DB *sqlx.DB
}

func NewStorageRdbImpl(db *sqlx.DB) StorageRdbImpl {
	return StorageRdbImpl{
		DB: db,
	}
}

type DBConfig struct {
	User     string
	Password string
	Addr     string
	Port     string
	DB       string
}

func (s StorageRdbImpl) AddDocument(doc Document) (DocumentID, error) {
	res, err := s.DB.NamedExec(`insert into documents (body, token_count) values (:body, :token_count)`,
		map[string]interface{}{
			"body":        doc.Body,
			"token_count": doc.TokenCount,
		})

	if err != nil {
		return 0, err
	}

	insertedID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return DocumentID(insertedID), err
}

func (s StorageRdbImpl) UpsertInvertedIndex(inverted InvertedIndex) error {
	encoded, err := encode(inverted)
	if err != nil {
		return err
	}

	for _, v := range encoded {
		_, err := s.DB.NamedExec(
			`insert into inverted_indexes (token_id, posting_list)
			values (:token_id, :posting_list)
			on duplicate key update posting_list = :posting_list`, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func encode(invertedIndex InvertedIndex) ([]EncodedInvertedIndex, error)

type EncodedInvertedIndex struct {
	TokenID     TokenID `db:"token_id"`
	PostingList []byte  `db:"posting_list"`
}
