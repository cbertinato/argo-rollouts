package honeycomb

import "context"

type mockAPI struct {
	response *QueryResult
	err      error
}

func (m *mockAPI) CreateQuery(ctx context.Context, query string, dataset string) (*Query, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &Query{}, nil
}

func (m *mockAPI) GetQueryResult(ctx context.Context, queryID string, dataset string) (*QueryResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}
