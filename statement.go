package teorm

import "strings"

type Statement struct {
	Table       string
	Model       interface{}
	Selects     []string
	Conditions  []string
	Args        []interface{}
	LimitVal    int
	OffsetVal   int
	Order       string
	Group       string
}

func (s *Statement) Clone() *Statement {
	newStmt := *s
	// Copy slices to avoid sharing backing arrays
	newStmt.Selects = make([]string, len(s.Selects))
	copy(newStmt.Selects, s.Selects)
	
	newStmt.Conditions = make([]string, len(s.Conditions))
	copy(newStmt.Conditions, s.Conditions)
	
	newStmt.Args = make([]interface{}, len(s.Args))
	copy(newStmt.Args, s.Args)
	
	return &newStmt
}

func (s *Statement) BuildCondition() (string, []interface{}) {
	if len(s.Conditions) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(s.Conditions, " AND "), s.Args
}
