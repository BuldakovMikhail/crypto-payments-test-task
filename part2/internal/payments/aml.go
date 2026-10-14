package payments

import (
 "context"
 "database/sql"
 "errors"
 "strings"
)

type AML interface { Check(context.Context,string)(highRisk bool,err error) }
type MockAML struct { DB *sql.DB; blacklist map[string]bool }
func NewMockAML(db *sql.DB,addresses []string)*MockAML {
 m:=&MockAML{DB:db,blacklist:map[string]bool{}}
 for _,a:=range addresses {m.blacklist[strings.ToLower(strings.TrimSpace(a))]=true};return m
}
func (m *MockAML) Check(ctx context.Context,sender string)(bool,error) {
 var down bool
 if err:=m.DB.QueryRowContext(ctx,`SELECT aml_down FROM chain_state WHERE id=1`).Scan(&down);err!=nil{return false,err}
 if down{return false,errors.New("AML unavailable")};return m.blacklist[strings.ToLower(sender)],nil
}
