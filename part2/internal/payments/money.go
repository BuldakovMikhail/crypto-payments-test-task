package payments

import (
 "encoding/json"
 "errors"
 "fmt"
 "strconv"
 "strings"
)

// Amount is measured in micro-USDT. The API only accepts decimal strings.
type Amount int64

const MaxAmount Amount = 1_000_000_000_000_000 // 1 billion USDT per transfer

func ParseAmount(s string) (Amount, error) {
 parts := strings.Split(s, ".")
 if len(parts)>2 || parts[0]=="" { return 0, errors.New("invalid amount") }
 for _, p := range parts { for _, c := range p { if c<'0'||c>'9' { return 0,errors.New("invalid amount") } } }
 fraction := ""
 if len(parts)==2 { fraction=parts[1]; if len(fraction)==0 || len(fraction)>6 { return 0,errors.New("amount must have at most 6 decimals") } }
 whole,err:=strconv.ParseInt(parts[0],10,64)
 if err!=nil || whole>int64(MaxAmount)/1_000_000 { return 0,errors.New("amount too large") }
 micro,_:=strconv.ParseInt(fraction+strings.Repeat("0",6-len(fraction)),10,64)
 a:=Amount(whole*1_000_000+micro)
 if a>MaxAmount { return 0,errors.New("amount too large") }
 return a,nil
}
func (a Amount) String() string { return fmt.Sprintf("%d.%06d",a/1_000_000,a%1_000_000) }
func (a Amount) MarshalJSON() ([]byte,error) { return json.Marshal(a.String()) }
func (a *Amount) UnmarshalJSON(b []byte) error {
 var s string
 if err:=json.Unmarshal(b,&s); err!=nil { return errors.New("amount must be a decimal string") }
 v,err:=ParseAmount(s); if err==nil { *a=v }; return err
}
func Fee(a Amount,bps int,fixed Amount) Amount {
 // Division before multiplication avoids overflow even if limits change.
 fee:=a/10000*Amount(bps)+(a%10000)*Amount(bps)/10000+fixed
 if fee>a { return a }; return fee
}
