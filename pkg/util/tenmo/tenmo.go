package tenmo

import (
	"io"
	"os"
	"encoding/json"
	"strconv"

	"time"
	"math/rand"
	"database/sql"

	"fmt"

	_ "github.com/lib/pq"
	ulid "github.com/oklog/ulid"
)


var DbClient *sql.DB

var (
	host     = ""
	port     = 5432
	user     = "tenmo"
	password = "tenmo"
	dbname   = "tenmo"
)

type ExecutionId string
type IncarnationId string
type ProcessId string
type EntityId string
type OperationId string

type event interface {
	eventType() string
}

const (
	None = "";
	OpRead = "r";
	OpWrite = "w";
)

func ExecIdRand(eid string) ExecutionId {
	return ExecutionId(fmt.Sprintf("ex://%s-%s", eid, newULID()))
}

func IncId(id string) IncarnationId {
	return IncarnationId(fmt.Sprintf("i://%s", id))
}

func IncIdUlid(id string) IncarnationId {
	return IncarnationId(fmt.Sprintf("i://%s?ulid=%s", id, newULID()))
}

func IncIdGeneration(id string, generation int64) IncarnationId {
	return IncarnationId(fmt.Sprintf("i://%s?gen=%d", id, generation))
}

func IdsEntIncUlid(id string) (EntityId, IncarnationId) {
	return EntId(id), IncIdUlid(id)
}

func IdsEntIncGeneration(id string, generation int64) (EntityId, IncarnationId) {
	return EntId(id), IncIdGeneration(id, generation)
}

func EntId(id string) EntityId {
	return EntityId(fmt.Sprintf("en://%s", id))
}

type Execution struct {
	ExecutionId ExecutionId;
	ParentId ExecutionId;
	CreatorId ExecutionId;
	ProcessId ProcessId;
	Description string;
}

type eventExecutionBeginsJson struct {
	EventUlid ulid.ULID `json:"event_ulid"`;
	Timestamp time.Time `json:"timestamp"`;
	ExecutionId string `json:"execution_id"`;
	ParentId string `json:"parent_id,omitempty"`;
	CreatorId string `json:"creator_id,omitempty"`;
	ProcessId string `json:"process_id,omitempty"`;
	Description string `json:"description,omitempty"`;
}

func (eb eventExecutionBeginsJson) eventType() string {
	return "EventExecutionBegins"
}


type eventExecutionEndsJson struct {
	EventUlid ulid.ULID `json:"event_ulid"`;
	Timestamp time.Time `json:"timestamp"`;
	ExecutionId string `json:"execution_id"`;
}

func (eb eventExecutionEndsJson) eventType() string {
	return "EventExecutionEnds"
}


type Operation struct {
	ExecutionId ExecutionId;
	OperationType string;
	EntityId EntityId;
	IncarnationId IncarnationId;
	EntityDescription string;
	IncarnationDescription string;
}

type eventOperationJson struct {
	EventUlid ulid.ULID `json:"event_ulid"`;
	OperationId string `json:"operation_id"`;
	Timestamp time.Time `json:"timestamp"`;
	ExecutionId string `json:"execution_id"`;
	OperationType string `json:"type"`;
	EntityId string `json:"entity_id"`;
	IncarnationId string `json:"incarnation_id"`;
	EntityDescription string `json:"entity_description,omitempty"`;
	IncarnationDescription string `json:"incarnation_description,omitempty"`;
}

func (eb eventOperationJson) eventType() string {
	return "EventOperation"
}

var entropy io.Reader;

func init() {
	println("Tenmo initializaing...")
	host = os.Getenv("TENMO_HOST")
	if host == "" {
		println("Tenmo is not enabled.")
		return
	}
	if portEnv, ok := os.LookupEnv("TENMO_PORT"); ok {
		i, err := strconv.Atoi(portEnv)
		if err != nil {
			port = i
		}
	}
	if userEnv, ok := os.LookupEnv("TENMO_USER"); ok {
		user = userEnv
	}
	if passwordEnv, ok := os.LookupEnv("TENMO_PASSWORD"); ok {
		password = passwordEnv
	}
	if dbnameEnv, ok := os.LookupEnv("TENMO_DBNAME"); ok {
		dbname = dbnameEnv
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	t := time.Now()
	entropy = ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	println("Tenmo connected!")
	DbClient = db

	// defer db.Close()
}

func newULID() ulid.ULID {
	return ulid.MustNew(ulid.Now(), entropy);
}

func noop() {}

func ExecutionRegistration(eeb Execution) (ExecutionId, func()) {
	var buf []byte
	var err error
	eid := ExecutionId(eeb.ExecutionId)
	id := newULID()
	t := time.Now()
	eebj := eventExecutionBeginsJson{id, t, string(eeb.ExecutionId), string(eeb.ParentId), string(eeb.CreatorId), string(eeb.ProcessId), eeb.Description}
	println(fmt.Sprintf("ExecutionRegistration: eventExecutionBeginsJson<%v>", eebj))
	if buf, err = json.Marshal(eebj); err != nil {
		return eid, noop
	}
	InsertEvent(id, t, eebj.eventType(), buf);
	return eid, func() {
		id := newULID()
		t := time.Now()
		eeej := eventExecutionEndsJson{id, t, string(eeb.ExecutionId)}
		if buf, err = json.Marshal(eeej); err == nil {
			InsertEvent(id, t, eeej.eventType(), buf);
		}
	}
}

func OperationRegistration(op Operation) (OperationId, IncarnationId, EntityId) {
	var buf []byte
	var err error
	id := newULID()
	opid := OperationId(id.String())
	exid := ExecutionId(op.ExecutionId)
	incid := IncarnationId(op.IncarnationId)
	entid := EntityId(op.EntityId)
	t := time.Now()
	opj := eventOperationJson{id, string(opid), t, string(exid), op.OperationType, string(op.EntityId), string(op.IncarnationId), op.EntityDescription, op.IncarnationDescription}
	println(fmt.Sprintf("OperationRegistration: eventOperationJson<%v>", opj))
	if buf, err = json.Marshal(opj); err == nil {
		InsertEvent(id, t, opj.eventType(), buf);
	}
	return opid, incid, entid
}


func InsertEvent(ulid ulid.ULID, createdAt time.Time, eventType string, payload []byte) {
	if DbClient == nil {
		return
	}
	sqlStatement := `
                INSERT INTO events(ulid, created_at, event_type, payload)
		VALUES ($1, $2, $3, $4::jsonb)`
	_, err := DbClient.Exec(sqlStatement, ulid.String(), createdAt, eventType, payload)
	if err != nil {
		panic(err)
	}
}
