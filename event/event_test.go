package event

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"database/sql"

	_ "github.com/lib/pq"
	"encoding/json"
	"github.com/kr/pretty"
)

func TestEvent(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Event")
}

var _ = Describe("Event", func() {
	var  (
		ntf *EventService
		testChannel string
		testClient  string
		pgstr string
		msg Message
	)

	BeforeEach(func() {

		testChannel = "chat"
		testClient  = "test"
		msg = Message{
			Target: testClient,
			Body: "hello",
		}
	})

	It("Message delivery", func() {
		var tresult string
		var terr string

		pgstr = "host=127.0.0.1 user=postgres password=postgres dbname=tpm sslmode=disable"
		db, err := sql.Open("postgres", pgstr)

		ntf = NewEventService(pgstr)

		ntf.Subscribe(testChannel, testClient, func(p string) {
			fmt.Println("event received")
			tresult = p
		}, func(err error) {
			terr = err.Error()
		})

		pretty.Println(ntf)

		jm, _ := json.Marshal(msg)

		_, err = db.Exec(fmt.Sprintf("NOTIFY %s, '%s'", testChannel, jm))
		Expect(err).NotTo(HaveOccurred())


		time.Sleep(2)
		Expect(tresult).To(Equal(msg.Body))

	})
})
