package event

import (
	"time"
	"errors"
	"encoding/json"

	"github.com/lib/pq"
	"sync"
)

// Сервис подписки на события Postgres
//
// см http://www.postgresql.org/docs/9.1/static/sql-notify.html
//
// для отправки сообщения можно использовать следующий SQL:
//
// notify CHANNEL, 'PAYLOAD'
//
// perform pg_notify(CHANNEL, PAYLOAD)
//
//
// пример использования:
//
// 		ntf := NewEventService()
//		ntf.Subscribe("chat", "firstClient", func(ch string, p string) {
//		}, func(err error) {
//  		})
//		ntf.Unsubscribe("chat", "firstClient")
//

const (
	ErrClientExists 	= "Client already exists for channel"
	ErrClientNotExists 	= "Client not found for channel"
)

type EventService struct {
	Listener 	*pq.Listener
	Channels 	map[string]Channel	// массив слушаемых каналов
	lock 		sync.Mutex
}

type Channel struct {
	Name 		string
	Clients 	map[string]Client
	MessageCount 	uint // количество принятых сообщений
}

func (c *Channel) handleError(err error) {
	for _, client := range c.Clients {
		go client.ErrorHandler(err)
	}
}

type Client struct {
	ID		string
	EventHandler 	EventCallback
	ErrorHandler 	ErrorCallback
}

type Message struct {
	Target	string `json:"target"`
	Body 	string `json:"body"`
}

type EventCallback func(message string)
type ErrorCallback func(err error)

// pgstr - строка для коннекта к постгресу
func NewEventService(pgstr string) (event *EventService) {

	event = &EventService{Channels: map[string]Channel{}}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			event.handleErrorAll(err)
		}
	}

	event.Listener = pq.NewListener(pgstr, 10*time.Second, time.Minute, reportProblem)

	go func() {
		var msg Message
		for {
			select {
			case n := <-event.Listener.Notify:
				if ch, ok := event.Channels[n.Channel]; ok {
					ch.MessageCount++
					err := json.Unmarshal([]byte(n.Extra), &msg)
					// не распарсили сообщение
					if err != nil {
						ch.handleError(err)
						continue
					}
					// распарсили и нашли клиента
					if client, ok := ch.Clients[msg.Target]; ok {
						go client.EventHandler(msg.Body)
					}
				}
				continue
			case <-time.After(60 * time.Second):
				go func() {
					err := event.Listener.Ping()
					if err != nil {
						event.handleErrorAll(err)
					}
				}()
				continue
			}
		}
	}()

	return
}

func (es *EventService) Subscribe(channel string, name string, eventHandler EventCallback, errorHandler ErrorCallback) error {

	es.lock.Lock()
	defer es.lock.Unlock()

	if _, ok := es.Channels[channel]; !ok {
		err := es.Listener.Listen(channel)
		if err != nil {
			return err
		}
		es.Channels[channel] = Channel{Name: channel, Clients: map[string]Client{}}
	}
	if _, ok := es.Channels[channel].Clients[name]; ok {
		return errors.New(ErrClientExists)
	}

	es.Channels[channel].Clients[name] = Client{name, eventHandler, errorHandler}
	return nil
}

func (es *EventService) Unsubscribe(channel string, name string) error {

	es.lock.Lock()
	defer es.lock.Unlock()

	if _, ok := es.Channels[channel].Clients[name]; !ok {
		return errors.New(ErrClientNotExists)
	}
	delete(es.Channels[channel].Clients, name)
	if _, ok := es.Channels[channel]; ok && len(es.Channels[channel].Clients)==0 {
		err := es.Listener.Unlisten(channel)
		if err != nil {
			return err
		}
		delete(es.Channels, channel)
	}
	return nil
}

func (es *EventService) handleErrorAll(err error) {
	for _, ch := range es.Channels {
		ch.handleError(err)
	}
}
