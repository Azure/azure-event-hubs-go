package eventhub

import (
	"github.com/Azure/azure-event-hubs-go/cbs"
)

func (ns *namespace) ensureCbsLink() error {
	ns.cbsMu.Lock()
	defer ns.cbsMu.Unlock()

	conn, err := ns.connection()
	if err != nil {
		return err
	}

	if ns.cbsLink == nil {
		link, err := cbs.NewLink(conn)
		if err != nil {
			return err
		}
		ns.cbsLink = link
	}
	return nil
}

func (ns *namespace) negotiateClaim(entityPath string) error {
	err := ns.ensureCbsLink()
	audience := ns.getEntityAudience(entityPath)
	if err != nil {
		return err
	}
	return cbs.NegotiateClaim(audience, ns.cbsLink, ns.tokenProvider)
}
