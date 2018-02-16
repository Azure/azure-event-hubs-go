package eventhub

import (
	"github.com/Azure/azure-event-hubs-go/cbs"
)

func (ns *Namespace) ensureCbsLink() error {
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

func (ns *Namespace) negotiateClaim(entityPath string) error {
	err := ns.ensureCbsLink()
	audience := ns.getEntityAudience(entityPath)
	tokenProvider, err := ns.getCBSTokenProvider()
	if err != nil {
		return err
	}
	return cbs.NegotiateClaim(audience, ns.cbsLink, tokenProvider)
}
