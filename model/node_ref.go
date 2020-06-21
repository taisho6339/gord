package model

import "fmt"

type NodeRef struct {
	ID   HashID
	Host string
	Port string
}

func NewNodeRef(host string, port string) *NodeRef {
	return &NodeRef{
		ID:   NewHashID(fmt.Sprintf("%s:%s", host, port)),
		Host: host,
		Port: port,
	}
}

func (n *NodeRef) Address() string {
	return fmt.Sprintf("%s:%s", n.Host, n.Port)
}
