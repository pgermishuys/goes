package goes

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
)

//GossipEndpointDiscoverer used for discovering and picking the most appropriate node in a cluster
type GossipEndpointDiscoverer struct {
	MaxDiscoverAttempts int
	GossipSeeds         []string
	ClusterMembers      []MemberInfo
}

// Discover will discover nodes via performing a gossip over HTTP and then picking the best candidate to connect to
func (discoverer *GossipEndpointDiscoverer) Discover() (MemberInfo, error) {
	if len(discoverer.GossipSeeds) == 0 {
		return MemberInfo{}, errors.New("There are no gossip seeds")
	}
	discoverer.GossipSeeds = shuffleGossipSeeds(discoverer.GossipSeeds)
	gossipIndex := 0
	for attempt := 1; attempt <= discoverer.MaxDiscoverAttempts; attempt++ {
		if gossipIndex >= len(discoverer.GossipSeeds) {
			gossipIndex = 0
		}
		gossipSeed := discoverer.GossipSeeds[gossipIndex]
		gossipIndex++
		log.Printf("[info] attempting to gossip via %+v", gossipSeed)
		member, err := discoverEndPoint(gossipSeed)
		if err != nil {
			if attempt == discoverer.MaxDiscoverAttempts {
				return MemberInfo{}, errors.New("Failed to discover any cluster node members via gossip. Maximum number of attempts reached")
			}
			continue
		}
		return member, nil
	}
	return MemberInfo{}, nil
}

func discoverEndPoint(gossipSeed string) (MemberInfo, error) {
	gossipResponse, err := gossip(gossipSeed)
	if err != nil {
		return MemberInfo{}, err
	}
	candidate, _ := getBestCandidate(gossipResponse)
	return candidate, nil
}

func shuffleGossipSeeds(src []string) []string {
	for i := range src {
		j := rand.Intn(i + 1)
		src[i], src[j] = src[j], src[i]
	}
	return src
}

func getBestCandidate(response GossipResponse) (MemberInfo, error) {
	if len(response.Members) == 0 {
		return MemberInfo{}, errors.New("There are no members to determine the best candidate from")
	}
	for _, member := range response.Members {
		if member.State == "Master" && member.IsAlive {
			return member, nil
		}
	}
	for _, member := range response.Members {
		if member.IsAlive {
			return member, nil
		}
	}
	return MemberInfo{}, nil
}

func gossip(gossipSeed string) (GossipResponse, error) {
	response, err := http.Get(gossipSeed + "/gossip")
	log.Printf("[info] got response from %s: %+v", gossipSeed, response)
	if err != nil || response.StatusCode != http.StatusOK {
		return GossipResponse{}, err
	}
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	var gossipResponse GossipResponse
	err = json.Unmarshal(body, &gossipResponse)
	return gossipResponse, nil
}

type GossipSeed struct {
	ExternalTCPIP    string
	ExternalHTTPPort int
}

type GossipResponse struct {
	Members []MemberInfo `json:"members"`
}

type MemberInfo struct {
	State            string `json:"state"`
	IsAlive          bool   `json:"isAlive"`
	ExternalTCPIP    string `json:"externalTcpIp"`
	ExternalHTTPPort int    `json:"externalHttpPort"`
	ExternalTCPPort  int    `json:"externalTcpPort"`
}
