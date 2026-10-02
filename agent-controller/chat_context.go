package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

// Rank facts against the current conversation before applying the writer budget.
// Work only on copied facts: neither the gameplay snapshot nor task state is mutated.
func boundedChatFacts(parts []string, focus string) string {
	parts = append([]string(nil), parts...)
	words := strings.FieldsFunc(strings.ToLower(focus), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	score := func(part string) int {
		part = strings.ToLower(part)
		n := 0
		for _, word := range words {
			if len(word) < 3 || strings.Contains(" what where which have your about could would there please ", " "+word+" ") {
				continue
			}
			if strings.Contains(part, word) {
				n++
			}
		}
		return n
	}
	sort.SliceStable(parts, func(i, j int) bool { return score(parts[i]) > score(parts[j]) })
	const omitted = "; [partial: more facts omitted]"
	joined := strings.Join(parts, "; ")
	if len(joined) <= chatContextSectionBytes {
		return joined
	}
	return trimUTF8(joined, chatContextSectionBytes-len(omitted)) + omitted
}

func itemChatName(id uint32, name string) string {
	if name != "" {
		return name
	}
	return fmt.Sprintf("item %d (name unknown)", id)
}

// Auctions are observations, never a price oracle. Reject stale, future or
// wrong-map evidence, and retain receipt uncertainty in bid summaries.
func auctionChatParts(state persistedAgent, snapshot snapshot, now time.Time) []string {
	pages := make(map[uint32]*auctionObservation)
	for id, page := range state.AuctionCatalog {
		pages[id] = page
	}
	for _, page := range []*auctionObservation{state.AuctionObservation, snapshot.Auctions} {
		if page != nil {
			pages[page.ItemID] = page
		}
	}
	ids := make([]uint32, 0, len(pages))
	for id := range pages {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	valid := func(at int64) bool {
		return at > 0 && at <= now.UnixMilli() && now.Sub(time.UnixMilli(at)) <= auctionObservationAge
	}
	var parts []string
	for _, id := range ids {
		page := pages[id]
		if page == nil || page.MapID != snapshot.Bot.MapID || !valid(page.ObservedAt) {
			continue
		}
		offers := append([]auctionOffer(nil), page.Offers...)
		sort.Slice(offers, func(i, j int) bool { return offers[i].AuctionID < offers[j].AuctionID })
		for _, offer := range offers {
			at := offer.ObservedAt
			if at == 0 {
				at = page.ObservedAt
			}
			if !valid(at) || (offer.ExpiresAt != 0 && offer.ExpiresAt <= now.Unix()) {
				continue
			}
			parts = append(parts, fmt.Sprintf("observed AH item %d x%d: minimum bid %d copper, buyout %d copper (0=none), age %ds",
				offer.ItemID, offer.Count, offer.MinimumBid, offer.Buyout, int(now.Sub(time.UnixMilli(at)).Seconds())))
		}
		if page.More || page.CacheTruncated {
			parts = append(parts, fmt.Sprintf("AH item %d: partial catalog", id))
		}
	}
	bids := make([]uint32, 0, len(state.PendingAuctionBids))
	for id := range state.PendingAuctionBids {
		bids = append(bids, id)
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i] < bids[j] })
	for _, id := range bids {
		bid := state.PendingAuctionBids[id]
		if bid != nil {
			parts = append(parts, fmt.Sprintf("bid %d item %d: %s, %d copper, escrow verified=%t",
				id, bid.ItemID, bid.Status, bid.Price, bid.EscrowVerified))
		}
	}
	return parts
}
