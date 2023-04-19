package org.prebid.server.bidder.rtbhouse.proto;

import com.iab.openrtb.response.SeatBid;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class RtbhouseBidResponse {

    String id;

    List<SeatBid> seatbid;

    String cur;

    Integer nbr;

    RtbhouseBidResponseExt ext;
}
