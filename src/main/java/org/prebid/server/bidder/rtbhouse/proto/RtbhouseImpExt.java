package org.prebid.server.bidder.rtbhouse.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.prebid.server.proto.openrtb.ext.request.ExtImpAuctionEnvironment;

@AllArgsConstructor(staticName = "of")
@Value
public class RtbhouseImpExt {

    @JsonProperty("ae")
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    ExtImpAuctionEnvironment auctionEnvironment;
}
