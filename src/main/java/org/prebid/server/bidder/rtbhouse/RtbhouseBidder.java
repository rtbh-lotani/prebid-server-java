package org.prebid.server.bidder.rtbhouse;

import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import io.vertx.core.http.HttpMethod;
import org.apache.commons.collections4.CollectionUtils;
import org.prebid.server.bidder.Bidder;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderCall;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.DecodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.util.BidderUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.math.BigDecimal;
import java.util.ArrayList;

public class RtbhouseBidder implements Bidder<BidRequest> {

    private static final String BIDDER_CURRENCY = "USD";

    private final String endpointUrl;
    private final JacksonMapper mapper;
    private final CurrencyConversionService currencyConversionService;

    public RtbhouseBidder(
            String endpointUrl,
            CurrencyConversionService currencyConversionService,
            JacksonMapper mapper) {
        this.endpointUrl = HttpUtil.validateUrl(Objects.requireNonNull(endpointUrl));
        this.mapper = Objects.requireNonNull(mapper);
        this.currencyConversionService = Objects.requireNonNull(currencyConversionService);
    }

    @Override
    public Result<List<HttpRequest<BidRequest>>> makeHttpRequests(BidRequest bidRequest) {
        final List<BidderError> errors = new ArrayList<>();
        final List<Imp> validImps = new ArrayList<>();

        for (Imp imp : bidRequest.getImp()) {
            try {
                final BigDecimal bidFloor = resolveBidFloor(bidRequest, imp.getBidfloorcur(), imp.getBidfloor());
                validImps.add(modifyImp(imp, bidFloor));
            } catch (PreBidException e) {
                errors.add(BidderError.badInput(e.getMessage()));
            }
        }

        if (errors.size() > 0) {
            return Result.withErrors(errors);
        }

        final BidRequest outgoingRequest = bidRequest.toBuilder()
                .cur(Collections.singletonList(BIDDER_CURRENCY))
                .imp(validImps)
                .build();

        return Result.withValue(HttpRequest.<BidRequest>builder()
                .method(HttpMethod.POST)
                .headers(HttpUtil.headers())
                .uri(endpointUrl)
                .body(mapper.encodeToBytes(outgoingRequest))
                .payload(outgoingRequest)
                .build());
    }

    @Override
    public Result<List<BidderBid>> makeBids(BidderCall<BidRequest> httpCall, BidRequest bidRequest) {
        try {
            final BidResponse bidResponse = mapper.decodeValue(httpCall.getResponse().getBody(), BidResponse.class);
            return Result.withValues(extractBids(bidResponse));
        } catch (DecodeException e) {
            return Result.withError(BidderError.badServerResponse(e.getMessage()));
        }
    }

    private static List<BidderBid> extractBids(BidResponse bidResponse) {
        if (bidResponse == null || CollectionUtils.isEmpty(bidResponse.getSeatbid())) {
            return Collections.emptyList();
        }

        return bidResponse.getSeatbid().stream()
                .filter(Objects::nonNull)
                .map(SeatBid::getBid)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(bid -> BidderBid.of(bid, BidType.banner, bidResponse.getCur()))
                .toList();
    }

    private BigDecimal resolveBidFloor(BidRequest bidRequest, String bidfloorcur, BigDecimal bidfloor) {
        if (BidderUtil.isValidPrice(bidfloor)
                && !StringUtils.equalsIgnoreCase(bidfloorcur, BIDDER_CURRENCY)
                && StringUtils.isNotBlank(bidfloorcur)) {
            return currencyConversionService.convertCurrency(bidfloor, bidRequest, bidfloorcur, BIDDER_CURRENCY);
        }

        return bidfloor;
    }

    private static Imp modifyImp(Imp imp, BigDecimal bidFloor) {
        return imp.toBuilder()
                .bidfloorcur(BIDDER_CURRENCY)
                .bidfloor(bidFloor)
                .build();
    }

}
