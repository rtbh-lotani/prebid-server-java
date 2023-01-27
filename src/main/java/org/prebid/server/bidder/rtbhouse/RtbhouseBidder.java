package org.prebid.server.bidder.rtbhouse;

import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import io.vertx.core.http.HttpMethod;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.prebid.server.util.BidderUtil;
import org.prebid.server.util.HttpUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        this.currencyConversionService = Objects.requireNonNull(currencyConversionService);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public Result<List<HttpRequest<BidRequest>>> makeHttpRequests(BidRequest bidRequest) {

        final List<Imp> modifiedImps = new ArrayList<>();
        final List<BidderError> errors = new ArrayList<>();

        for (Imp imp : bidRequest.getImp()) {
            try {
                modifiedImps.add(makeImp(imp, bidRequest));
            } catch (PreBidException e) {
                errors.add(BidderError.badInput(e.getMessage()));
            }
        }

        if (errors.size() > 0) {
            return Result.withErrors(errors);
        }

        final BidRequest outgoingRequest = bidRequest.toBuilder()
                .cur(Collections.singletonList(BIDDER_CURRENCY))
                .imp(modifiedImps)
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

    private Imp makeImp(Imp imp, BidRequest bidRequest) {
        return imp.toBuilder().bidfloor(resolveBidFloor(imp, bidRequest)).build();
    }

    private BigDecimal resolveBidFloor(Imp imp, BidRequest bidRequest) {
        final BigDecimal bidFloor = imp.getBidfloor();
        final String bidFloorCur = resolveBidFloorCurrency(bidRequest, imp.getBidfloorcur());
        if (!BidderUtil.isValidPrice(bidFloor)
                || StringUtils.equals(bidFloorCur, BIDDER_CURRENCY)
                || StringUtils.isEmpty(bidFloorCur)) {
            return null;
        }

        return currencyConversionService.convertCurrency(bidFloor, bidRequest, bidFloorCur, BIDDER_CURRENCY);
    }

    private static String resolveBidFloorCurrency(BidRequest bidRequest, String bidFloorCurrency) {
        if (StringUtils.isNotEmpty(bidFloorCurrency)) {
            return bidFloorCurrency;
        }
        final List<String> bidRequestCurrencies = bidRequest.getCur();
        return CollectionUtils.isNotEmpty(bidRequestCurrencies) ? bidRequestCurrencies.get(0) : null;
    }

}
