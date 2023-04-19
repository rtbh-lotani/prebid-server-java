package org.prebid.server.bidder.rtbhouse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Imp;
import org.prebid.server.bidder.rtbhouse.proto.RtbhouseBidResponse;
import com.iab.openrtb.response.SeatBid;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.bidder.Bidder;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderCall;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.CompositeBidderResponse;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.Price;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.bidder.rtbhouse.proto.RtbhouseBidResponseExt;
import org.prebid.server.bidder.rtbhouse.proto.RtbhouseImpExt;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.DecodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtImpAuctionEnvironment;
import org.prebid.server.proto.openrtb.ext.request.rtbhouse.ExtImpRtbhouse;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.FledgeAuctionConfig;
import org.prebid.server.util.BidderUtil;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.util.ObjectUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class RtbhouseBidder implements Bidder<BidRequest> {

    private static final TypeReference<ExtPrebid<?, ExtImpRtbhouse>> RTBHOUSE_EXT_TYPE_REFERENCE =
            new TypeReference<>() {
            };
    private static final String BIDDER_CURRENCY = "USD";

    private final String endpointUrl;
    private final JacksonMapper mapper;
    private final CurrencyConversionService currencyConversionService;

    public RtbhouseBidder(String endpointUrl,
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
                final ExtPrebid<?, ExtImpRtbhouse> impExt = parseRtbhouseExt(imp);
                final ExtImpRtbhouse rtbhouseImpExt = impExt.getBidder();
                final Price bidFloorPrice = resolveBidFloor(imp, rtbhouseImpExt, bidRequest);

                modifiedImps.add(modifyImp(imp, impExt, bidFloorPrice));
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

        return Result.withValue(BidderUtil.defaultRequest(outgoingRequest, endpointUrl, mapper));
    }

    @Override
    public CompositeBidderResponse makeBidderResponse(BidderCall<BidRequest> httpCall, BidRequest bidRequest) {
        try {
            final RtbhouseBidResponse bidResponse = mapper.decodeValue(httpCall.getResponse().getBody(),
                    RtbhouseBidResponse.class);
            return CompositeBidderResponse.withBids(extractBids(bidRequest, bidResponse), extractFledge(bidResponse));
        } catch (DecodeException e) {
            return CompositeBidderResponse.withError(BidderError.badServerResponse(e.getMessage()));
        }
    }

    /**
     * @deprecated for this bidder in favor of @link{makeBidderResponse} which supports additional response data
     */
    @Override
    @Deprecated(forRemoval = true)
    public Result<List<BidderBid>> makeBids(BidderCall<BidRequest> httpCall, BidRequest bidRequest) {
        return Result.withError(BidderError.generic("Deprecated adapter method invoked"));
    }

    private static List<BidderBid> extractBids(BidRequest bidRequest, RtbhouseBidResponse bidResponse) {
        if (bidResponse == null || CollectionUtils.isEmpty(bidResponse.getSeatbid())) {
            return Collections.emptyList();
        }

        final String bidCurrency = StringUtils.isNotBlank(bidResponse.getCur())
                ? bidResponse.getCur()
                : BIDDER_CURRENCY;

        return bidResponse.getSeatbid().stream()
                .filter(Objects::nonNull)
                .map(SeatBid::getBid)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(bid -> BidderBid.of(bid, BidType.banner, bidCurrency))
                .toList();
    }

    private ExtPrebid<?, ExtImpRtbhouse> parseRtbhouseExt(Imp imp) {
        final ObjectNode impExtRaw = imp.getExt();
        final ExtPrebid<?, ExtImpRtbhouse> impExt;
        if (impExtRaw == null) {
            throw new PreBidException("rtbhouse parameters section is missing");
        }

        try {
            impExt = mapper.mapper().convertValue(impExtRaw, RTBHOUSE_EXT_TYPE_REFERENCE);
        } catch (IllegalArgumentException e) {
            throw new PreBidException(e.getMessage());
        }

        final ExtImpRtbhouse impExtRtbhouse = impExt != null ? impExt.getBidder() : null;
        if (impExtRtbhouse == null) {
            throw new PreBidException("rtbhouse parameters section is missing");
        }
        return impExt;
    }

    private Imp modifyImp(Imp imp, ExtPrebid<?, ExtImpRtbhouse> impExt, Price bidFloorPrice) {
        return imp.toBuilder()
                .bidfloorcur(ObjectUtil.getIfNotNull(bidFloorPrice, Price::getCurrency))
                .bidfloor(ObjectUtil.getIfNotNull(bidFloorPrice, Price::getValue))
                .ext(makeImpExt(impExt.getAuctionEnvironment()))
                .build();
    }

    private ObjectNode makeImpExt(ExtImpAuctionEnvironment auctionEnvironment) {
        return auctionEnvironment != ExtImpAuctionEnvironment.SERVER_SIDE_AUCTION
                ? mapper.mapper().valueToTree(RtbhouseImpExt.of(auctionEnvironment))
                : null;
    }

    private Price resolveBidFloor(Imp imp, ExtImpRtbhouse impExt, BidRequest bidRequest) {
        final List<String> brCur = bidRequest.getCur();
        final Price initialBidFloorPrice = Price.of(imp.getBidfloorcur(), imp.getBidfloor());

        final BigDecimal impExtBidFloor = impExt.getBidFloor();
        final String impExtCurrency = impExtBidFloor != null && brCur != null && brCur.size() > 0
                ? brCur.get(0) : null;
        final Price impExtBidFloorPrice = Price.of(impExtCurrency, impExtBidFloor);
        final Price resolvedPrice = initialBidFloorPrice.getValue() == null
                ? impExtBidFloorPrice : initialBidFloorPrice;

        return BidderUtil.isValidPrice(resolvedPrice)
                && !StringUtils.equalsIgnoreCase(resolvedPrice.getCurrency(), BIDDER_CURRENCY)
                ? convertBidFloor(resolvedPrice, imp.getId(), bidRequest)
                : resolvedPrice;
    }

    private Price convertBidFloor(Price bidFloorPrice, String impId, BidRequest bidRequest) {
        final String bidFloorCur = bidFloorPrice.getCurrency();
        try {
            final BigDecimal convertedPrice = currencyConversionService
                    .convertCurrency(bidFloorPrice.getValue(), bidRequest, bidFloorCur, BIDDER_CURRENCY);

            return Price.of(BIDDER_CURRENCY, convertedPrice);
        } catch (PreBidException e) {
            throw new PreBidException(String.format(
                    "Unable to convert provided bid floor currency from %s to %s for imp `%s`",
                    bidFloorCur, BIDDER_CURRENCY, impId));
        }
    }

    private static List<FledgeAuctionConfig> extractFledge(RtbhouseBidResponse bidResponse) {
        return Optional.ofNullable(bidResponse)
                .map(RtbhouseBidResponse::getExt)
                .map(RtbhouseBidResponseExt::getFledgeAuctionConfigs)
                .orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .map(e -> FledgeAuctionConfig.builder().impId(e.getKey()).config(e.getValue()).build())
                .toList();
    }

}
